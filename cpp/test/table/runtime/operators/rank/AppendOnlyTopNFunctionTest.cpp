#include <gtest/gtest.h>
#include <random>
#include "ReferenceAppendOnlyTopNFunction.h"
#include "runtime/state/EmbeddedRocksDBStateBackend.h"
#include "runtime/state/RocksDBMemoryControllerUtils.h"
#include <tuple>
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/data/RowKind.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace omnistream {
nlohmann::json makeRankConfig(bool outputRankNumber, bool generateUpdateBefore, int rankEnd)
{
    std::string outputTypes =
        outputRankNumber ? R"("BIGINT","BIGINT","BIGINT","BIGINT")" : R"("BIGINT","BIGINT","BIGINT")";
    std::string json =
        std::string(R"({"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":[)") +
        outputTypes + R"(],"partitionKey":[0],"outputRankNumber":)" + (outputRankNumber ? "true" : "false") +
        R"(,"rankRange":"rankStart=1, rankEnd=)" + std::to_string(rankEnd) + R"(","generateUpdateBefore":)" +
        (generateUpdateBefore ? "true" : "false") +
        R"(,"processFunction":"AppendOnlyTopNFunction","sortFieldIndices":[1],"sortAscendingOrders":[false],"sortNullsIsLast":[true]})";
    return nlohmann::json::parse(json);
}

KeyedProcessOperator<RowData*, RowData*, RowData*>* openAppendOnlyTopN(
    const nlohmann::json& rankConfig,
    BatchOutputTest* output,
    bool metadataCache = true,
    const std::string& backend = "HashMapStateBackend",
    bool batchWrites = false)
{
    KeyedProcessFunction<RowData*, RowData*, RowData*>* func =
        metadataCache ? static_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(
                            new AppendOnlyTopNFunction<RowData*>(rankConfig, true, batchWrites))
                      : static_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(
                            new ReferenceAppendOnlyTopNFunction<RowData*>(rankConfig));
    nlohmann::json newRankConfig = rankConfig;
    auto* op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend(backend);
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    if (backend == "EmbeddedRocksDBStateBackend") {
        static int instance = 0;
        auto path = std::filesystem::temp_directory_path() /
                    ("rank-boundary-ut-" + std::to_string(getpid()) + "-" + std::to_string(instance++));
        std::filesystem::create_directories(path);
        taskInfo->setRocksdbStorePaths({path.string()});
        env2->setRocksDBSharedResources(
            RocksDBMemoryControllerUtils::allocateRocksDBSharedResources(32 * 1024 * 1024, 0.5, 0.1, false));
    }
    auto jobId = omnistream::JobIDPOD(1, 2);
    auto jobVertexId = omnistream::JobVertexID(3, 4);
    auto attemptId = omnistream::ExecutionAttemptIDPOD::randomId();
    auto* localStore = new TaskLocalStateStore(jobId, jobVertexId, 0, nullptr);
    auto* responder = new omnistream::NoOpCheckpoingResponder();
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>(jobId, attemptId, localStore, responder));
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer =
        backend == "EmbeddedRocksDBStateBackend"
            ? new StreamTaskStateInitializerImpl(new EmbeddedRocksDBStateBackend(*taskInfo), env2)
            : new StreamTaskStateInitializerImpl(env2);
    TypeSerializer* ser = new BinaryRowDataSerializer(1);
    op->initializeState(initializer, ser);
    op->open();
    return op;
}

omnistream::VectorBatch* makeLongBatch(
    const std::vector<int64_t>& col0, const std::vector<int64_t>& col1, const std::vector<int64_t>& col2)
{
    int rowCnt = static_cast<int>(col0.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(col0.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(col1.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(col2.data())));
    return vb;
}

void expectRowKinds(omnistream::VectorBatch* actual, const std::vector<RowKind>& expected)
{
    ASSERT_NE(actual, nullptr);
    ASSERT_EQ(actual->GetRowCount(), static_cast<int32_t>(expected.size()));
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual->getRowKind(i), expected[i]) << "rowKind mismatch at " << i;
    }
}

} // namespace omnistream

// Sort keys below the current Top-N threshold are ignored and produce no extra output.
TEST(AppendOnlyTopNFunctionTest, IgnoreSortKeyOutsideTopN)
{
    auto rankConfig = omnistream::makeRankConfig(false, true, 2);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* first = omnistream::makeLongBatch({0, 0}, {20, 30}, {200, 300});
    op->processBatch(new StreamRecord(first));
    auto* afterFirst = output->getVectorBatch();
    ASSERT_NE(afterFirst, nullptr);
    const int rowsAfterFirst = afterFirst->GetRowCount();
    EXPECT_GT(rowsAfterFirst, 0);

    auto* second = omnistream::makeLongBatch({0}, {10}, {100});
    op->processBatch(new StreamRecord(second));

    // An ignored key emits no batch, so the previous output stays in place.
    auto* actual = output->getVectorBatch();
    EXPECT_EQ(actual, afterFirst);
    EXPECT_EQ(actual->GetRowCount(), rowsAfterFirst);
}

// Three rows share one sort key and each stays inside Top-N, so the output keeps all three inserts.
TEST(AppendOnlyTopNFunctionTest, SameSortKeyInOneBatchKeepsAllInserts)
{
    auto rankConfig = omnistream::makeRankConfig(false, false, 10);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {10, 10, 10}, {1, 2, 3});
    op->processBatch(new StreamRecord(inputVB));

    auto* expectedVB = omnistream::makeLongBatch({0, 0, 0}, {10, 10, 10}, {1, 2, 3});
    auto* actual = output->getVectorBatch();
    omnistream::expectRowKinds(actual, {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT});
    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(actual, expectedVB));
}

namespace {
using RankOutputRow = std::tuple<int64_t, bool, int64_t, int64_t, int64_t, int, int64_t>;
class BoundaryOutput : public BatchOutputTest {
public:
    std::vector<RankOutputRow> rows;
    void collect(void* record) override
    {
        auto* batch = static_cast<omnistream::VectorBatch*>(static_cast<StreamRecord*>(record)->getValue());
        auto* key = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0));
        auto* sort = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1));
        auto* value = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(2));
        auto* rank =
            batch->GetVectorCount() > 3 ? reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(3)) : nullptr;
        for (int i = 0; i < batch->GetRowCount(); ++i) {
            rows.emplace_back(
                key->GetValue(i),
                sort->IsNull(i),
                sort->IsNull(i) ? 0 : sort->GetValue(i),
                value->GetValue(i),
                rank ? rank->GetValue(i) : 0,
                static_cast<int>(batch->getRowKind(i)),
                batch->getTimestamp(i));
        }
        delete batch;
        delete static_cast<StreamRecord*>(record);
    }
};

} // namespace

TEST(AppendOnlyTopNMetadataTest, ReuseMetadataAndReleaseAtOperatorClose)
{
    for (const std::string backend : {"HashMapStateBackend", "EmbeddedRocksDBStateBackend"}) {
        BoundaryOutput output;
        auto* op =
            omnistream::openAppendOnlyTopN(omnistream::makeRankConfig(true, true, 2), &output, true, backend, true);
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({0, 0}, {30, 20}, {1, 2})));
        output.rows.clear();
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({0, 0}, {20, 10}, {3, 4})));
        EXPECT_TRUE(output.rows.empty());
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({0, 0, 0}, {40, 25, 35}, {5, 6, 7})));
        op->close();
        delete op;
    }
}

TEST(AppendOnlyTopNMetadataTest, RandomizedOutputMatchesOriginalMaster)
{
    for (const std::string backend : {"HashMapStateBackend", "EmbeddedRocksDBStateBackend"}) {
        for (bool nullsLast : {false, true}) {
            for (bool outputRank : {false, true}) {
                for (bool batchWrites : {false, true}) {
                    auto config = omnistream::makeRankConfig(outputRank, batchWrites, 5);
                    if (!outputRank) {
                        config["rankRange"] = "rankStart=2, rankEnd=5";
                    }
                    config["sortNullsIsLast"] = {nullsLast};
                    BoundaryOutput enabled, reference;
                    auto* withCache = omnistream::openAppendOnlyTopN(config, &enabled, true, backend, batchWrites);
                    auto* original = omnistream::openAppendOnlyTopN(config, &reference, false, backend);
                    std::mt19937 generator(20261105);
                    for (int batchIndex = 0; batchIndex < 180; ++batchIndex) {
                        std::vector<int64_t> keys, sorts, values;
                        int count = batchIndex % 19;
                        for (int i = 0; i < count; ++i) {
                            keys.push_back(generator() % 7);
                            sorts.push_back(static_cast<int64_t>(generator() % 11) - 5);
                            values.push_back(batchIndex * 100 + i);
                        }
                        auto* left = omnistream::makeLongBatch(keys, sorts, values);
                        auto* right = omnistream::makeLongBatch(keys, sorts, values);
                        for (int i = 0; i < count; ++i) {
                            left->setTimestamp(i, 100000 + batchIndex * 100 + i);
                            right->setTimestamp(i, 100000 + batchIndex * 100 + i);
                            if (generator() % 5 == 0) {
                                left->Get(1)->SetNull(i);
                                right->Get(1)->SetNull(i);
                            }
                        }
                        enabled.rows.clear();
                        reference.rows.clear();
                        withCache->processBatch(new StreamRecord(left));
                        original->processBatch(new StreamRecord(right));
                        // Stable partition ordering preserves each key's full changelog.
                        auto byKey = [](const RankOutputRow& a, const RankOutputRow& b) {
                            return std::get<0>(a) < std::get<0>(b);
                        };
                        std::stable_sort(enabled.rows.begin(), enabled.rows.end(), byKey);
                        std::stable_sort(reference.rows.begin(), reference.rows.end(), byKey);
                        ASSERT_EQ(enabled.rows, reference.rows)
                            << backend << " nullsLast=" << nullsLast << " outputRank=" << outputRank
                            << " batchWrites=" << batchWrites << " batch=" << batchIndex;
                    }
                    withCache->close();
                    original->close();
                    delete withCache;
                    delete original;
                }
            }
        }
    }
}

// Fault injection uses a real RocksDB failure: write to a dropped CF. Keep
// this test-only access off the production state API and its virtual table.
struct RankStateTestAccess {
    using State = RocksdbMapState<RowData*, VoidNamespace, RowData*, std::vector<RowData*>*>;
    static void dropColumnFamily(State* state)
    {
        ASSERT_TRUE(state->stateTable->getRocksDB()->DropColumnFamily(state->stateTable->getColumnFamily()).ok());
    }
};

TEST(AppendOnlyTopNMetadataTest, FailedBatchWritePublishesNoMetadataOrOutput)
{
    BoundaryOutput output;
    auto config = omnistream::makeRankConfig(true, true, 3);
    auto* op = omnistream::openAppendOnlyTopN(config, &output, true, "EmbeddedRocksDBStateBackend", true);
    // Obtain the same state using the runtime context, without touching its
    // contents or exposing the operator's private working structures.
    omnistream::RowType keyType(false, {"BIGINT"});
    omnistream::RowType rowType(false, {"BIGINT", "BIGINT", "BIGINT"});
    auto* desc = new MapStateDescriptor<RowData*, std::vector<RowData*>*>(
        "data-state-with-append", new RowDataSerializer(&keyType), new ListSerializer(new RowDataSerializer(&rowType)));
    desc->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
    auto* state = dynamic_cast<RankStateTestAccess::State*>(
        op->getRuntimeContext()->getMapState<RowData*, std::vector<RowData*>*>(desc));
    ASSERT_NE(state, nullptr);
    RankStateTestAccess::dropColumnFamily(state);
    EXPECT_THROW(
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({0, 1}, {30, 40}, {1, 2}))), std::exception);
    EXPECT_TRUE(output.rows.empty());
    op->close();
    delete op;
}

TEST(AppendOnlyTopNMetadataTest, InvalidConfigurationIsRejected)
{
    auto invalid = omnistream::makeRankConfig(true, true, 3);
    invalid.erase("partitionKey");
    EXPECT_THROW(AppendOnlyTopNFunction<RowData*> function(invalid), std::exception);
    auto ascending = omnistream::makeRankConfig(true, true, 3);
    ascending["sortAscendingOrders"] = {true};
    EXPECT_THROW(AppendOnlyTopNFunction<RowData*> function(ascending), std::exception);
}
