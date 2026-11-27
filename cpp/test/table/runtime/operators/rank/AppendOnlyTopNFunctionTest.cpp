/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include <gtest/gtest.h>
#include <map>
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/HeapKeyedStateBackend.h"
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
#include "table/data/writer/BinaryRowWriter.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace omnistream {
nlohmann::json makeRankConfig(bool outputRankNumber, bool generateUpdateBefore, int64_t rankEnd)
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
    auto* func = new AppendOnlyTopNFunction<RowData*>(rankConfig, metadataCache, batchWrites);
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
    TypeSerializer* ser = new BinaryRowDataSerializer(static_cast<int>(rankConfig.at("partitionKey").size()));
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
using RankOutputRow = std::tuple<int64_t, bool, int64_t, int64_t, int64_t, int, int64_t, bool>;
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
                key->IsNull(i) ? 0 : key->GetValue(i),
                sort->IsNull(i),
                sort->IsNull(i) ? 0 : sort->GetValue(i),
                value->GetValue(i),
                rank ? rank->GetValue(i) : 0,
                static_cast<int>(batch->getRowKind(i)),
                batch->getTimestamp(i),
                key->IsNull(i));
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
        "data-state-with-append",
        new RowDataSerializer(&keyType),
        new BinaryRowDataListSerializer(3, new RowDataSerializer(&rowType)));
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

namespace {
class HeapReleaseTrackedRow : public BinaryRowData {
public:
    explicit HeapReleaseTrackedRow(int* destroyed) : BinaryRowData(1), destroyed_(destroyed)
    {
    }
    ~HeapReleaseTrackedRow() override
    {
        ++*destroyed_;
    }

private:
    int* destroyed_;
};
} // namespace

TEST(AppendOnlyTopNHeapReleaseTest, StateMapOwnsRemainingListsAndRows)
{
    using InnerMap = emhash7::HashMap<RowData*, std::vector<RowData*>*>;
    int destroyed = 0;
    {
        omnistream::CopyOnWriteStateMap<int64_t, int64_t, InnerMap*> map(LongSerializer::INSTANCE, 2);
        auto* values = new InnerMap;
        (*values)[new HeapReleaseTrackedRow(&destroyed)] =
            new std::vector<RowData*>{new HeapReleaseTrackedRow(&destroyed), new HeapReleaseTrackedRow(&destroyed)};
        map.put(1, 0, values);
        EXPECT_EQ(destroyed, 0);
    }
    EXPECT_EQ(destroyed, 3); // sort key plus two rows
}

TEST(AppendOnlyTopNHeapReleaseTest, RemoveThenDestructorReleaseOnce)
{
    using InnerMap = emhash7::HashMap<RowData*, std::vector<RowData*>*>;
    int destroyed = 0;
    {
        omnistream::CopyOnWriteStateMap<int64_t, int64_t, InnerMap*> map(LongSerializer::INSTANCE, 2);
        auto add = [&](int64_t partition) {
            auto* values = new InnerMap;
            (*values)[new HeapReleaseTrackedRow(&destroyed)] =
                new std::vector<RowData*>{new HeapReleaseTrackedRow(&destroyed)};
            map.put(partition, 0, values);
        };
        add(1);
        add(2);
        map.remove(1, 0);
        EXPECT_EQ(destroyed, 2);
        map.remove(1, 0); // removing an absent partition must not release again
        EXPECT_EQ(destroyed, 2);
        // The second partition remains owned until map destruction.
    }
    EXPECT_EQ(destroyed, 4);
}

TEST(AppendOnlyTopNHeapReleaseTest, ExtractedRowOwnsStringsAfterBatchRelease)
{
    std::unique_ptr<RowData> ordinary;
    {
        auto batch = std::unique_ptr<omnistream::VectorBatch>(omnistream::VectorBatch::CreateVectorBatch(
            1, {OMNI_LONG, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR}));
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0))->SetValue(0, 42);
        const std::vector<std::string> strings{"short", "a longer string crossing the inline boundary", "123456789"};
        for (int i = 0; i < 3; ++i) {
            std::string_view value = strings[i];
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(i + 1))
                ->SetValue(0, value);
        }
        batch->Get(4)->SetNull(0);
        batch->setRowKind(0, RowKind::UPDATE_AFTER);
        ordinary.reset(batch->extractRowData(0));
    }
    EXPECT_EQ(*ordinary->getLong(0), 42);
    EXPECT_EQ(ordinary->getStringView(1), "short");
    EXPECT_EQ(ordinary->getStringView(2), "a longer string crossing the inline boundary");
    EXPECT_EQ(ordinary->getStringView(3), "123456789");
    EXPECT_TRUE(ordinary->isNullAt(4));
    EXPECT_EQ(ordinary->getRowKind(), RowKind::UPDATE_AFTER);
    auto* row = static_cast<BinaryRowData*>(ordinary.get());
    EXPECT_EQ(row->getBufferCapacity(), row->getSizeInBytes());
}

TEST(AppendOnlyTopNHeapReleaseTest, RawCacheIsCreatedOnFirstRead)
{
    BinaryRowData row(2);
    BinaryRowWriter writer(&row);
    const std::vector<uint8_t> payload{0, 1, 2, 0, 255, 4, 5, 6, 7};
    writer.writeRawValue(0, payload.data(), payload.size());
    writer.setNullAt(1);
    writer.complete();
    EXPECT_EQ(static_cast<BinaryRawValueData*>(row.getRawValue(0))->toBytes(), payload);
    EXPECT_EQ(row.getRawValue(1), nullptr);
    EXPECT_EQ(static_cast<BinaryRawValueData*>(row.getRawValue(0))->toBytes(), payload);
}

TEST(AppendOnlyTopNMetadataTest, RankStartZeroIncludesFirstRank)
{
    for (const std::string backend : {"HashMapStateBackend", "EmbeddedRocksDBStateBackend"}) {
        SCOPED_TRACE(backend);
        auto config = omnistream::makeRankConfig(true, true, 2);
        config["rankRange"] = "rankStart=0, rankEnd=2";
        BoundaryOutput output;
        auto* op = omnistream::openAppendOnlyTopN(config, &output, true, backend, true);
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({0, 0, 0}, {20, 30, 10}, {2, 3, 1})));
        ASSERT_EQ(output.rows.size(), 2u);
        EXPECT_EQ(std::get<3>(output.rows[0]), 3);
        EXPECT_EQ(std::get<4>(output.rows[0]), 1);
        EXPECT_EQ(std::get<3>(output.rows[1]), 2);
        EXPECT_EQ(std::get<4>(output.rows[1]), 2);
        op->close();
        delete op;
    }
}

TEST(AppendOnlyTopNMetadataTest, NegativeRankBoundsAreRejectedAsInvalidRange)
{
    for (const std::string range :
         {"rankStart=-1, rankEnd=5", "rankStart=0, rankEnd=-6", "rankStart=1, rankEnd=-9223372036854775808"}) {
        SCOPED_TRACE(range);
        auto config = omnistream::makeRankConfig(true, true, 3);
        config["rankRange"] = range;
        try {
            AppendOnlyTopNFunction<RowData*> function(config);
            ADD_FAILURE() << "Expected invalid TopN rank range";
        } catch (const std::invalid_argument& error) {
            EXPECT_STREQ(error.what(), "Invalid TopN rank range");
        }
    }
}

TEST(AppendOnlyTopNMetadataTest, InvalidAndOutOfRangeRankConfigurationIsRejected)
{
    for (const std::string range :
         {"rankStart=0, rankEnd=0", "rankStart=6, rankEnd=5", "rankStart=1, rankEnd=9223372036854775808"}) {
        auto config = omnistream::makeRankConfig(true, true, 3);
        config["rankRange"] = range;
        EXPECT_THROW(AppendOnlyTopNFunction<RowData*> function(config), std::exception) << range;
    }
}

// Flink AppendOnlyTopN data-state-with-append is MapState<sortKey, List<input row>>.
// Only sort keys inside the rank range are stored, and equal sort keys share one list.
TEST(AppendOnlyTopNFunctionTest, DataStateWithAppendMatchesFlinkSortKeyMap)
{
    auto rankConfig = omnistream::makeRankConfig(false, false, 10);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {30, 20, 20}, {300, 200, 201});
    op->processBatch(new StreamRecord(inputVB));
    EXPECT_TRUE(true);

    auto* partitionKey = BinaryRowData::createBinaryRowDataWithMem(1);
    int64_t partitionValue = 0;
    partitionKey->setLong(0, partitionValue);
    op->setCurrentKey(partitionKey);

    auto* heap = dynamic_cast<HeapKeyedStateBackend<RowData*>*>(op->getKeyedStateBackend());
    ASSERT_NE(heap, nullptr);
    using RankMap = emhash7::HashMap<RowData*, std::vector<RowData*>*>;
    auto* table = reinterpret_cast<CopyOnWriteStateTable<RowData*, VoidNamespace, RankMap*>*>(
        heap->getStateTablePtr("data-state-with-append"));
    auto* entries = table->get(VoidNamespace());
    ASSERT_NE(entries, nullptr);

    std::map<int64_t, std::vector<int64_t>> payloadBySortKey;
    for (const auto& entry : *entries) {
        ASSERT_NE(entry.first, nullptr);
        ASSERT_FALSE(entry.first->isNullAt(0));
        ASSERT_NE(entry.second, nullptr);
        std::vector<int64_t> payloads;
        for (RowData* row : *entry.second) {
            ASSERT_NE(row, nullptr);
            EXPECT_EQ(*row->getLong(1), *entry.first->getLong(0));
            payloads.push_back(*row->getLong(2));
        }
        payloadBySortKey.emplace(*entry.first->getLong(0), payloads);
    }
    EXPECT_EQ(payloadBySortKey.size(), 2u);
    EXPECT_EQ(payloadBySortKey[30], std::vector<int64_t>{300});
    EXPECT_EQ(payloadBySortKey[20], (std::vector<int64_t>{200, 201}));
}

TEST(AppendOnlyTopNMetadataTest, CachedAndUncachedChangelogsMatchAcrossBatches)
{
    for (const std::string backend : {"HashMapStateBackend", "EmbeddedRocksDBStateBackend"}) {
        for (bool updateBefore : {false, true}) {
            SCOPED_TRACE(backend + (updateBefore ? " with updates" : " without updates"));
            auto config = omnistream::makeRankConfig(true, updateBefore, 3);
            config["rankRange"] = "rankStart=2, rankEnd=3";
            BoundaryOutput cachedOutput;
            BoundaryOutput uncachedOutput;
            auto* cached = omnistream::openAppendOnlyTopN(config, &cachedOutput, true, backend, true);
            auto* uncached = omnistream::openAppendOnlyTopN(config, &uncachedOutput, false, backend, false);
            for (int batchIndex = 0; batchIndex < 40; ++batchIndex) {
                std::vector<int64_t> keys, sorts, payloads;
                for (int row = 0; row < 8; ++row) {
                    keys.push_back((batchIndex * 3 + row) % 5);
                    sorts.push_back((batchIndex * 7 + row * 11) % 17);
                    payloads.push_back(batchIndex * 8 + row);
                }
                auto makeBatch = [&]() {
                    auto* batch = omnistream::makeLongBatch(keys, sorts, payloads);
                    for (int row = 0; row < 8; ++row) {
                        if ((batchIndex + row) % 7 == 0) {
                            batch->Get(1)->SetNull(row);
                        }
                        if ((batchIndex + row) % 11 == 0) {
                            batch->Get(0)->SetNull(row);
                        }
                        batch->setTimestamp(row, batchIndex * 100 + row);
                    }
                    return batch;
                };
                cachedOutput.rows.clear();
                uncachedOutput.rows.clear();
                cached->processBatch(new StreamRecord(makeBatch()));
                uncached->processBatch(new StreamRecord(makeBatch()));
                EXPECT_EQ(cachedOutput.rows, uncachedOutput.rows) << "batch=" << batchIndex;
            }
            cached->close();
            uncached->close();
            delete cached;
            delete uncached;
        }
    }
}

TEST(AppendOnlyTopNMetadataTest, StreamingLimitRetainsEmptyKeysAcrossBatchesOnBothBackends)
{
    for (const std::string backend : {"HashMapStateBackend", "EmbeddedRocksDBStateBackend"}) {
        SCOPED_TRACE(backend);
        auto config = omnistream::makeRankConfig(false, false, 4);
        config["partitionKey"] = nlohmann::json::array();
        config["sortFieldIndices"] = nlohmann::json::array();
        config["sortAscendingOrders"] = nlohmann::json::array();
        config["sortNullsIsLast"] = nlohmann::json::array();
        config["rankRange"] = "rankStart=2, rankEnd=4";
        BoundaryOutput output;
        auto* op = omnistream::openAppendOnlyTopN(config, &output, true, backend, true);
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({1, 2}, {90, 80}, {10, 20})));
        ASSERT_EQ(output.rows.size(), 1u);
        EXPECT_EQ(std::get<3>(output.rows[0]), 20);
        output.rows.clear();
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({3, 4, 5}, {100, 70, 120}, {30, 40, 50})));
        ASSERT_EQ(output.rows.size(), 2u);
        EXPECT_EQ(std::get<3>(output.rows[0]), 30);
        EXPECT_EQ(std::get<3>(output.rows[1]), 40);
        output.rows.clear();
        op->processBatch(new StreamRecord(omnistream::makeLongBatch({6}, {200}, {60})));
        EXPECT_TRUE(output.rows.empty());
        op->close();
        delete op;
    }
}
