#include "table/runtime/operators/deduplicate/ProcTimeDeduplicateKeepFirstRowFunction.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "../../../../core/operators/OutputTest.h"
#include "core/api/common/TaskInfoImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/RowKind.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/typeutils/RowDataSerializer.h"

using namespace omnistream;
using json = nlohmann::json;

namespace {
class CountingBatchOutput : public BatchOutputTest {
public:
    int collects = 0;

    void collect(void* record) override
    {
        ++collects;
        BatchOutputTest::collect(record);
    }
};

const std::string kKeyedProcessUniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";

const std::string kProcTimeKeepFirstDesc = R"DELIM({
        "miniBatchSize": -1,
        "inputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR"
        ],
        "keepLastRow": false,
        "isCompactChanges": false,
        "minRetentionTime": 0,
        "rowtimeIndex": -1,
        "isRowtime": false,
        "generateUpdateBefore": false,
        "outputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "generateInsert": true,
        "originDescription": null,
        "grouping": [0, 1]
      })DELIM";

omnistream::VectorBatch* MakeBatch(
    const std::vector<int64_t>& key1,
    const std::vector<int64_t>& key2,
    const std::vector<int64_t>& val,
    const std::vector<std::string>& strs,
    const std::vector<int64_t>& timestamps)
{
    const int rowCount = static_cast<int>(key1.size());
    auto* vbatch = new omnistream::VectorBatch(rowCount);

    auto* vKey1 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* vKey2 = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* vVal = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto stringVec =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        vKey1->SetValue(i, key1[i]);
        vKey2->SetValue(i, key2[i]);
        vVal->SetValue(i, val[i]);
        std::string_view view(strs[i].data(), strs[i].size());
        stringVec->SetValue(i, view);
        vbatch->setTimestamp(i, timestamps[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(vKey1);
    vbatch->Append(vKey2);
    vbatch->Append(vVal);
    vbatch->Append(stringVec.release());
    return vbatch;
}

template <typename T>
bool CompareCol(omniruntime::vec::BaseVector* left, omniruntime::vec::BaseVector* right)
{
    auto* l = reinterpret_cast<omniruntime::vec::Vector<T>*>(left);
    auto* r = reinterpret_cast<omniruntime::vec::Vector<T>*>(right);
    if (l->GetSize() != r->GetSize()) {
        return false;
    }
    for (int i = 0; i < l->GetSize(); ++i) {
        if (l->GetValue(i) != r->GetValue(i)) {
            return false;
        }
    }
    return true;
}

bool BatchesEqual(omnistream::VectorBatch* b1, omnistream::VectorBatch* b2)
{
    if (b1->GetVectorCount() != b2->GetVectorCount() || b1->GetRowCount() != b2->GetRowCount()) {
        return false;
    }
    for (int i = 0; i < b1->GetRowCount(); ++i) {
        if (b1->getRowKind(i) != b2->getRowKind(i) || b1->getTimestamp(i) != b2->getTimestamp(i)) {
            return false;
        }
    }
    return CompareCol<int64_t>(b1->Get(0), b2->Get(0)) && CompareCol<int64_t>(b1->Get(1), b2->Get(1)) &&
           CompareCol<int64_t>(b1->Get(2), b2->Get(2)) &&
           CompareCol<omniruntime::vec::LargeStringContainer<std::string_view>>(b1->Get(3), b2->Get(3));
}

KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>* CreateOperator(
    BatchOutputTest* output)
{
    json desc = json::parse(kProcTimeKeepFirstDesc);
    OperatorConfig opConfig(
        kKeyedProcessUniqueName,
        "Deduplicate(keep=[FirstRow], key=[$1, $2], order=[PROCTIME])",
        desc["inputTypes"],
        desc["outputTypes"],
        desc);

    StreamOperatorFactory factory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, omnistream::VectorBatch*, omnistream::VectorBatch*>*>(
        factory.createOperatorAndCollector(opConfig, output));

    auto* env = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env);
    auto* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    return keyedOp;
}
} // namespace

TEST(ProcTimeDeduplicateKeepFirstRowTest, KeepFirstRowInBatchAndAcrossBatches)
{
    auto* output = new CountingBatchOutput();
    auto* keyedOp = CreateOperator(output);

    // Same key appears twice in one batch: only the first arrival is kept.
    auto* input1 = MakeBatch({0, 0, 1}, {1000, 1000, 2000}, {1, 9, 3}, {"first", "dup", "other"}, {100, 101, 102});
    keyedOp->processBatch(new StreamRecord(input1));
    auto* actual1 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto* expect1 = MakeBatch({0, 1}, {1000, 2000}, {1, 3}, {"first", "other"}, {100, 102});
    expect1->setRowKind(0, RowKind::INSERT);
    expect1->setRowKind(1, RowKind::INSERT);
    EXPECT_TRUE(BatchesEqual(expect1, actual1));

    // Already-seen key is dropped; a new key is inserted.
    auto* input2 = MakeBatch({0, 2}, {1000, 3000}, {8, 4}, {"again", "newk"}, {200, 201});
    keyedOp->processBatch(new StreamRecord(input2));
    auto* actual2 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto* expect2 = MakeBatch({2}, {3000}, {4}, {"newk"}, {201});
    expect2->setRowKind(0, RowKind::INSERT);
    EXPECT_TRUE(BatchesEqual(expect2, actual2));

    // All keys already seen: no output batch at all.
    const int collectsBefore = output->collects;
    auto* input3 = MakeBatch({0, 1}, {1000, 2000}, {7, 6}, {"x", "y"}, {300, 301});
    keyedOp->processBatch(new StreamRecord(input3));
    EXPECT_EQ(collectsBefore, output->collects);

    delete expect1;
    delete expect2;
}

TEST(ProcTimeDeduplicateKeepFirstRowTest, RejectsNonInsertInput)
{
    auto* output = new CountingBatchOutput();
    auto* keyedOp = CreateOperator(output);
    auto* input = MakeBatch({0}, {1000}, {1}, {"tombstone"}, {100});
    input->setRowKind(0, RowKind::DELETE);
    EXPECT_THROW(keyedOp->processBatch(new StreamRecord(input)), std::logic_error);
    EXPECT_EQ(0, output->collects);
}
