#include "table/runtime/operators/rank/AppendOnlyFirstNFunction.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "../../../../core/operators/OutputTest.h"
#include "core/api/common/TaskInfoImpl.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/RowKind.h"
#include "table/typeutils/RowDataSerializer.h"

using namespace omnistream;
using json = nlohmann::json;

namespace {
const std::string kKeyedProcessUniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";

const std::string kFirstNDesc = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "VARCHAR"],
        "outputTypes": ["BIGINT", "BIGINT", "VARCHAR"],
        "partitionKey": [0],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=2",
        "generateUpdateBefore": false,
        "processFunction": "AppendOnlyFirstNFunction",
        "sortFieldIndices": [1],
        "sortAscendingOrders": [true],
        "sortNullsIsLast": [true]
      })DELIM";

const std::string kFirstNWithRankNumberDesc = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "VARCHAR"],
        "outputTypes": ["BIGINT", "BIGINT", "VARCHAR", "BIGINT"],
        "partitionKey": [0],
        "outputRankNumber": true,
        "rankRange": "rankStart=1, rankEnd=2",
        "generateUpdateBefore": false,
        "processFunction": "AppendOnlyFirstNFunction",
        "sortFieldIndices": [1],
        "sortAscendingOrders": [true],
        "sortNullsIsLast": [true]
      })DELIM";

const std::string kFirstNOffsetDesc = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "VARCHAR"],
        "outputTypes": ["BIGINT", "BIGINT", "VARCHAR"],
        "partitionKey": [0],
        "outputRankNumber": false,
        "rankRange": "rankStart=2, rankEnd=2",
        "generateUpdateBefore": false,
        "processFunction": "AppendOnlyFirstNFunction",
        "sortFieldIndices": [1],
        "sortAscendingOrders": [true],
        "sortNullsIsLast": [true]
      })DELIM";

omnistream::VectorBatch* MakeBatch(
    const std::vector<int64_t>& keys,
    const std::vector<int64_t>& vals,
    const std::vector<std::string>& strs,
    const std::vector<int64_t>& timestamps,
    const std::vector<int64_t>* ranks = nullptr)
{
    const int rowCount = static_cast<int>(keys.size());
    auto* vbatch = new omnistream::VectorBatch(rowCount);
    auto* vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto* vVal = new omniruntime::vec::Vector<int64_t>(rowCount);
    auto stringVec =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        vKey->SetValue(i, keys[i]);
        vVal->SetValue(i, vals[i]);
        std::string_view view(strs[i].data(), strs[i].size());
        stringVec->SetValue(i, view);
        vbatch->setTimestamp(i, timestamps[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(vKey);
    vbatch->Append(vVal);
    vbatch->Append(stringVec.release());
    if (ranks != nullptr) {
        auto* rankVec = new omniruntime::vec::Vector<int64_t>(rowCount);
        for (int i = 0; i < rowCount; ++i) {
            rankVec->SetValue(i, (*ranks)[static_cast<size_t>(i)]);
        }
        vbatch->Append(rankVec);
    }
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
    bool colsEqual = CompareCol<int64_t>(b1->Get(0), b2->Get(0)) && CompareCol<int64_t>(b1->Get(1), b2->Get(1)) &&
                     CompareCol<omniruntime::vec::LargeStringContainer<std::string_view>>(b1->Get(2), b2->Get(2));
    if (!colsEqual) {
        return false;
    }
    if (b1->GetVectorCount() > 3) {
        return CompareCol<int64_t>(b1->Get(3), b2->Get(3));
    }
    return true;
}

KeyedProcessOperator<RowData*, RowData*, RowData*>* CreateOperator(BatchOutputTest* output, const std::string& descJson)
{
    json desc = json::parse(descJson);
    OperatorConfig opConfig(
        kKeyedProcessUniqueName,
        "Rank(strategy=[AppendFastStrategy], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=2])",
        desc["inputTypes"],
        desc["outputTypes"],
        desc);

    StreamOperatorFactory factory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
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
    auto* typeInfo = new std::vector<omnistream::RowField>({omnistream::RowField("col0", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    return keyedOp;
}
} // namespace

TEST(AppendOnlyFirstNFunctionTest, KeepFirstNInBatchAndAcrossBatches)
{
    auto* output = new BatchOutputTest();
    auto* keyedOp = CreateOperator(output, kFirstNDesc);

    auto* input1 = MakeBatch({0, 0, 1, 0}, {12, 19, 33, 11}, {"a1", "a2", "b1", "a3"}, {1, 2, 3, 4});
    keyedOp->processBatch(new StreamRecord(input1));
    auto* actual1 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto* expect1 = MakeBatch({0, 0, 1}, {12, 19, 33}, {"a1", "a2", "b1"}, {1, 2, 3});
    EXPECT_TRUE(BatchesEqual(expect1, actual1));

    auto* input2 = MakeBatch({1, 2}, {44, 55}, {"b2", "c1"}, {5, 6});
    keyedOp->processBatch(new StreamRecord(input2));
    auto* actual2 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto* expect2 = MakeBatch({1, 2}, {44, 55}, {"b2", "c1"}, {5, 6});
    EXPECT_TRUE(BatchesEqual(expect2, actual2));

    auto* input3 = MakeBatch({0, 1}, {99, 88}, {"drop0", "drop1"}, {7, 8});
    keyedOp->processBatch(new StreamRecord(input3));
    auto* actual3 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    EXPECT_EQ(actual2, actual3);

    delete expect1;
    delete expect2;
}

TEST(AppendOnlyFirstNFunctionTest, OutputRankNumber)
{
    auto* output = new BatchOutputTest();
    auto* keyedOp = CreateOperator(output, kFirstNWithRankNumberDesc);

    auto* input1 = MakeBatch({0, 0, 0, 1, 1}, {12, 19, 11, 33, 44}, {"a1", "a2", "a3", "b1", "b2"}, {1, 2, 3, 4, 5});
    keyedOp->processBatch(new StreamRecord(input1));
    auto* actual1 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    std::vector<int64_t> ranks{1, 2, 1, 2};
    auto* expect1 = MakeBatch({0, 0, 1, 1}, {12, 19, 33, 44}, {"a1", "a2", "b1", "b2"}, {1, 2, 4, 5}, &ranks);
    EXPECT_TRUE(BatchesEqual(expect1, actual1));

    delete expect1;
}

TEST(AppendOnlyFirstNFunctionTest, OffsetKeepsOnlyRankStart)
{
    auto* output = new BatchOutputTest();
    auto* keyedOp = CreateOperator(output, kFirstNOffsetDesc);

    auto* input1 = MakeBatch({0, 0, 0, 1, 1}, {12, 19, 11, 33, 44}, {"a1", "a2", "a3", "b1", "b2"}, {1, 2, 3, 4, 5});
    keyedOp->processBatch(new StreamRecord(input1));
    auto* actual1 = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    auto* expect1 = MakeBatch({0, 1}, {19, 44}, {"a2", "b2"}, {2, 5});
    EXPECT_TRUE(BatchesEqual(expect1, actual1));

    delete expect1;
}
