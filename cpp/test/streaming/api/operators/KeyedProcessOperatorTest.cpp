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
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/data/RowData.h"
#include <nlohmann/json.hpp>
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include <vector>
#include <test/util/test_util.h>
#include "test/core/operators/OutputTest.h"
#include "runtime/operators/rank/AppendOnlyTopNFunction.h"

using json = nlohmann::json;

class MockUserFunction : public KeyedProcessFunction<RowData*, RowData*, RowData*> {
public:
    void open(const Configuration& parameters) override {};
    void processElement(RowData* input, Context* ctx, TimestampedCollector* out) override
    {
        isCalled = true;
    }
    bool isCalled = false;
    JoinedRowData* getResultRow() override
    {
        return nullptr;
    };
    void processBatch(omnistream::VectorBatch* inputBatch, Context& ctx, TimestampedCollector& out) override {};
    ValueState<RowData*>* getValueState() override
    {
        return nullptr;
    };
};

TEST(KeyedProcessOperatorTest, Constructor)
{
    std::string desc =
        R"delim({"originDescription":null,"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"grouping":[0],"distinctInfos":[],
"aggInfoList":
	{
	"aggregateCalls":[{"name":"AVG($1)","aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"true","filterArg":-1}],
	"accTypes":["BIGINT","BIGINT","BIGINT"],
	"aggValueTypes":["BIGINT"],
	"indexOfCountStar":2
	}
})delim";
    json config = json::parse(desc);

    GroupAggFunction* groupAgg = new GroupAggFunction(1L, config);
    KeyedProcessOperator<RowData*, RowData*, RowData*> keyedProcessOperator(groupAgg, new OutputTest(), config);
}

TEST(KeyedProcessOperatorTest, Open)
{
    std::string desc =
        R"delim({"originDescription":null,"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"grouping":[0],"distinctInfos":[],
"aggInfoList":
	{
	"aggregateCalls":[{"name":"AVG($1)","aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"true","filterArg":-1}],
	"accTypes":["BIGINT","BIGINT","BIGINT"],
	"aggValueTypes":["BIGINT"],
	"indexOfCountStar":2
	}
})delim";
    json config = json::parse(desc);
    GroupAggFunction* groupAgg = new GroupAggFunction(1L, config);
    KeyedProcessOperator<RowData*, RowData*, RowData*> keyedProcessOperator(groupAgg, new OutputTest(), config);
    keyedProcessOperator.setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {RowField("col1", BasicLogicalType::BIGINT), omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    ASSERT_NO_THROW(keyedProcessOperator.initializeState(initializer, ser));

    ASSERT_NO_THROW(keyedProcessOperator.open());
}

TEST(KeyedProcessOperatorTest, ProcessElementWithMockedUserFunction)
{
    MockUserFunction* userFunction = new MockUserFunction();

    std::string desc =
        R"delim({"originDescription":null,"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"grouping":[0],"distinctInfos":[],
"aggInfoList":
	{
	"aggregateCalls":[{"name":"AVG($1)","aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"true","filterArg":-1}],
	"accTypes":["BIGINT","BIGINT","BIGINT"],
	"aggValueTypes":["BIGINT"],
	"indexOfCountStar":2
	}
})delim";
    json config = json::parse(desc);

    KeyedProcessOperator<RowData*, RowData*, RowData*> keyedProcessOperator(userFunction, new OutputTest(), config);
    keyedProcessOperator.setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setInt(0, 1);
    row->setInt(1, 1);
    StreamRecord* record = new StreamRecord(reinterpret_cast<void*>(row));
    keyedProcessOperator.setCurrentKey(row);
    ASSERT_NO_THROW(keyedProcessOperator.processElement(record));
    ASSERT_EQ(userFunction->isCalled, true);

    delete record;
}

TEST(KeyedProcessOperatorTest, DISABLED_GroupAggFailsWhenAggregateCallMissingFilterArg)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": ["BIGINT", "BIGINT"],
    "outputTypes": ["BIGINT", "BIGINT"],
    "grouping": [0],
    "distinctInfos": [],
    "aggInfoList": {
        "aggregateCalls": [
            {"name":"SUM($1)", "aggregationFunction":"LongSumAggFunction", "argIndexes":[1], "consumeRetraction":"false"}
        ],
        "accTypes": ["BIGINT"],
        "aggValueTypes": ["BIGINT"],
        "indexOfCountStar": -1
    }
})delim";
    json config = json::parse(desc);

    auto* groupAgg = new GroupAggFunction(1L, config);
    KeyedProcessOperator<RowData*, RowData*, RowData*> keyedProcessOperator(groupAgg, new OutputTest(), config);
    keyedProcessOperator.setup();

    auto* env2 = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);
    auto* typeInfo = new std::vector<omnistream::RowField>(
        {RowField("k", BasicLogicalType::BIGINT), RowField("v", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedProcessOperator.initializeState(initializer, ser);

    ASSERT_THROW(keyedProcessOperator.open(), std::runtime_error);
}

TEST(KeyedProcessOperatorTest, DISABLED_GroupAggFailsWhenSharedDistinctExceeds64)
{
    json config = {
        {"originDescription", nullptr},
        {"inputTypes", {"BIGINT", "BIGINT"}},
        {"outputTypes", {"BIGINT"}},
        {"grouping", {0}},
        {"distinctInfos", json::array()},
        {"aggInfoList",
         {{"aggregateCalls", json::array()},
          {"accTypes", json::array()},
          {"aggValueTypes", json::array()},
          {"indexOfCountStar", -1}}}};

    const int distinctCount = 65;
    json filterArgs = json::array();
    json argIndexes = json::array();
    json aggIndexes = json::array();
    for (int i = 0; i < distinctCount; ++i) {
        config["aggInfoList"]["aggregateCalls"].push_back(
            {{"name", "COUNT($1)"},
             {"aggregationFunction", "CountAggFunction"},
             {"argIndexes", {1}},
             {"consumeRetraction", "false"},
             {"filterArg", -1}});
        config["aggInfoList"]["accTypes"].push_back("BIGINT");
        config["aggInfoList"]["aggValueTypes"].push_back("BIGINT");
        filterArgs.push_back(-1);
        argIndexes.push_back(1);
        aggIndexes.push_back(i);
        config["outputTypes"].push_back("BIGINT");
    }
    config["distinctInfos"].push_back(
        {{"filterArgs", filterArgs}, {"argIndexes", argIndexes}, {"aggIndexes", aggIndexes}});

    auto* groupAgg = new GroupAggFunction(1L, config);
    KeyedProcessOperator<RowData*, RowData*, RowData*> keyedProcessOperator(groupAgg, new OutputTest(), config);
    keyedProcessOperator.setup();

    auto* env2 = new omnistream::RuntimeEnvironmentV2();
    auto* taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    auto* initializer = new StreamTaskStateInitializerImpl(env2);
    auto* typeInfo = new std::vector<omnistream::RowField>(
        {RowField("k", BasicLogicalType::BIGINT), RowField("d", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedProcessOperator.initializeState(initializer, ser);

    ASSERT_THROW(keyedProcessOperator.open(), std::runtime_error);
}

TEST(KeyedProcessOperatorTest, FastTop1FunctionTest)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "outputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "partitionKey": [
        0
    ],
    "outputRankNumber": false,
    "rankRange": "rankStart=1, rankEnd=1",
    "generateUpdateBefore": false,
    "processFunction": "FastTop1Function",
    "sortFieldIndices": [
        1,
        2
    ],
    "sortAscendingOrders": [
        false,
        true
    ],
    "sortNullsIsLast": [
        true,
        false
    ]
})delim";
    json config = json::parse(desc);

    FastTop1Function<long>* fastTop1Function = new FastTop1Function<long>(config);
    BatchOutputTest* output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(fastTop1Function, output, config);
    keyedProcessOperator.setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col2", BasicLogicalType::BIGINT),
         omnistream::RowField("col3", BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    // test case
    /*
     * 2,5,6
     * 2,5,7
     * 3,144,14
     * 3,4,45
     * 4,4,4
     * 5,6,9
     */
    int rowCnt = 6;
    std::vector<long> col0 = {2, 2, 3, 3, 4, 5};
    std::vector<long> col1 = {5, 5, 144, 4, 4, 6};
    std::vector<long> col2 = {6, 7, 14, 45, 4, 9};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout << "input vectorbatch created" << std::endl;
    StreamRecord* record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);
    auto outputvb = output->getVectorBatch();

    /*
     * 2,5,6
     * 3,144,14
     * 4,4,4
     * 5,6,9
     */
    int rowCnt2 = 4;
    std::vector<long> expectedcol0{2, 3, 4, 5};
    std::vector<long> expectedcol1{5, 144, 4, 6};
    std::vector<long> expectedcol2{6, 14, 4, 9};
    omnistream::VectorBatch* expectedvb = new omnistream::VectorBatch(rowCnt2);
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol0.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol1.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol2.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    EXPECT_EQ(matched, true);
}

TEST(KeyedProcessOperatorTest, Appened)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "outputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "partitionKey": [
        0
    ],
    "outputRankNumber": true,
    "rankRange": "rankStart=1, rankEnd=3",
    "generateUpdateBefore": false,
    "processFunction": "AppendOnlyTopNFunction",
    "sortFieldIndices": [
        1
    ],"sortAscendingOrders": [
        false
    ],"sortNullsIsLast": [
        true
    ]})delim";
    json config = json::parse(desc);

    AppendOnlyTopNFunction<long>* TopNFunction = new AppendOnlyTopNFunction<long>(config);
    BatchOutputTest* output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(TopNFunction, output, config);
    keyedProcessOperator.setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col2", BasicLogicalType::BIGINT),
         omnistream::RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    // test case
    /*
     * 2,5,6
     * 2,5,7
     * 3,144,14
     * 3,4,45
     * 4,4,4
     * 5,6,9
     */
    int rowCnt = 6;
    std::vector<long> col0 = {1, 1, 1, 1, 1, 5};
    std::vector<long> col1 = {5, 5, 144, 14, 24, 6};
    std::vector<long> col2 = {6, 7, 14, 45, 4, 9};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout << "input vectorbatch created" << std::endl;
    StreamRecord* record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);
    auto outputvb = output->getVectorBatch();

    // One batch emits the final Top-3. Partition 1 keeps sort keys 144, 24, 14.
    // The two sort-key 5 rows fall out. Partition 5 keeps its only row.
    int rowCnt2 = 4;
    std::vector<long> expectedcol0 = {1, 1, 1, 5};
    std::vector<long> expectedcol1 = {144, 24, 14, 6};
    std::vector<long> expectedcol2 = {14, 4, 45, 9};
    std::vector<long> expectedcol3 = {1, 2, 3, 1};
    omnistream::VectorBatch* expectedvb = new omnistream::VectorBatch(rowCnt2);
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol0.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol1.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol2.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol3.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    EXPECT_EQ(matched, true);
}

TEST(KeyedProcessOperatorTest, DISABLED_Appened2)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "outputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "partitionKey": [
        0
    ],
    "outputRankNumber": true,
    "rankRange": "rankStart=1, rankEnd=3",
    "generateUpdateBefore": false,
    "processFunction": "AppendOnlyTopNFunction",
    "sortFieldIndices": [
        1, 2
    ],"sortAscendingOrders": [
        false, true
    ],"sortNullsIsLast": [
        true, false
    ]})delim";
    json config = json::parse(desc);

    AppendOnlyTopNFunction<long>* TopNFunction = new AppendOnlyTopNFunction<long>(config);
    BatchOutputTest* output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(TopNFunction, output, config);
    keyedProcessOperator.setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField>* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col2", BasicLogicalType::BIGINT),
         omnistream::RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    // test case
    /*
     * 2,5,6
     * 2,5,7
     * 3,144,14
     * 3,4,45
     * 4,4,4
     * 5,6,9
     */
    int rowCnt = 6;
    std::vector<long> col0 = {1, 1, 1, 1, 1, 5};
    std::vector<long> col1 = {5, 5, 144, 14, 24, 6};
    std::vector<long> col2 = {6, 7, 14, 45, 4, 9};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout << "input vectorbatch created" << std::endl;
    StreamRecord* record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);
    auto outputvb = output->getVectorBatch();

    /*
     * 2,5,6
     * 3,144,14
     * 4,4,4
     * 5,6,9
     */
    int rowCnt2 = 10;
    std::vector<long> expectedcol0 = {1, 1, 1, 1, 1, 1, 1, 1, 1, 5};
    std::vector<long> expectedcol1 = {5, 5, 144, 5, 5, 14, 5, 24, 14, 6};
    std::vector<long> expectedcol2 = {6, 7, 14, 6, 7, 45, 6, 4, 45, 9};
    std::vector<long> expectedcol3 = {1, 2, 1, 2, 3, 2, 3, 2, 3, 1};
    omnistream::VectorBatch* expectedvb = new omnistream::VectorBatch(rowCnt2);
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol0.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol1.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol2.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol3.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    EXPECT_EQ(matched, true);
}
