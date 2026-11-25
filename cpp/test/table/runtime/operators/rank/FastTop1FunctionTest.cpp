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
#include "table/runtime/operators/rank/FastTop1Function.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "typeutils/RowDataSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

// Utility function to create a VectorBatch for testing.
omnistream::VectorBatch* createTestBatch()
{
    auto batch = new omnistream::VectorBatch(6);

    // Partition Key Column
    auto keyColumn = new omniruntime::vec::Vector<int64_t>(6);
    for (int i = 0; i < 6; i++) {
        keyColumn->SetValue(i, i % 2); // Alternating keys (0, 1).
    }
    batch->Append(keyColumn);

    // Value Column
    auto valueColumn1 = new omniruntime::vec::Vector<int64_t>(6);
    auto valueColumn2 = new omniruntime::vec::Vector<int64_t>(6);
    for (int i = 0; i < 6; i++) {
        valueColumn1->SetValue(i, i * 10); // Values: 0, 10, 20, ...
        valueColumn2->SetValue(i, i * 5);
    }
    batch->Append(valueColumn1);
    batch->Append(valueColumn2);

    return batch;
}

omnistream::VectorBatch* createTestBatch2()
{
    auto batch = new omnistream::VectorBatch(4);

    // Partition Key Column
    auto keyColumn = new omniruntime::vec::Vector<int64_t>(4);
    for (int i = 0; i < 4; i++) {
        keyColumn->SetValue(i, i % 2); // Alternating keys (0, 1).
    }
    batch->Append(keyColumn);

    // Value Column
    auto valueColumn1 = new omniruntime::vec::Vector<int64_t>(4);
    auto valueColumn2 = new omniruntime::vec::Vector<int64_t>(4);
    valueColumn1->SetValue(0, 50);
    valueColumn1->SetValue(1, 100);
    valueColumn1->SetValue(2, 50);
    valueColumn1->SetValue(3, 100);
    valueColumn2->SetValue(0, 200);
    valueColumn2->SetValue(1, 400);
    valueColumn2->SetValue(2, 400);
    valueColumn2->SetValue(3, 200);
    batch->Append(valueColumn1);
    batch->Append(valueColumn2);

    return batch;
}

// Test: Basic functionality of open().
TEST(FastTop1FunctionTest, OpenInitialization)
{
    // Initialize the FastTop1Function with mock configuration.
    std::string description = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "VARCHAR(2147483647)", "BIGINT", "BIGINT"],
        "partitionKey": [0],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=1",
        "generateUpdateBefore": false,
        "processFunction": "FastTop1Function",
        "sortFieldIndices": [1, 2],
        "sortAscendingOrders": [false, true],
        "sortNullsIsLast": [true, false]
    })DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    KeyedProcessFunction<long, RowData*, RowData*>* top1Function =
        reinterpret_cast<KeyedProcessFunction<long, RowData*, RowData*>*>(new FastTop1Function<long>(rankConfig));

    //    GroupAggFunction *func = new GroupAggFunction(0l, opConfig.getDescription());
    //    auto *op = new KeyedProcessOperator(top1Function, chainOutput, opConfig.getDescription());
    nlohmann::json newRankConfig = rankConfig;
    auto* op = new KeyedProcessOperator(top1Function, new BatchOutputTest(), newRankConfig);
    op->setup();
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
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());
}

TEST(FastTop1FunctionTest, ProcessSingleBatch)
{
    // Initialize the FastTop1Function with mock configuration.
    std::string description = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "VARCHAR(2147483647)", "BIGINT", "BIGINT"],
        "partitionKey": [0],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=1",
        "generateUpdateBefore": false,
        "processFunction": "FastTop1Function",
        "sortFieldIndices": [1, 2],
        "sortAscendingOrders": [false, true],
        "sortNullsIsLast": [true, false]
    })DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    KeyedProcessFunction<long, RowData*, RowData*>* top1Function =
        reinterpret_cast<KeyedProcessFunction<long, RowData*, RowData*>*>(new FastTop1Function<long>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(top1Function, output, newRankConfig);
    op->setup();
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
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto record = new StreamRecord(createTestBatch());
    op->processBatch(record);
    auto expectedBatch = new omnistream::VectorBatch(2);
    // Partition Key Column
    auto keyColumn = new omniruntime::vec::Vector<int64_t>(2);
    auto value1 = new omniruntime::vec::Vector<int64_t>(2);
    auto value2 = new omniruntime::vec::Vector<int64_t>(2);
    keyColumn->SetValue(0, 0);
    value1->SetValue(0, 40);
    value2->SetValue(0, 20);

    keyColumn->SetValue(1, 1);
    value1->SetValue(1, 50);
    value2->SetValue(1, 25);

    expectedBatch->Append(keyColumn);
    expectedBatch->Append(value1);
    expectedBatch->Append(value2);

    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedBatch);
    EXPECT_EQ(matched, true);
}

TEST(FastTop1FunctionTest, ProcessMultipleBatches)
{
    // Initialize the FastTop1Function with mock configuration.
    std::string description = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "BIGINT", "BIGINT"],
        "partitionKey": [0],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=1",
        "generateUpdateBefore": false,
        "processFunction": "FastTop1Function",
        "sortFieldIndices": [1, 2],
        "sortAscendingOrders": [false, false],
        "sortNullsIsLast": [true, false]
    })DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    KeyedProcessFunction<long, RowData*, RowData*>* top1Function =
        reinterpret_cast<KeyedProcessFunction<long, RowData*, RowData*>*>(new FastTop1Function<long>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(top1Function, output, newRankConfig);
    op->setup();
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
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto record = new StreamRecord(createTestBatch());
    op->processBatch(record);
    auto expectedBatch = new omnistream::VectorBatch(2);
    // Partition Key Column
    auto keyColumn = new omniruntime::vec::Vector<int64_t>(2);
    auto value1 = new omniruntime::vec::Vector<int64_t>(2);
    auto value2 = new omniruntime::vec::Vector<int64_t>(2);
    keyColumn->SetValue(0, 0);
    value1->SetValue(0, 40);
    value2->SetValue(0, 20);

    keyColumn->SetValue(1, 1);
    value1->SetValue(1, 50);
    value2->SetValue(1, 25);

    expectedBatch->Append(keyColumn);
    expectedBatch->Append(value1);
    expectedBatch->Append(value2);

    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedBatch);
    EXPECT_EQ(matched, true);

    auto record2 = new StreamRecord(createTestBatch2());
    op->processBatch(record2);

    expectedBatch = new omnistream::VectorBatch(2);
    // Partition Key Column
    keyColumn = new omniruntime::vec::Vector<int64_t>(2);
    value1 = new omniruntime::vec::Vector<int64_t>(2);
    value2 = new omniruntime::vec::Vector<int64_t>(2);
    keyColumn->SetValue(0, 0);
    value1->SetValue(0, 50);
    value2->SetValue(0, 400);

    keyColumn->SetValue(1, 1);
    value1->SetValue(1, 100);
    value2->SetValue(1, 400);

    expectedBatch->Append(keyColumn);
    expectedBatch->Append(value1);
    expectedBatch->Append(value2);

    matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedBatch);
    EXPECT_EQ(matched, true);
}

/*
    row| key0	key1	Col2	Col3
    0  | 1000	1001	200	    300
    1  | 1002	1003	201	    301
    2  | 1000	1002	202	    302
    3  | 1002	1003	200	    300
    4  | 1000	1002	201	    301
    5  | 1004	1005	202	    302
    6  | 1004	1005	200	    300
    7  | 1000	1002	201	    301
*/
omnistream::VectorBatch* createTwoKeyVectorBatch()
{
    constexpr int rows = 8;
    constexpr int cols = 4;
    auto vBatch = new omnistream::VectorBatch(rows);

    auto key0 = new omniruntime::vec::Vector<int64_t>(rows);
    auto key1 = new omniruntime::vec::Vector<int64_t>(rows);

    std::vector<std::pair<int64_t, int64_t>> keys = {
        {1000, 1001}, {1002, 1003}, {1000, 1002}, {1002, 1003}, {1000, 1002}, {1004, 1005}, {1004, 1005}, {1000, 1002}};

    for (int i = 0; i < rows; ++i) {
        key0->SetValue(i, keys[i].first);
        key1->SetValue(i, keys[i].second);
    }
    vBatch->Append(key0);
    vBatch->Append(key1);

    for (int col = 2; col < cols; ++col) {
        auto vec = new omniruntime::vec::Vector<int64_t>(rows);
        for (int i = 0; i < rows; ++i) {
            vec->SetValue(i, col * 100 + (i % 3));
        }
        vBatch->Append(vec);
    }

    return vBatch;
}

/*
    row| key0	key1	Col2	Col3
    0  | 1000	1001	100	    150
    1  | 1002	1003	101	    151
    2  | 1000	1002	102	    152
    3  | 1002	1003	103	    153
    4  | 1000	1002	100	    150
    5  | 1004	1005	101	    151
    6  | 1004	1005	102	    152
    7  | 1000	1002	103	    153
*/
omnistream::VectorBatch* createTwoKeyVectorBatch2()
{
    constexpr int rows = 8;
    constexpr int cols = 4;
    auto vBatch = new omnistream::VectorBatch(rows);

    auto key0 = new omniruntime::vec::Vector<int64_t>(rows);
    auto key1 = new omniruntime::vec::Vector<int64_t>(rows);

    std::vector<std::pair<int64_t, int64_t>> keys = {
        {1000, 1001}, {1002, 1003}, {1000, 1002}, {1002, 1003}, {1000, 1002}, {1004, 1005}, {1004, 1005}, {1000, 1002}};

    for (int i = 0; i < rows; ++i) {
        key0->SetValue(i, keys[i].first);
        key1->SetValue(i, keys[i].second);
    }
    vBatch->Append(key0);
    vBatch->Append(key1);

    for (int col = 2; col < cols; ++col) {
        auto vec = new omniruntime::vec::Vector<int64_t>(rows);
        for (int i = 0; i < rows; ++i) {
            vec->SetValue(i, col * 50 + (i % 4));
        }
        vBatch->Append(vec);
    }

    return vBatch;
}

// Test: Basic functionality of open().
TEST(FastTop1FunctionTest, OpenInitializationWithTwoPKeys)
{
    // Initialize the FastTop1Function with mock configuration.
    std::string description = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "VARCHAR(2147483647)", "BIGINT", "BIGINT"],
        "partitionKey": [0, 1],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=1",
        "generateUpdateBefore": false,
        "processFunction": "FastTop1Function",
        "sortFieldIndices": [2, 3],
        "sortAscendingOrders": [false, true],
        "sortNullsIsLast": [true, false]
    })DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    KeyedProcessFunction<RowData*, RowData*, RowData*>* top1Function =
        reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(
            new FastTop1Function<RowData*>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    auto* op = new KeyedProcessOperator(top1Function, new BatchOutputTest(), newRankConfig);
    op->setup();
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
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT),
        omnistream::RowField("col3", BasicLogicalType::BIGINT),
        omnistream::RowField("col4", BasicLogicalType::BIGINT),
        omnistream::RowField("col5", BasicLogicalType::BIGINT),
        omnistream::RowField("col6", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());
}

TEST(FastTop1FunctionTest, ProcessSingleBatchWithTwoPKeys)
{
    // Initialize the FastTop1Function with mock configuration.
    std::string description = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "VARCHAR(2147483647)", "BIGINT", "BIGINT"],
        "partitionKey": [0, 1],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=1",
        "generateUpdateBefore": false,
        "processFunction": "FastTop1Function",
        "sortFieldIndices": [3],
        "sortAscendingOrders": [true],
        "sortNullsIsLast": [true, false]
    })DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    KeyedProcessFunction<RowData*, RowData*, RowData*>* top1Function =
        reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(
            new FastTop1Function<RowData*>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(top1Function, output, newRankConfig);
    op->setup();
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
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col2", BasicLogicalType::BIGINT),
         omnistream::RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto record = new StreamRecord(createTwoKeyVectorBatch());
    op->processBatch(record);
    int rowCnt = 4;
    auto expectedBatch = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0 = {1000, 1002, 1000, 1004}; // partition key 1
    std::vector<int64_t> col1 = {1001, 1003, 1002, 1005}; // partition key 2
    std::vector<int64_t> col2 = {200, 200, 201, 200};
    std::vector<int64_t> col3 = {300, 300, 301, 300};

    auto Vec0 = omniruntime::TestUtil::CreateVector(rowCnt, col0.data());
    auto Vec1 = omniruntime::TestUtil::CreateVector(rowCnt, col1.data());
    auto Vec2 = omniruntime::TestUtil::CreateVector(rowCnt, col2.data());
    auto Vec3 = omniruntime::TestUtil::CreateVector(rowCnt, col3.data());

    expectedBatch->Append(Vec0);
    expectedBatch->Append(Vec1);
    expectedBatch->Append(Vec2);
    expectedBatch->Append(Vec3);

    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedBatch);
    EXPECT_EQ(matched, true);

    delete expectedBatch;
}

TEST(FastTop1FunctionTest, ProcessMultipleBatchesWithTwoPKeys)
{
    // Initialize the FastTop1Function with mock configuration.
    std::string description = R"DELIM({
        "originDescription": null,
        "inputTypes": ["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
        "outputTypes": ["BIGINT", "VARCHAR(2147483647)", "BIGINT", "BIGINT"],
        "partitionKey": [0, 1],
        "outputRankNumber": false,
        "rankRange": "rankStart=1, rankEnd=1",
        "generateUpdateBefore": false,
        "processFunction": "FastTop1Function",
        "sortFieldIndices": [3],
        "sortAscendingOrders": [true],
        "sortNullsIsLast": [true, false]
    })DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    KeyedProcessFunction<RowData*, RowData*, RowData*>* top1Function =
        reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(
            new FastTop1Function<RowData*>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(top1Function, output, newRankConfig);
    op->setup();
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
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT),
         omnistream::RowField("col2", BasicLogicalType::BIGINT),
         omnistream::RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto vecBatch = createTwoKeyVectorBatch();
    auto record = new StreamRecord(vecBatch);
    op->processBatch(record);

    int rowCnt = 4;
    auto expectedBatch = new omnistream::VectorBatch(rowCnt);

    std::vector<int64_t> col0 = {1000, 1002, 1000, 1004}; // partition key 1
    std::vector<int64_t> col1 = {1001, 1003, 1002, 1005}; // partition key 2
    std::vector<int64_t> col2 = {200, 200, 201, 200};
    std::vector<int64_t> col3 = {300, 300, 301, 300};

    auto Vec0 = omniruntime::TestUtil::CreateVector(rowCnt, col0.data());
    auto Vec1 = omniruntime::TestUtil::CreateVector(rowCnt, col1.data());
    auto Vec2 = omniruntime::TestUtil::CreateVector(rowCnt, col2.data());
    auto Vec3 = omniruntime::TestUtil::CreateVector(rowCnt, col3.data());

    expectedBatch->Append(Vec0);
    expectedBatch->Append(Vec1);
    expectedBatch->Append(Vec2);
    expectedBatch->Append(Vec3);

    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedBatch);
    EXPECT_EQ(matched, true);
    //   delete vecBatch;
    delete output->getStreamRecord();

    auto vecBatch2 = createTwoKeyVectorBatch2();
    auto record2 = new StreamRecord(vecBatch2);
    op->processBatch(record2);

    expectedBatch = new omnistream::VectorBatch(rowCnt);

    col0 = {1000, 1002, 1000, 1004}; // partition key 1
    col1 = {1001, 1003, 1002, 1005}; // partition key 2
    col2 = {100, 101, 100, 101};
    col3 = {150, 151, 150, 151};

    Vec0 = omniruntime::TestUtil::CreateVector(rowCnt, col0.data());
    Vec1 = omniruntime::TestUtil::CreateVector(rowCnt, col1.data());
    Vec2 = omniruntime::TestUtil::CreateVector(rowCnt, col2.data());
    Vec3 = omniruntime::TestUtil::CreateVector(rowCnt, col3.data());

    expectedBatch->Append(Vec0);
    expectedBatch->Append(Vec1);
    expectedBatch->Append(Vec2);
    expectedBatch->Append(Vec3);

    matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedBatch);
    EXPECT_EQ(matched, true);
    //   delete vecBatch2;
    delete output->getStreamRecord();

    delete expectedBatch;
}
