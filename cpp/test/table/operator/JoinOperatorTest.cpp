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
#include <nlohmann/json.hpp>
#include "table/runtime/operators/join/stream/StreamingJoinOperator.h"
#include "table/runtime/operators/join/stream/AbstractStreamingJoinOperator.h"

#include "test/core/operators/OutputTest.h"

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "runtime/state/TaskStateManager.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"

std::string Q4ConfigStr =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "precision":3, "type":"TIMESTAMP", "timestampKind":0},
                    {"kind":"logical", "isNull":true, "precision":3, "type":"TIMESTAMP", "timestampKind":0},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "precision":3, "type":"TIMESTAMP", "timestampKind":0}]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "precision":3, "type":"TIMESTAMP", "timestampKind":0},
                            {"kind":"logical", "isNull":true, "precision":3, "type":"TIMESTAMP", "timestampKind":0},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "precision":3, "type":"TIMESTAMP", "timestampKind":0}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "nonEquiCondition": {"exprType":"BINARY", "left":{"exprType":"FIELD_REFERENCE", "dataType":9, "colVal":3}, "returnType":2, "right":{"exprType":"FIELD_REFERENCE", "dataType":9, "colVal":1}, "operator":"GREATER_THAN_OR_EQUAL"},
                "joinType":"InnerJoin",
                "rightInputTypes":["BIGINT", "BIGINT", "TIMESTAMP(3)"],
                "outputTypes":["BIGINT", "TIMESTAMP(3)", "TIMESTAMP(3)", "BIGINT", "BIGINT", "BIGINT", "TIMESTAMP(3)"],
                "leftInputTypes":["BIGINT", "TIMESTAMP(3)", "TIMESTAMP(3)", "BIGINT"],
                "leftJoinKey":[0],
                "leftInputSpec":"NoUniqueKey",
                "rightInputSpec":"NoUniqueKey",
                "originDescription":"[5]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, dateTime, expires, category, auction, price, dateTime0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";

std::string simpleConfigStr_nofilter =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"}
                ]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "rightUniqueKeys":[],
                "nonEquiCondition": null,
                "joinType":"InnerJoin",
                "rightInputTypes":["BIGINT", "BIGINT"],
                "outputTypes":["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                "leftInputTypes":["BIGINT", "BIGINT"],
                "leftJoinKey":[0],
                "leftInputSpec":"NoUniqueKey",
                "rightInputSpec":"NoUniqueKey",
                "leftUniqueKeys":[],
                "originDescription":"[5]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, dateTime, expires, category, auction, price, dateTime0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";
std::string simpleConfigStr_hasUniqueKey_filter =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"}
                ]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "rightUniqueKeys":[[0, 1]],
                "nonEquiCondition": {"exprType":"BINARY", "left":{"exprType":"FIELD_REFERENCE", "dataType":9, "colVal":4}, "returnType":2, "right":{"exprType":"FIELD_REFERENCE", "dataType":9, "colVal":1}, "operator":"GREATER_THAN_OR_EQUAL"},
                "joinType":"InnerJoin",
                "rightInputTypes":["BIGINT", "BIGINT"],
                "outputTypes":["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                "leftInputTypes":["BIGINT", "BIGINT", "BIGINT"],
                "leftJoinKey":[0],
                "leftUniqueKeys":[[0, 2]],
                "leftInputSpec":"HasUniqueKey",
                "rightInputSpec":"HasUniqueKey",
                "originDescription":"[5]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, dateTime, expires, category, auction, price, dateTime0], leftInputSpec=[HasUniqueKey], rightInputSpec=[HasUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";

std::string simpleConfigStr_hasUniqueKey =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"}
                ]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "rightUniqueKeys":[[0, 1]],
                "nonEquiCondition": null,
                "joinType":"InnerJoin",
                "rightInputTypes":["BIGINT", "BIGINT"],
                "outputTypes":["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                "leftInputTypes":["BIGINT", "BIGINT", "BIGINT"],
                "leftJoinKey":[0],
                "leftUniqueKeys":[[0, 2]],
                "leftInputSpec":"HasUniqueKey",
                "rightInputSpec":"HasUniqueKey",
                "originDescription":"[5]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, dateTime, expires, category, auction, price, dateTime0], leftInputSpec=[HasUniqueKey], rightInputSpec=[HasUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";
std::string simpleConfigStr_joinKeyContainsUniqueKey_filter =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"}
                ]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "rightUniqueKeys":[[0]],
                "nonEquiCondition": {"exprType":"BINARY", "left":{"exprType":"FIELD_REFERENCE", "dataType":9, "colVal":4}, "returnType":2, "right":{"exprType":"FIELD_REFERENCE", "dataType":9, "colVal":1}, "operator":"GREATER_THAN_OR_EQUAL"},
                "joinType":"InnerJoin",
                "rightInputTypes":["BIGINT", "BIGINT"],
                "outputTypes":["BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                "leftInputTypes":["BIGINT", "BIGINT", "BIGINT"],
                "leftJoinKey":[0],
                "leftUniqueKeys":[[0]],
                "leftInputSpec":"JoinKeyContainsUniqueKey",
                "rightInputSpec":"JoinKeyContainsUniqueKey",
                "originDescription":"[5]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, dateTime, expires, category, auction, price, dateTime0], leftInputSpec=[HasUniqueKey], rightInputSpec=[HasUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";

std::string simpleLeftJoin =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"}
                ]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "rightUniqueKeys":[],
                "nonEquiCondition": null,
                "joinType":"LeftOuterJoin",
                "rightInputTypes":["BIGINT", "BIGINT"],
                "outputTypes":["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                "leftInputTypes":["BIGINT", "BIGINT"],
                "leftJoinKey":[0],
                "leftInputSpec":"NoUniqueKey",
                "rightInputSpec":"NoUniqueKey",
                "leftUniqueKeys":[],
                "originDescription":"[5]:Join(joinType=[LeftOuterJoin], where=[(auction = auction0)], select=[auction, bidder, auction0, bidder0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";

std::string simpleRightJoin =
    R"delimiter({
            "output":
            {
                "kind":"Row",
                "type":[
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"},
                    {"kind":"logical", "isNull":true, "type":"BIGINT"}
                ]
            },
            "inputs":[
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    },
                    {
                        "kind":"Row",
                        "type":[
                            {"kind":"logical", "isNull":true, "type":"BIGINT"},
                            {"kind":"logical", "isNull":true, "type":"BIGINT"}
                        ]
                    }
            ],
            "name":"Join[5]",
            "description":{
                "filterNulls":[true],
                "rightJoinKey":[0],
                "rightUniqueKeys":[],
                "nonEquiCondition": null,
                "joinType":"RightOuterJoin",
                "rightInputTypes":["BIGINT", "BIGINT"],
                "outputTypes":["BIGINT", "BIGINT", "BIGINT", "BIGINT"],
                "leftInputTypes":["BIGINT", "BIGINT"],
                "leftJoinKey":[0],
                "leftInputSpec":"NoUniqueKey",
                "rightInputSpec":"NoUniqueKey",
                "leftUniqueKeys":[],
                "originDescription":"[5]:Join(joinType=[LeftOuterJoin], where=[(auction = auction0)], select=[auction, bidder, auction0, bidder0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])"
                },
            "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
        })delimiter";

omnistream::VectorBatch* createVectorBatch(const std::vector<std::vector<int64_t>> rowData)
{
    int numRows = rowData.size();
    int numCols = rowData[0].size();

    omnistream::VectorBatch* batch = new omnistream::VectorBatch(numRows);

    // this is transposed
    for (int col = 0; col < numCols; ++col) {
        auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
        for (int row = 0; row < numRows; ++row) {
            vector->SetValue(row, rowData[row][col]);
        }
        batch->Append(vector);
    }

    return batch;
}

TEST(InnerJoinTest, SimpleInnerJoinLongKeyInsertDelete)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleConfigStr_nofilter);
    omnistream::OperatorConfig opConfig(
        parsedJson["id"],  // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

    // create streamOperatorFactory
    // initiate keyedState
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
    // This typeInfo is for Key Serializer.
    std::vector<omnistream::RowField> typeInfo{omnistream::RowField("col0", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<RowData*>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 1}, {2, 2}};

    std::vector<std::vector<int64_t>> rightTable = {
        {2, 1},  // INSERT
        {8, 8},  // INSERT
        {2, 6},  // INSERT
        {2, 1}}; // DELETE

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }
    // the last row is DELETE
    vectorBatchRight->setRowKind(3, RowKind::DELETE);

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);
    auto expectedVB = createVectorBatch({
        {2, 2, 2, 1},
        {2, 1, 2, 1},
        {2, 2, 2, 6},
        {2, 1, 2, 6},
        {2, 2, 2, 1},
        {2, 1, 2, 1},
    });
    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }
    expectedVB->setRowKind(4, RowKind::DELETE);
    expectedVB->setRowKind(5, RowKind::DELETE);

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }
    // StreamingJoinOperator consumes the input VectorBatch through StreamRecord.
    delete outputVB;
    delete expectedVB;
}

TEST(InnerJoinTest, SimpleJoinWithNonEquiCondition)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleConfigStr_nofilter);
    parsedJson["description"]["nonEquiCondition"] = {
        {"exprType", "BINARY"},
        {"returnType", 4},
        {"operator", "GREATER_THAN_OR_EQUAL"},
        {"left", {{"exprType", "FIELD_REFERENCE"}, {"dataType", 1}, {"colVal", 0}}},
        {"right", {{"exprType", "FIELD_REFERENCE"}, {"dataType", 1}, {"colVal", 3}}}};

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();
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
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<RowData*>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 1}, {2, 2}};

    std::vector<std::vector<int64_t>> rightTable = {{2, 1}, {8, 8}, {2, 6}, {1, 4}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }
    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);

    auto expectedVB = createVectorBatch({{2, 2, 2, 1}, {2, 1, 2, 1}});

    for (int i = 0; i < 2; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 2);
    for (int i = 0; i < 2; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }
    // StreamingJoinOperator consumes the input VectorBatch through StreamRecord.
    delete outputVB;
    delete expectedVB;
}

TEST(InnerJoinTest, SimpleJoinWithNullKey)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleConfigStr_nofilter);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

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
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 1}, {2, 2}};

    std::vector<std::vector<int64_t>> rightTable = {{2, 1}, {8, 8}, {2, 6}, {1, 4}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectorBatchRight->Get(0))->SetNull(2);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectorBatchLeft->Get(0))->SetNull(2);

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);

    auto expectedVB = createVectorBatch({{2, 2, 2, 1}, {1, 3, 1, 4}, {1, 2, 1, 4}});

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    delete outputVB;
    delete expectedVB;
}

TEST(InnerJoinTest, InnerJoin)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleConfigStr_nofilter);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

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
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 7}, {2, 8}};

    std::vector<std::vector<int64_t>> rightTable = {{2, 1}, {1, 4}, {2, 6}, {8, 8}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);

    auto expectedVB =
        createVectorBatch({{2, 8, 2, 1}, {2, 7, 2, 1}, {1, 3, 1, 4}, {1, 2, 1, 4}, {2, 8, 2, 6}, {2, 7, 2, 6}});

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 6);
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    // StreamingJoinOperator consumes the input VectorBatch through StreamRecord.
    delete outputVB;
    delete expectedVB;
}

TEST(InnerJoinTest, InnerJoinAdvance)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleConfigStr_nofilter);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

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
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}};

    std::vector<std::vector<int64_t>> rightTable = {{1, 4}, {2, 5}, {1, 6}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);

    auto expectedVB = createVectorBatch({{{1, 3, 1, 4}, {1, 2, 1, 4}, {1, 3, 1, 6}, {1, 2, 1, 6}}});

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 4);
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    leftTable = {{2, 1}, {3, 4}};

    rightTable = {{4, 7}, {4, 8}, {2, 9}, {1, 9}};

    vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    leftRecord = new StreamRecord(vectorBatchLeft);
    rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);

    expectedVB = createVectorBatch({{2, 1, 2, 5}});

    for (int i = 0; i < 1; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 1);
    for (int i = 0; i < 1; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    op->processBatch2(rightRecord);

    expectedVB = createVectorBatch({{2, 1, 2, 9}, {1, 3, 1, 9}, {1, 2, 1, 9}});

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 3);
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    // StreamingJoinOperator consumes the input VectorBatch through StreamRecord.
    delete outputVB;
    delete expectedVB;
}

TEST(OuterJoinTest, LeftJoin)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleLeftJoin);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

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
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 1}, {3, 4}};

    std::vector<std::vector<int64_t>> rightTable = {{1, 4}, {2, 5}, {1, 6}, {4, 7}, {4, 8}, {2, 9}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);

    // op->processBatch2(rightRecord);
    op->processBatch1(leftRecord);

    auto expectedVB = createVectorBatch({{1, 2, 1, 4}, {1, 3, 1, 6}, {2, 1, 1, 4}, {3, 4, 1, 6}});

    for (int i = 0; i < 4; i++) {
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(i);
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(i);
    }

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    // EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 4);
    for (int i = 0; i < outputVB->GetRowCount(); i++) {
        // EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
        std::cout << to_string(outputVB->getRowKind(i)) << std::endl;
    }

    omniruntime::vec::VectorHelper::PrintVecBatch(outputVB);

    op->processBatch2(rightRecord);
    // op->processBatch1(leftRecord);

    expectedVB = createVectorBatch(
        {{1, 3, 1, 4},
         {1, 2, 1, 4},
         {2, 1, 2, 5},
         {1, 3, 1, 6},
         {1, 2, 1, 6},
         {2, 1, 2, 9},
         {1, 3, 0, 0},
         {1, 2, 0, 0},
         {2, 1, 0, 0}});

    for (int i = 6; i < 9; i++) {
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(i);
        reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(i);
    }

    for (int i = 0; i < 6; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }
    for (int i = 6; i < 9; i++) {
        expectedVB->setRowKind(i, RowKind::DELETE);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    // EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 9);
    for (int i = 0; i < outputVB->GetRowCount(); i++) {
        // EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
        std::cout << to_string(outputVB->getRowKind(i)) << std::endl;
    }

    omniruntime::vec::VectorHelper::PrintVecBatch(outputVB);

    // StreamingJoinOperator consumes the input VectorBatch through StreamRecord.
    delete outputVB;
    delete expectedVB;
}

TEST(OuterJoinTest, DISABLED_RightJoin)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleRightJoin);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT),
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 1}, {3, 4}};

    std::vector<std::vector<int64_t>> rightTable = {{1, 4}, {2, 5}, {1, 6}, {4, 7}, {4, 8}, {2, 9}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    // op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);
    op->processBatch1(leftRecord);

    auto expectedVB = createVectorBatch(
        {{1, 2, 1, 4},
         {1, 3, 1, 4},
         {2, 1, 2, 5},
         {1, 2, 1, 6},
         {1, 3, 1, 6},
         {0, 0, 4, 7},
         {0, 0, 4, 8},
         {2, 1, 2, 9}});

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(0))->SetNull(5);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(0))->SetNull(6);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(1))->SetNull(5);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(1))->SetNull(6);

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 8);
    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }
    // StreamingJoinOperator consumes both input batches through StreamRecord.
    if (outputVB) {
        delete outputVB;
    }
    if (expectedVB) {
        delete expectedVB;
    }
}

TEST(OuterJoinTest, LeftJoinAdvance)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleLeftJoin);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

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
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}};

    std::vector<std::vector<int64_t>> rightTable = {{1, 4}, {2, 5}, {1, 6}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);

    op->processBatch1(leftRecord);

    auto expectedVB = createVectorBatch({{1, 2, 0, 0}, {1, 3, 0, 0}});

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(0);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(0);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(1);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(1);

    for (int i = 0; i < 2; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 2);
    for (int i = 0; i < outputVB->GetRowCount(); i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
        std::cout << to_string(outputVB->getRowKind(i)) << std::endl;
    }

    omniruntime::vec::VectorHelper::PrintVecBatch(outputVB);

    op->processBatch2(rightRecord);

    expectedVB =
        createVectorBatch({{1, 3, 0, 0}, {1, 2, 0, 0}, {1, 3, 1, 4}, {1, 2, 1, 4}, {1, 3, 1, 6}, {1, 2, 1, 6}});

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(0);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(0);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(1);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(1);

    expectedVB->setRowKind(0, RowKind::DELETE);
    expectedVB->setRowKind(1, RowKind::DELETE);

    for (int i = 2; i < 6; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());
    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 6);
    for (int i = 0; i < outputVB->GetRowCount(); i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
        std::cout << to_string(outputVB->getRowKind(i)) << std::endl;
    }

    omniruntime::vec::VectorHelper::PrintVecBatch(outputVB);

    leftTable = {{2, 1}, {3, 4}};

    rightTable = {{4, 7}, {4, 8}, {2, 9}, {1, 9}};

    vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    leftRecord = new StreamRecord(vectorBatchLeft);
    rightRecord = new StreamRecord(vectorBatchRight);

    // op->processBatch2(rightRecord);
    op->processBatch1(leftRecord);

    expectedVB = createVectorBatch({{2, 1, 2, 5}, {3, 4, 0, 0}});

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(2))->SetNull(1);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(3))->SetNull(1);

    for (int i = 0; i < 2; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());
    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 2);
    for (int i = 0; i < outputVB->GetRowCount(); i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
        std::cout << to_string(outputVB->getRowKind(i)) << std::endl;
    }
    omniruntime::vec::VectorHelper::PrintVecBatch(outputVB);

    op->processBatch2(rightRecord);
    // op->processBatch1(leftRecord);

    expectedVB = createVectorBatch({{2, 1, 2, 9}, {1, 3, 1, 9}, {1, 2, 1, 9}});

    for (int i = 0; i < 3; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());
    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 3);
    for (int i = 0; i < outputVB->GetRowCount(); i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
        std::cout << to_string(outputVB->getRowKind(i)) << std::endl;
    }
    omniruntime::vec::VectorHelper::PrintVecBatch(outputVB);
    // StreamingJoinOperator consumes both input batches through StreamRecord.
    if (outputVB) {
        delete outputVB;
    }
    if (expectedVB) {
        delete expectedVB;
    }
}

TEST(OuterJoinTest, DISABLED_RightJoinAdvance)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleRightJoin);

    omnistream::OperatorConfig opConfig(
        parsedJson["id"],
        "Group_By_Simple",
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT),
        omnistream::RowField("col3", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<int64_t>(opConfig.getDescription(), outputTest);
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}};

    std::vector<std::vector<int64_t>> rightTable = {{1, 4}, {2, 5}, {1, 6}};

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);

    auto expectedVB = createVectorBatch(
        {{1, 2, 1, 4},
         {1, 3, 1, 4},
         {2, 1, 2, 5},
         {1, 2, 1, 6},
         {1, 3, 1, 6},
         {0, 0, 4, 7},
         {0, 0, 4, 8},
         {2, 1, 2, 8}});

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(0))->SetNull(5);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(0))->SetNull(6);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(1))->SetNull(5);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(1))->SetNull(6);

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 8);
    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    leftTable = {{2, 1}, {3, 4}};

    rightTable = {{4, 7}, {4, 8}, {2, 9}};

    vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }

    leftRecord = new StreamRecord(vectorBatchLeft);
    rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);

    expectedVB = createVectorBatch(
        {{1, 2, 1, 4},
         {1, 3, 1, 4},
         {2, 1, 2, 5},
         {1, 2, 1, 6},
         {1, 3, 1, 6},
         {0, 0, 4, 7},
         {0, 0, 4, 8},
         {2, 1, 2, 8}});

    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(0))->SetNull(5);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(0))->SetNull(6);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(1))->SetNull(5);
    reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(expectedVB->Get(1))->SetNull(6);

    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }

    outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());

    matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);

    EXPECT_TRUE(matched);
    EXPECT_EQ(outputVB->GetRowCount(), 8);
    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }

    // StreamingJoinOperator consumes the input VectorBatch through StreamRecord.
    delete outputVB;
    delete expectedVB;
}

TEST(StreamingJoinTest, DISABLED_SimpleInnerJoinLongKeyInsertDeleteWithRokcsDB)
{
    nlohmann::json parsedJson = nlohmann::json::parse(simpleConfigStr_nofilter);
    omnistream::OperatorConfig opConfig(
        parsedJson["id"],  // uniqueName:
        "Group_By_Simple", // Name
        parsedJson["description"]["inputType"],
        parsedJson["description"]["outputType"],
        parsedJson["description"]);

    BatchOutputTest* outputTest = new BatchOutputTest();

    // create streamOperatorFactory
    // initiate keyedState
    std::vector<std::string> backendHomes = {"/tmp/rocksdb_ut/StreamingJoinTest/"};
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    // This typeInfo is for Key Serializer.
    std::vector<omnistream::RowField> typeInfo{omnistream::RowField("col0", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));

    auto op = new StreamingJoinOperator<RowData*>(opConfig.getDescription(), outputTest);
    op->setDescription(opConfig.getDescription());
    op->setup();
    op->initializeState(initializer, ser);
    op->open();

    std::vector<std::vector<int64_t>> leftTable = {{1, 2}, {1, 3}, {2, 1}, {2, 2}};

    std::vector<std::vector<int64_t>> rightTable = {
        {2, 1},  // INSERT
        {8, 8},  // INSERT
        {2, 6},  // INSERT
        {2, 1}}; // DELETE

    omnistream::VectorBatch* vectorBatchLeft = createVectorBatch(leftTable);
    for (int i = 0; i < leftTable.size(); i++) {
        vectorBatchLeft->setRowKind(i, RowKind::INSERT);
    }
    omnistream::VectorBatch* vectorBatchRight = createVectorBatch(rightTable);
    for (int i = 0; i < rightTable.size(); i++) {
        vectorBatchRight->setRowKind(i, RowKind::INSERT);
    }
    // the last row is DELETE
    vectorBatchRight->setRowKind(3, RowKind::DELETE);

    auto leftRecord = new StreamRecord(vectorBatchLeft);
    auto rightRecord = new StreamRecord(vectorBatchRight);
    op->processBatch1(leftRecord);
    op->processBatch2(rightRecord);
    auto expectedVB =
        createVectorBatch({{2, 1, 2, 1}, {2, 2, 2, 1}, {2, 1, 2, 6}, {2, 2, 2, 6}, {2, 1, 2, 1}, {2, 2, 2, 1}});
    for (int i = 0; i < 4; i++) {
        expectedVB->setRowKind(i, RowKind::INSERT);
    }
    expectedVB->setRowKind(4, RowKind::DELETE);
    expectedVB->setRowKind(5, RowKind::DELETE);

    omnistream::VectorBatch* outputVB = reinterpret_cast<omnistream::VectorBatch*>(outputTest->getVectorBatch());
    bool matched = omniruntime::TestUtil::VecBatchMatchIgnoreOrder(outputVB, expectedVB);
    EXPECT_TRUE(matched);
    for (int i = 0; i < 6; i++) {
        EXPECT_EQ(expectedVB->getRowKind(i), outputVB->getRowKind(i));
    }
    delete outputVB;
    delete expectedVB;
}
