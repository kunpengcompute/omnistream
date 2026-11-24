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
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <optional>

#include "test/core/operators/OutputTest.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "api/common/TaskInfoImpl.h"
#include "table/runtime/operators/join/stream/StreamingJoinOperator.h"
#include "streaming/api/operators/StreamCalcBatch.h"

namespace omnistream {
VectorBatch* createVectorBatchFromCsv(
    std::string filepath, int nrow, int ncol, std::optional<std::vector<std::string>> colTypes = std::nullopt)
{
    VectorBatch* result = new omnistream::VectorBatch(nrow);
    result->ResizeVectorCount(ncol);
    for (int i = 0; i < ncol; i++) {
        if (colTypes && (*colTypes)[i] == "STRING") {
            auto vec = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(nrow);
            result->SetVector(i, vec);
        } else {
            auto vec = new omniruntime::vec::Vector<int64_t>(nrow);
            result->SetVector(i, vec);
        }
    }
    std::ifstream file(filepath); // Replace with your file path
    if (!file.is_open()) {
        std::cerr << "Error opening file\n";
        return nullptr;
    }
    std::string line;
    int irow = 0;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string value;
        std::vector<std::string> row;

        while (std::getline(ss, value, ',')) { // Assuming CSV is comma-separated
            row.push_back(value);
        }
        // Store values in respective column vectors
        for (size_t i = 0; i < row.size(); ++i) {
            if (colTypes && (*colTypes)[i] == "STRING") {
                std::string str = row[i];
                std::string_view value(str.data(), str.size());
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    result->GetVectors()[i])
                    ->SetValue(irow, value);
            } else {
                reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(result->GetVectors()[i])
                    ->SetValue(irow, std::stoll(row[i]));
            }
        }
        irow++;
    }
    file.close();
    return result;
}

omnistream::VectorBatch* BuildStringVectorBatch(std::string* strings, int rowCount)
{
    auto vector = omniruntime::TestUtil::CreateVarcharVector(strings, rowCount);

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCount);
    vb->Append(vector);

    return vb;
}

std::string Q4ConfigStr_calc =
    R"delimiter({
        "name" : "Calc(select=[id, category, price])",
        "description":{
            "originDescription":null,
            "inputTypes":["BIGINT","TIMESTAMP(3)","TIMESTAMP(3)","BIGINT","BIGINT","TIMESTAMP(3)"],
            "outputTypes":["BIGINT","BIGINT","BIGINT"],
            "indices":[
                {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0},
                {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":3},
                {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":4}],
            "condition":null
        },
        "id":"StreamExecCalc"
    })delimiter";

std::string Q4ConfigStr_join =
    R"delimiter({
        "name":"Join[5]",
        "description":{
            "filterNulls":[true],
            "rightJoinKey":[0],
            "nonEquiCondition": {"exprType":"BINARY","returnType":4,"operator":"AND",
                "left":{"exprType":"BINARY","returnType":4,"operator":"GREATER_THAN_OR_EQUAL",
                    "left":{"exprType":"FIELD_REFERENCE","dataType":12,"colVal":5},
                    "right":{"exprType":"FIELD_REFERENCE","dataType":12,"colVal":1}
                },
                "right":{"exprType":"BINARY","returnType":4,"operator":"LESS_THAN_OR_EQUAL",
                    "left":{"exprType":"FIELD_REFERENCE","dataType":12,"colVal":5},
                    "right":{"exprType":"FIELD_REFERENCE","dataType":12,"colVal":2}
                }
            },
            "joinType":"InnerJoin",
            "rightInputTypes":["BIGINT","BIGINT","TIMESTAMP(3)"],
            "outputTypes":["BIGINT","TIMESTAMP(3)","TIMESTAMP(3)","BIGINT","BIGINT","TIMESTAMP(3)"],
            "leftInputTypes":["BIGINT","TIMESTAMP(3)","TIMESTAMP(3)"],
            "leftJoinKey":[0],
            "leftInputSpec":"NoUniqueKey",
            "rightInputSpec":"NoUniqueKey",
            "originDescription":"[5]:Join(joinType=[InnerJoin], where=[((id = auction) AND (dateTime0 >= dateTime) AND (dateTime0 <= expires))], select=[id, dateTime, expires, category, auction, price, dateTime0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])"
        },
        "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
    })delimiter";

std::string Q20ConfigStr_calc =
    R"delimiter({
        "name" : "Calc(select=[auction, bidder, price, channel, url, B.dateTime, B.extra, itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra])",
        "description":{
            "originDescription": null,
            "inputTypes": ["BIGINT", "BIGINT", "BIGINT", "STRING", "STRING", "TIMESTAMP(3)", "STRING", "BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "TIMESTAMP(3)", "TIMESTAMP(3)", "BIGINT", "STRING"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "STRING", "STRING", "TIMESTAMP(3)", "STRING", "STRING", "STRING", "BIGINT", "BIGINT", "TIMESTAMP(3)", "TIMESTAMP(3)", "BIGINT", "BIGINT", "STRING"],
            "indices": [
                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 0},
                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 1},
                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 2},
                {"exprType": "FIELD_REFERENCE", "dataType": 15, "width": 2147483647, "colVal": 3},
                {"exprType": "FIELD_REFERENCE", "dataType": 15, "width": 2147483647, "colVal": 4},
                {"exprType": "FIELD_REFERENCE", "dataType": 12, "colVal": 5},
                {"exprType": "FIELD_REFERENCE", "dataType": 15, "width": 2147483647, "colVal": 6},
                {"exprType": "FIELD_REFERENCE", "dataType": 15, "width": 2147483647, "colVal": 8},
                {"exprType": "FIELD_REFERENCE", "dataType": 15, "width": 2147483647, "colVal": 9},
                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 10},
                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 11},
                {"exprType": "FIELD_REFERENCE", "dataType": 12, "colVal": 12},
                {"exprType": "FIELD_REFERENCE", "dataType": 12, "colVal": 13},
                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 14},
                {"exprType": "LITERAL", "dataType": 2, "isNull": false, "value": 10},
                {"exprType": "FIELD_REFERENCE", "dataType": 15, "width": 2147483647, "colVal": 15}
            ],
            "condition": null
        },
        "id":"StreamExecCalc"
    })delimiter";

std::string Q20ConfigStr_join =
    R"delimiter({
        "name":"Join[10]",
        "description":{
            "originDescription": null,
            "leftInputTypes": ["BIGINT", "BIGINT", "BIGINT", "STRING", "STRING", "TIMESTAMP(3)", "STRING"],
            "rightInputTypes": ["BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "TIMESTAMP(3)", "TIMESTAMP(3)", "BIGINT", "STRING"],
            "outputTypes": ["BIGINT", "BIGINT", "BIGINT", "STRING", "STRING", "TIMESTAMP(3)", "STRING", "BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "TIMESTAMP(3)", "TIMESTAMP(3)", "BIGINT", "STRING"],
            "leftJoinKey": [0],
            "rightJoinKey": [0],
            "nonEquiCondition": null,
            "joinType": "InnerJoin",
            "filterNulls": [true],
            "leftInputSpec": "NoUniqueKey",
            "rightInputSpec": "NoUniqueKey",
            "leftUniqueKeys": [],
            "rightUniqueKeys": []
        },
        "id":"org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator"
    })delimiter";

std::string CountChar_desc =
    R"delimiter({
        "name" : "Calc()",
        "description":{
            "originDescription": null,
            "inputTypes": ["STRING", "STRING"],
            "outputTypes": ["BIGINT"],
            "indices": [
                {"exprType":"FUNCTION","returnType":2,"function_name":"CountChar", "arguments":[{"exprType":"FIELD_REFERENCE","dataType":15,"colVal":0,"width":200}, {"exprType": "LITERAL" ,"dataType":16,"isNull":false,"value":"a", "width":200}]}
            ],
            "condition": null
        },
        "id":"StreamExecCalc"
    })delimiter";

std::string SplitIndex_desc =
    R"delimiter({
        "name" : "Calc()",
        "description":{
            "originDescription": null,
            "inputTypes": ["STRING"],
            "outputTypes": ["STRING"],
            "indices": [
                {"exprType":"FUNCTION","returnType":15,"function_name":"SplitIndex", "arguments":[{"exprType":"FIELD_REFERENCE","dataType":15,"colVal":0,"width":200},
                                                                                                {"exprType": "LITERAL" ,"dataType":16,"isNull":false,"value":",", "width":200},
                                                                                                {"exprType": "LITERAL" ,"dataType":1,"isNull":false,"value":1}]
                }
            ],
            "condition": null
        },
        "id":"StreamExecCalc"
    })delimiter";

/*TEST(OperatorChainTest, DISABLED_CountCharTest)
{
    // 1. Setup operatorChain
    std::vector<nlohmann::json> operatorDescriptions{nlohmann::json::parse(CountChar_desc)};
    std::vector<omnistream::OperatorConfig> opChainConfig;
    for (int i = 0; i < operatorDescriptions.size(); i++) {
        auto parsedJson = operatorDescriptions[i];
        omnistream::OperatorConfig opConfig(
                parsedJson["id"],
                parsedJson["name"],
                parsedJson["description"]["inputType"],
                parsedJson["description"]["outputType"],
                parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }
    // The last output, a dummy test output
    BatchOutputTest *output = new BatchOutputTest();
    OperatorChain* chain = new OperatorChain(opChainConfig);
    StreamOperator * headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new
TaskInfoImpl("CountCharChainTest", 1, 1, 0))); chain->initializeStateAndOpenOperators(initializer);

    // 2. Feed input to operator chain (give it to head operator. The process happens recursively)
    auto calcOp = dynamic_cast<StreamCalcBatch *>(headOp);
    VectorBatch* testVectorBatch = createVectorBatchFromCsv("input/test_countchar.csv", 4, 1,
operatorDescriptions[0]["description"]["inputTypes"]); calcOp->processBatch(new StreamRecord(testVectorBatch));

    // 3. Collect output from the output of the tail
    VectorBatch* outputVB = reinterpret_cast<VectorBatch*> (output->getVectorBatch());

    int rowCnt = 4;
    std::vector<int64_t> col0 = {2, 2, 1, 1};
    omnistream::VectorBatch* expectedVB = new omnistream::VectorBatch(rowCnt);
    expectedVB->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
}

TEST(OperatorChainTest, DISABLED_SplitIndexTest)
{
    // 1. Setup operatorChain
    std::vector<nlohmann::json> operatorDescriptions{nlohmann::json::parse(SplitIndex_desc)};
    std::vector<omnistream::OperatorConfig> opChainConfig;
    for (int i = 0; i < operatorDescriptions.size(); i++) {
        auto parsedJson = operatorDescriptions[i];
        omnistream::OperatorConfig opConfig(
                parsedJson["id"],
                parsedJson["name"],
                parsedJson["description"]["inputType"],
                parsedJson["description"]["outputType"],
                parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }
    // The last output, a dummy test output
    BatchOutputTest *output = new BatchOutputTest();
    OperatorChain* chain = new OperatorChain(opChainConfig);
    StreamOperator * headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new
TaskInfoImpl("SplitIndexChainTest", 1, 1, 0))); chain->initializeStateAndOpenOperators(initializer);

    auto calcOp = dynamic_cast<StreamCalcBatch *>(headOp);

    int rowCnt = 4;
    std::string inputStr[rowCnt] = {"Jack,John,Mary", "Jack,Johnny,Mary", "Jack,Mary", "Jack"};
    auto inputVB = BuildStringVectorBatch(inputStr, rowCnt);
    calcOp->processBatch(new StreamRecord(inputVB));

    // 3. Collect output from the output of the tail
    VectorBatch* outputVB = reinterpret_cast<VectorBatch*> (output->getVectorBatch());

    std::string str5 = "John";
    std::string str6 = "Johnny";
    std::string str7 = "Mary";
    std::string str8 = "";
    std::string outputStr[rowCnt] = {"John", "Johnny", "Mary", ""};
    auto expectedVB = BuildStringVectorBatch(outputStr, rowCnt);
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);
}



TEST(OperatorChainTest, DISABLED_Q4JoinProj)
{
    // 1. Setup operatorChain
    std::vector<std::string> operatorDescriptors{Q4ConfigStr_join, Q4ConfigStr_calc};
    std::vector<omnistream::OperatorConfig> opChainConfig;
    for (int i = 0; i < operatorDescriptors.size(); i++) {
        auto desc = operatorDescriptors[i];
        nlohmann::json parsedJson = nlohmann::json::parse(desc);
        omnistream::OperatorConfig opConfig(
                parsedJson["id"],  // uniqueName:
                parsedJson["name"], // Name
                parsedJson["description"]["inputType"],
                parsedJson["description"]["outputType"],
                parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }
    // The last output, a dummy test output
    BatchOutputTest *output = new BatchOutputTest();
    OperatorChain* chain = new OperatorChain(opChainConfig);
    StreamOperator * headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new
TaskInfoImpl("Q4OperatorChainTest", 1, 1, 0))); chain->initializeStateAndOpenOperators(initializer);

    // 2. Feed input to operator chain (give it to head operator. The process happens recursively)
    auto joinOp = dynamic_cast<StreamingJoinOperator<int64_t>*>(headOp);
    // left table
    VectorBatch* vectorBatchLeft = createVectorBatchFromCsv("input/genauction_small.csv", 11, 3);
    joinOp->processBatch1(new StreamRecord(vectorBatchLeft));
    // right table
    VectorBatch* vectorBatchRight = createVectorBatchFromCsv("input/genbid_small.csv", 150, 3);
    joinOp->processBatch2(new StreamRecord(vectorBatchRight));

    // 3. Collect output from the output of the tail
    VectorBatch* outputVB = reinterpret_cast<VectorBatch*> (output->getVectorBatch());

    auto expectedVB = createVectorBatchFromCsv("input/result_small.csv", 19, 3);
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB);
    EXPECT_TRUE(matched);

    vectorBatchLeft->FreeAllVectors();
    vectorBatchRight->FreeAllVectors();
    outputVB->FreeAllVectors();
}


TEST(OperatorChainTest, DISABLED_Q20JoinProj)
{
    // 1. Setup operatorChain
    std::vector<nlohmann::json> operatorDescriptions{nlohmann::json::parse(Q20ConfigStr_join),
nlohmann::json::parse(Q20ConfigStr_calc)}; std::vector<omnistream::OperatorConfig> opChainConfig; for (int i = 0; i <
operatorDescriptions.size(); i++) { auto parsedJson = operatorDescriptions[i]; omnistream::OperatorConfig opConfig(
                parsedJson["id"],
                parsedJson["name"],
                parsedJson["description"]["inputType"],
                parsedJson["description"]["outputType"],
                parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }
    // The last output, a dummy test output
    BatchOutputTest *output = new BatchOutputTest();
    // OperatorChain* chain;
    OperatorChain* chain = new OperatorChain(opChainConfig);
    StreamOperator * headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new
TaskInfoImpl("Q20OperatorChainTest", 1, 1, 0))); chain->initializeStateAndOpenOperators(initializer);

    // 2. Feed input to operator chain (give it to head operator. The process happens recursively)
    auto joinOp = dynamic_cast<StreamingJoinOperator<int64_t>*>(headOp);

    // left table
    VectorBatch* vectorBatchLeft = createVectorBatchFromCsv("input/q20_auction.csv", 11, 7,
operatorDescriptions[0]["description"]["leftInputTypes"]);

    joinOp->processBatch1(new StreamRecord(vectorBatchLeft));

    // right table
    VectorBatch* vectorBatchRight = createVectorBatchFromCsv("input/q20_bid.csv", 150, 9,
operatorDescriptions[0]["description"]["rightInputTypes"]); joinOp->processBatch2(new StreamRecord(vectorBatchRight));

    // // 3. Collect output from the output of the tail
    VectorBatch* outputVB = reinterpret_cast<VectorBatch*> (output->getVectorBatch());
    auto expectedVB = createVectorBatchFromCsv("input/q20_result.csv", 46, 16,
operatorDescriptions[1]["description"]["outputTypes"]); // calc output bool matched =
omniruntime::TestUtil::VecBatchMatch(outputVB, expectedVB); EXPECT_TRUE(matched);

    vectorBatchLeft->FreeAllVectors();
    vectorBatchRight->FreeAllVectors();
    outputVB->FreeAllVectors();
}*/
} // namespace omnistream
