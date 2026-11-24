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

#include "test/core/operators/OutputTest.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "api/common/TaskInfoImpl.h"
#include "runtime/operators/join/lookup/LookupJoinRunner.h"
#include "streaming/api/operators/ProcessOperator.h"

using namespace std;

std::string Q13ConfigStr_calc =
    R"delimiter({
    "name" : "Calc(select=[auction, bidder, price, dateTime, value])",
    "description":{
        "originDescription":null,
        "inputTypes":["BIGINT","BIGINT","BIGINT","TIMESTAMP(3)","BIGINT","BIGINT","VARCHAR"],
        "outputTypes":["BIGINT","BIGINT","BIGINT","TIMESTAMP(3)","VARCHAR"],
        "indices":[
            {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":0},
            {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":1},
            {"exprType":"FIELD_REFERENCE","dataType":2,"colVal":2},
            {"exprType":"FIELD_REFERENCE","dataType":12,"colVal":3},
            {"exprType":"FIELD_REFERENCE","dataType":16,"width":2147483647,"colVal":6}],
        "condition":null
    },
        "id":"StreamExecCalc"
    })delimiter";

std::string Q13ConfigStr_lookupJoin =
    R"delimiter({
        "name":"LookupJoin[4]",
        "description":{
            "originDescription":null,
            "joinType":"InnerJoin",
            "condition":null,
            "projectionOnTemporalTable":[],
            "filterOnTemporalTable":null,
            "inputTypes":["BIGINT","BIGINT","BIGINT","TIMESTAMP(3)","BIGINT"],
            "lookupInputTypes":["BIGINT","VARCHAR"],
            "outputTypes":["BIGINT","BIGINT","BIGINT","TIMESTAMP(3)","BIGINT","BIGINT","VARCHAR"],
            "lookupKeys":{"0":{"index":4}},
            "temporalTableSourceSpec":"table [input/q13_lookupjoinCalcChain.csv]",
            "connectorPath": "input/q13_lookupjoinCalcChain.csv",
            "connectorType": "filesystem",
            "formatType": "csv"
        },
        "id":"org.apache.flink.streaming.api.operators.ProcessOperator"
    })delimiter";

std::string Q13ConfigStr_sink =
    R"delimiter({
        "name":"Q13SinkOperator",
        "id":"org.apache.flink.table.runtime.operators.sink.SinkOperator"
    })delimiter";

/*
 * 100,110,120,1000,1
 * 101,111,121,1000,2
 * 102,112,122,1000,3
 * 103,113,123,1000,4
 */
omnistream::VectorBatch* BuildInputVectorBatchJoin()
{
    int rowCnt = 4;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);
    std::vector<long> col3(rowCnt);
    std::vector<long> col4(rowCnt);

    std::vector<std::string> col1_str(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 100 + i + 1;
        col1[i] = 110 + i + 1;
        col2[i] = 120 + i + 1;
        col3[i] = 1000;
        col4[i] = i + 1;
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col4.data()));
    std::cout << "input vectorbatch created" << std::endl;
    return vb;
}

/*
 * 100,110,120,1000,test_1
 * 101,111,121,1000,test_2
 * 102,112,122,1000,test_3
 * 103,113,123,1000,test_4
 */
inline omnistream::VectorBatch* BuildExpectedVectorBatch()
{
    int rowCnt = 4;
    std::vector<long> col0{101, 102, 103, 104};
    std::vector<long> col1{111, 112, 113, 114};
    std::vector<long> col2{121, 122, 123, 124};
    std::vector<long> col3{1000, 1000, 1000, 1000};
    std::vector<std::string> col4_str{"test_1", "test_2", "test_3", "test_4"};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col3.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col4_str.data(), rowCnt));
    return vb;
}

/*
 * ProcessOperator --> StreamCalc --> SinkOperator
 * After being processed by the first two operators, SinkOperator writes the result into /tmp/{SinkOperator's name}.txt,
 * or /tmp/sink.txt by default Check the .txt file to check if operator chain works fine
 */
/*TEST(Q13OperatorChainTest, DISABLED_LookupJoinProjSink)
{
    // Setup operatorChain
    std::vector<std::string> operatorDescriptors{Q13ConfigStr_lookupJoin, Q13ConfigStr_calc, Q13ConfigStr_sink};
    std::vector<omnistream::OperatorConfig> opChainConfig;

    BatchOutputTest *output = new BatchOutputTest();
    // nlohmann::json description = nlohmann::json::parse(operatorDescriptors[0]);
    for (int i = 0; i < operatorDescriptors.size(); i++)
    {
        auto desc = operatorDescriptors[i];
        nlohmann::json parsedJson = nlohmann::json::parse(desc);
        omnistream::OperatorConfig opConfig(parsedJson["id"],
                                            parsedJson["name"],
                                            parsedJson["description"]["inputTypes"],
                                            parsedJson["description"]["outputTypes"],
                                            parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }

    OperatorChain *chain = new OperatorChain(opChainConfig);
    StreamOperator *headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer =
        new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("Q13OperatorChainTest", 1, 1, 0)));
    chain->initializeStateAndOpenOperators(initializer);

    auto processOp = dynamic_cast<ProcessOperator *>(headOp);

    omnistream::VectorBatch *inputBatch = BuildInputVectorBatchJoin();
    processOp->processBatch(new StreamRecord(inputBatch));

    inputBatch->FreeAllVectors();
    delete chain;
}

*//*
 * ProcessOperator --> StreamCalc
 * This test does not have a SinkOperator in the operator chain, so compare the output VB with an expected VB
 *//*
TEST(Q13OperatorChainTest, DISABLED_LookupJoinProj)
{
    // Setup operatorChain
    std::vector<std::string> operatorDescriptors{Q13ConfigStr_lookupJoin, Q13ConfigStr_calc};
    std::vector<omnistream::OperatorConfig> opChainConfig;

    BatchOutputTest *output = new BatchOutputTest();
    // nlohmann::json description = nlohmann::json::parse(operatorDescriptors[0]);
    for (int i = 0; i < operatorDescriptors.size(); i++)
    {
        auto desc = operatorDescriptors[i];
        nlohmann::json parsedJson = nlohmann::json::parse(desc);
        omnistream::OperatorConfig opConfig(parsedJson["id"],
                                            parsedJson["name"],
                                            parsedJson["description"]["inputTypes"],
                                            parsedJson["description"]["outputTypes"],
                                            parsedJson["description"]);
        opChainConfig.push_back(opConfig);
    }

    OperatorChain *chain = new OperatorChain(opChainConfig);
    StreamOperator *headOp = chain->createMainOperatorAndCollector(opChainConfig, output);
    StreamTaskStateInitializerImpl *initializer =
        new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("Q13OperatorChainTest", 1, 1, 0)));
    chain->initializeStateAndOpenOperators(initializer);

    auto processOp = dynamic_cast<ProcessOperator *>(headOp);

    omnistream::VectorBatch *inputBatch = BuildInputVectorBatchJoin();
    processOp->processBatch(new StreamRecord(inputBatch));

    omnistream::VectorBatch *outputBatch = reinterpret_cast<omnistream::VectorBatch *>(output->getVectorBatch());
    omnistream::VectorBatch *expectedBatch = BuildExpectedVectorBatch();

    bool matched = omniruntime::TestUtil::VecBatchMatch(outputBatch, expectedBatch);

    EXPECT_EQ(matched, true);

    inputBatch->FreeAllVectors();
    outputBatch->FreeAllVectors();
}*/
