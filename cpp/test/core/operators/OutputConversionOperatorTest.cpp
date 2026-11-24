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
#include <iostream>
#include <nlohmann/json.hpp>
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"
#include "streaming/api/operators/sink/OutputConversionOperator.h"

using json = nlohmann::json;

std::string outputconversionDescription =
    R"DELIM({"originDescription":"TableToDataSteam(type=ROW<`key` STRING, `countRightId` BIGINT NOT NULL, `sumRightId` BIGINT, `value` STRING> NOT NULL, rowtime=false)",
"inputTypes":["VARCHAR(2147483647)","BIGINT","BIGINT","VARCHAR(2147483647)"],
"outputTypes":["VARCHAR(2147483647)","BIGINT","BIGINT","VARCHAR(2147483647)"]})DELIM";

omnistream::VectorBatch* newVectorBatchOutputConversion()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<std::string> key = {"2_0_7y0tKfD", "11_0_9PxSTa", "7_0_UiVsMjV", "13_1_R6Qnyd", "5_0_YgXQRyu"};
    std::vector<int64_t> countRightId = {3, 4, 5, 6, 7};
    std::vector<int64_t> sumRightId = {1009, 1007, 1003, 2002, 2004};
    std::vector<std::string> value = {"1009", "1007", "1003", "2002", "2004"};

    vbatch->Append(omniruntime::TestUtil::CreateVarcharVector(key.data(), 5));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, countRightId.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, sumRightId.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVarcharVector(value.data(), 5));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

TEST(OutputConversionOperatorTest, DISABLED_ProcessTest)
{
    // Operator description
    json parsedJson = json::parse(outputconversionDescription);
    omnistream::OperatorPOD outputconversionPOD(
        "nexmark_source", std::string(OPERATOR_NAME_OUTPUT_CONVERSION), outputconversionDescription, {}, {});
    auto* output = new ExternalOutputTest();
    auto* outputConversionOperator = dynamic_cast<OutputConversionOperator*>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(outputconversionPOD, output, nullptr));
    omnistream::VectorBatch* vBatch = newVectorBatchOutputConversion();
    auto* streamRecord = new StreamRecord(vBatch);
    outputConversionOperator->open();
    outputConversionOperator->processElement(streamRecord);
    //    auto* rowOutput = dynamic_cast<ExternalOutputTest*>(outputConversionOperator->getOutput());
    std::cout << "=========== print result ==========" << std::endl;
    auto resultVec = output->getAll();
    // print Row
    for (int j = 0; j < resultVec.size(); j++) {
        auto resultRow = resultVec[j];
        std::cout << to_string(resultRow->getKind()) << std::endl;
        int arity = resultRow->getArity();
        std::vector<std::string> outputTypes = parsedJson["outputTypes"].get<std::vector<std::basic_string<char>>>();
        for (int i = 0; i < arity; i++) {
            if (outputTypes[i] == "BIGINT" || outputTypes[i].find("TIMESTAMP") != std::string::npos) {
                std::cout << std::any_cast<long>(resultRow->getField(i)) << "| ";
            } else if (outputTypes[i] == "INTEGER") {
                std::cout << std::any_cast<int>(resultRow->getField(i)) << "| ";
            } else if (outputTypes[i] == "DOUBLE") {
                std::cout << std::any_cast<long>(resultRow->getField(i)) << "| ";
            } else if (outputTypes[i] == "BOOLEAN") {
                std::cout << std::any_cast<int>(resultRow->getField(i)) << "| ";
            } else if (
                outputTypes[i] == "STRING" || outputTypes[i] == "VARCHAR" || outputTypes[i] == "VARCHAR(2147483647)") {
                auto str_result = std::any_cast<std::string>(resultRow->getField(i));
                std::cout << str_result << "| ";
            } else {
                std::cout << "Unknown type." << std::endl;
            }
        }
        std::cout << std::endl;
    }
}
