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

#include "streaming/api/operators/StreamExpand.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "OutputTest.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"

using json = nlohmann::json;

std::string x = R"DELIM({"projects": [{)DELIM"
                R"DELIM(            "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],)DELIM"
                R"DELIM(            "indices": [)DELIM"
                R"DELIM(                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 0},)DELIM"
                R"DELIM(               {"exprType": "LITERAL", "isNull": true, "dataType": 2},)DELIM"
                R"DELIM(               {"exprType": "LITERAL", "isNull": false, "dataType": 2, "value": 1})DELIM"
                R"DELIM(          ],)DELIM"
                R"DELIM(            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"])DELIM"
                R"DELIM(        },{)DELIM"
                R"DELIM(            "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],)DELIM"
                R"DELIM(            "indices": [)DELIM"
                R"DELIM(                {"exprType": "LITERAL", "isNull": true, "dataType": 2},)DELIM"
                R"DELIM(                {"exprType": "FIELD_REFERENCE", "dataType": 2, "colVal": 1},)DELIM"
                R"DELIM(                {"exprType": "LITERAL", "isNull": false, "dataType": 2, "value": 2})DELIM"
                R"DELIM(            ],)DELIM"
                R"DELIM(            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"])DELIM"
                R"DELIM(        },)DELIM"
                R"DELIM(        {)DELIM"
                R"DELIM(            "inputTypes": ["BIGINT", "BIGINT", "BIGINT"],)DELIM"
                R"DELIM(            "indices": [{"exprType": "LITERAL", "isNull": true, "dataType": 2},)DELIM"
                R"DELIM(                {"exprType": "LITERAL", "isNull": true, "dataType": 2},)DELIM"
                R"DELIM(                {"exprType": "LITERAL", "isNull": false, "dataType": 2, "value": 3})DELIM"
                R"DELIM(            ],)DELIM"
                R"DELIM(            "outputTypes": ["BIGINT", "BIGINT", "BIGINT"]})DELIM"
                R"DELIM(    ],"originDescription": "null"})DELIM";

omnistream::VectorBatch* BuildExpandInputVectorBatch()
{
    int rowCnt = 10;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = 0;
        col1[i] = i;
        col2[i] = 2 * i;
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout << "input vectorbatch created" << std::endl;
    return vb;
}

std::vector<omnistream::VectorBatch*> ProcessAndGetOutputTest(std::string desc, omnistream::VectorBatch* inputRecord)
{
    json parsedJson = json::parse(desc);
    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    StreamExpand streamExpandOp(parsedJson, output);
    streamExpandOp.open();
    StreamRecord* record = new StreamRecord(inputRecord);
    streamExpandOp.processBatch(record);
    auto out = output->getAll();
    return out;
}

TEST(StreamExpandTest, VectorbatchSimpleProjection)
{
    auto inputRecord = BuildExpandInputVectorBatch();
    auto outputRecord = ProcessAndGetOutputTest(x, inputRecord);
    // Check size
    int rowCnt = 10;
    int batchSize = 3;
    EXPECT_EQ(batchSize, outputRecord.size());
    // Check value
    auto outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[0]->Get(0));
    auto outcol1 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[0]->Get(1));
    auto outcol2 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[0]->Get(2));

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(outcol0->GetValue(i), 0);
        EXPECT_EQ(outcol1->IsNull(i), true);
        EXPECT_EQ(outcol2->GetValue(i), 1);
    }

    outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[1]->Get(0));
    outcol1 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[1]->Get(1));
    outcol2 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[1]->Get(2));

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(outcol1->GetValue(i), i);
        EXPECT_EQ(outcol0->IsNull(i), true);
        EXPECT_EQ(outcol2->GetValue(i), 2);
    }

    outcol0 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[2]->Get(0));
    outcol1 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[2]->Get(1));
    outcol2 = static_cast<omniruntime::vec::Vector<int64_t>*>(outputRecord[2]->Get(2));

    for (int i = 0; i < rowCnt; i++) {
        EXPECT_EQ(outcol0->IsNull(i), true);
        EXPECT_EQ(outcol1->IsNull(i), true);
        EXPECT_EQ(outcol2->GetValue(i), 3);
    }
}
