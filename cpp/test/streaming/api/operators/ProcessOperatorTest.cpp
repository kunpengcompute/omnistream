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
#include <vector>
#include <nlohmann/json.hpp>
#include "streaming/api/operators/ProcessOperator.h"
#include "table/runtime/operators/join/lookup/LookupJoinRunner.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "test/core/operators/OutputTest.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"

using json = nlohmann::json;
std::string plan = R"delim(
{"originDescription":null,
"inputTypes":["BIGINT","BIGINT","BIGINT","TIMESTAMP(3) *ROWTIME*","BIGINT"],
"lookupInputTypes":["BIGINT","VARCHAR(2147483647)"],
"outputTypes":["BIGINT","BIGINT","BIGINT","TIMESTAMP(3) *ROWTIME*","BIGINT","BIGINT","STRING"],
"joinType":"InnerJoin",
"selectedFields" : [0, 1],
"condition":null,
"projectionOnTemporalTable":[],
"filterOnTemporalTable":null,
"lookupKeys":{"0":{"index":4}},
"temporalTableSourceSpec":"table [default_catalog.default_database.side_input]",
"temporalTableSourceTypeName":"CsvTableSource",
"connectorType":"filesystem",
"connectorPath":"input/lookupjoin_test.csv",
"formatType":"csv"}
)delim";

omnistream::VectorBatch* BuildInputVectorBatch3()
{
    int rowCnt = 5;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);
    std::vector<long> col3(rowCnt);
    std::vector<long> col4(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = i + 1;
        col1[i] = i + 2;
        col2[i] = i + 3;
        col3[i] = i + 4;
        col4[i] = i + 421;
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

TEST(ProcessOperatorTest, Constructor)
{
    auto description = nlohmann::json::parse(plan);
    BatchOutputTest* output = new BatchOutputTest();
    LookupJoinRunner* runner = new LookupJoinRunner(description, output);
    std::cout << "LookupJoinRunner constructed" << std::endl;
    ProcessOperator op(runner, description, output);
    std::cout << "ProcessOperator constructed" << std::endl;
}

TEST(ProcessOperatorTest, Open)
{
    auto description = nlohmann::json::parse(plan);
    BatchOutputTest* output = new BatchOutputTest();
    LookupJoinRunner* runner = new LookupJoinRunner(description, output);
    ProcessOperator op(runner, description, output);
    op.open();
}

/*
 * 421,4,hello_world_0,4,0
 * 422,4,hello_world_3,4,0
 * 423,5,hello_world_5,5,0
 * 424,5,hello_world_7,5,0
 */
omnistream::VectorBatch* BuildExpectedVectorBatch3()
{
    int rowCnt = 4;
    std::vector<long> col0{421, 422, 423, 424};
    std::vector<long> col1{4, 4, 5, 5};
    std::vector<std::string> col2{"hello_world_0", "hello_world_3", "hello_world_5", "hello_world_7"};
    std::vector<long> col3{0, 0, 0, 0};

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVarcharVector(col2.data(), rowCnt));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col3.data()));
    return vb;
}
TEST(ProcessOperatorTest, DISABLED_LookupJoinRunner)
{
    auto description = nlohmann::json::parse(plan);
    OutputTestVectorBatch* output = new OutputTestVectorBatch();
    LookupJoinRunner* runner = new LookupJoinRunner(description, output);
    ProcessOperator op(runner, description, output);
    op.open();
    auto inputBatch = BuildInputVectorBatch3();
    auto record = new StreamRecord(inputBatch);
    op.processBatch(record);
    // auto outputvb = output->getVectorBatch();

    // auto expectedvb = BuildExpectedVectorBatch3();
    //  TODO: We should have a VecBatchMatchIgnoreRowOrder for this one!
    // bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    // EXPECT_EQ(matched, true);
}
