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

#include <emhash7.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "table/sources/CsvTableSource.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "core/typeutils/LongSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
using json = nlohmann::json;
omnistream::VectorBatch* BuildInputVectorBatch2()
{
    int rowCnt = 5;
    std::vector<long> col0(rowCnt);
    std::vector<long> col1(rowCnt);
    std::vector<long> col2(rowCnt);

    for (int i = 0; i < rowCnt; i++) {
        col0[i] = i;
        col1[i] = 0;
        col2[i] = 2 * i;
    }

    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout << "input vectorbatch created" << std::endl;
    return vb;
}
/*
TEST(TableSourceTest, CsvTableSourceBuildHash)
{
    LongSerializer* serializer = new LongSerializer();
    InternalTypeInfo longTypeInfo(BasicLogicalType::BIGINT, serializer);
    std::vector<TypeInformation*> typeInfos (3, &longTypeInfo);
    std::vector<std::string> fieldNames{"col0", "col1", "col2"};
    CsvTableSource src("input/csv_test.csv", fieldNames, typeInfos);

    // hash side uses col2 as key, probe size uses its col0 as key
    nlohmann::json obj = nlohmann::json::parse(R"({"0":{"index":2}})");
    CsvLookupFunction<long> lookupFunc(src.getConfig(), obj);
    lookupFunc.open();
    auto outVec = lookupFunc.getTestFunc(3L);
    std::vector<int32_t> expectedOutput{0, 3, 4};
    EXPECT_EQ(outVec, expectedOutput);
}

TEST(TableSourceTest, CsvTableSourceInnerJoinLong)
{
    LongSerializer* serializer = new LongSerializer();
    InternalTypeInfo longTypeInfo(BasicLogicalType::BIGINT, serializer);
    std::vector<TypeInformation*> typeInfos (3, &longTypeInfo);
    std::vector<std::string> fieldNames{"col0", "col1", "col2"};
    CsvTableSource src("input/csv_test.csv", fieldNames, typeInfos);

    // hash side uses col2 as key, probe size uses its col0 as key
    nlohmann::json obj = nlohmann::json::parse(R"({"0":{"index":2}})");
    CsvLookupFunction<long> lookupFunc(src.getConfig(), obj);
    lookupFunc.open();

    TestCollector* collector = new TestCollector();
    auto probeBatch = BuildInputVectorBatch2();
    lookupFunc.eval(probeBatch, collector);
    auto outputvb = collector->collectedData;
    EXPECT_EQ(outputvb->GetRowCount(), 3);
    EXPECT_EQ(outputvb->GetVectorCount(), 6);

    std::vector<long> expected{1,2,3,3,0,6};
    for (int i = 0; i < outputvb->GetRowCount(); i++) {
        for(int j = 0; j < 6;j++) {
            long val = static_cast<omniruntime::vec::Vector<int64_t>*>(outputvb->GetVectors()[j])->GetValue(i);
            EXPECT_EQ(val, expected[j]);
        }
    }
}
 */
