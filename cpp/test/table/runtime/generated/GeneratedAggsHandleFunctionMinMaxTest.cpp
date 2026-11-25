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
#include "table/runtime/generated/GeneratedAggsHandleFunctionMinMax.h"

TEST(GeneratedAggsHandleFunctionMinMaxTest, AccumulateUpdates)
{
    GeneratedAggsHandleFunctionMinMax aggsHandleFunction(1, 0, 0, AggMaxOrMin::MAX);

    std::vector<int> typeIDs({0, 0});
    BinaryRowData* result = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(2);
    row1->setLong(0, 4L);
    row1->setLong(1, 6L);
    aggsHandleFunction.accumulate(row1);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(6L, *result->getLong(0));

    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(2);
    row2->setLong(0, 2L);
    row2->setLong(1, 1L);
    aggsHandleFunction.accumulate(row2);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(6L, *result->getLong(0));

    BinaryRowData* row3 = BinaryRowData::createBinaryRowDataWithMem(2);
    row3->setLong(0, 1L);
    row3->setLong(1, 9L);
    aggsHandleFunction.accumulate(row3);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(9L, *result->getLong(0));

    BinaryRowData* row4 = BinaryRowData::createBinaryRowDataWithMem(2);
    row4->setLong(0, 1L);
    row4->setLong(1, 1L);
    aggsHandleFunction.accumulate(row4);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(9L, *result->getLong(0));

    delete row1;
    delete row2;
    delete row3;
    delete row4;
    delete result;
}

TEST(GeneratedAggsHandleFunctionMinMaxTest, AccumulateHandlesNull)
{
    GeneratedAggsHandleFunctionMinMax aggsHandleFunction(1, 0, 0, AggMaxOrMin::MAX);
    BinaryRowData* accumulator = BinaryRowData::createBinaryRowDataWithMem(1);

    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(2);

    row1->setLong(0, nullptr);
    row1->setLong(1, nullptr);
    aggsHandleFunction.accumulate(row1);
    aggsHandleFunction.getAccumulators(accumulator);
    EXPECT_TRUE(accumulator->isNullAt(0)); // Max should be null

    row1->setLong(0, 3L);
    row1->setLong(1, 7L);
    aggsHandleFunction.accumulate(row1);
    aggsHandleFunction.getAccumulators(accumulator);

    EXPECT_EQ(7L, *accumulator->getLong(0)); // Max should be 7

    row1->setLong(1, nullptr);
    aggsHandleFunction.accumulate(row1);
    aggsHandleFunction.getAccumulators(accumulator);

    EXPECT_EQ(7L, *accumulator->getLong(0)); // Max should be 7

    delete row1;
}

TEST(GeneratedAggsHandleFunctionMinMaxTest, ResetAccumulators)
{
    GeneratedAggsHandleFunctionMinMax aggsHandleFunction(0, 0, 0, AggMaxOrMin::MAX);

    BinaryRowData* accumulator = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(1);
    row1->setLong(0, 3L);
    aggsHandleFunction.accumulate(row1);

    aggsHandleFunction.resetAccumulators(); // Reset Accumulator
    aggsHandleFunction.getAccumulators(accumulator);

    EXPECT_TRUE(accumulator->isNullAt(0)); // After reset, max should be null

    delete row1;
}

TEST(GeneratedAggsHandleFunctionMinMaxTest, AccumulateNegativeNumbers)
{
    GeneratedAggsHandleFunctionMinMax aggsHandleFunction(1, 0, 0, AggMaxOrMin::MAX);

    std::vector<int> typeIDs({0, 0});
    BinaryRowData* result = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(2);
    row1->setLong(0, 4L);
    row1->setLong(1, -6L);
    aggsHandleFunction.accumulate(row1);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(-6L, *result->getLong(0));

    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(2);
    row2->setLong(0, 2L);
    row2->setLong(1, -1L);
    aggsHandleFunction.accumulate(row2);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(-1L, *result->getLong(0));

    BinaryRowData* row3 = BinaryRowData::createBinaryRowDataWithMem(2);
    row3->setLong(0, 1L);
    row3->setLong(1, 9L);
    aggsHandleFunction.accumulate(row3);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(9L, *result->getLong(0));

    BinaryRowData* row4 = BinaryRowData::createBinaryRowDataWithMem(2);
    row4->setLong(0, 1L);
    row4->setLong(1, -10L);
    aggsHandleFunction.accumulate(row4);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(9L, *result->getLong(0));

    delete row1;
    delete row2;
    delete row3;
    delete row4;
    delete result;
}
