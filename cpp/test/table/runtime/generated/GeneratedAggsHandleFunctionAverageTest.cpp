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
#include "table/runtime/generated/GeneratedAggsHandleFunctionAverage.h"

TEST(GeneratedAggsHandleFunctionAverageTest, getAccumulators)
{
    GeneratedAggsHandleFunctionAverage aggsHandleFunction(1, 0, 1, 0);
    BinaryRowData* accumulator = BinaryRowData::createBinaryRowDataWithMem(2);

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setLong(0, 0L);
    row->setLong(1, 4L);
    aggsHandleFunction.accumulate(row);
    row->setLong(0, 0L);
    row->setLong(1, 2L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getAccumulators(accumulator);
    EXPECT_EQ(6L, *accumulator->getLong(0)); // SUM
    EXPECT_EQ(2L, *accumulator->getLong(1)); // COUNT

    delete row;
}

TEST(GeneratedAggsHandleFunctionAverageTest, AccumulateUpdates)
{
    GeneratedAggsHandleFunctionAverage aggsHandleFunction(1, 0, 1, 0);
    BinaryRowData* result = BinaryRowData::createBinaryRowDataWithMem(1);

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setLong(0, 0L);
    row->setLong(1, 8L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(8L, *result->getLong(0)); // COUNT = 1, SUM = 8 | AVG = 8 (8/1 = 8)

    row->setLong(0, 0L);
    row->setLong(1, 3L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(5L, *result->getLong(0)); // COUNT = 2, SUM = 11 | AVG = 5 (11/2 = 5.5)

    row->setLong(0, 0L);
    row->setLong(1, -11L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(0L, *result->getLong(0)); // COUNT = 3, SUM = 0 | AVG = 0 (0/3 = 0)

    row->setLong(0, 0L);
    row->setLong(1, -15L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getValue(result);
    EXPECT_EQ(-3L, *result->getLong(0)); // COUNT = 4, SUM = -15 | AVG = -3 (-15/4 = -3.75)

    delete row;
}

TEST(GeneratedAggsHandleFunctionAverageTest, setAggregateIndex)
{
    GeneratedAggsHandleFunctionAverage aggsHandleFunction(3, 0, 1, 0);
    BinaryRowData* result = BinaryRowData::createBinaryRowDataWithMem(1);

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(4);
    row->setLong(0, 0L);
    row->setLong(1, 0L);
    row->setLong(2, 0L);
    row->setLong(3, 5L);
    aggsHandleFunction.accumulate(row);
    row->setLong(0, 0L);
    row->setLong(1, 0L);
    row->setLong(2, 0L);
    row->setLong(3, 3L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getValue(result);

    EXPECT_EQ(4L, *result->getLong(0));

    delete row;
}

TEST(GeneratedAggsHandleFunctionAverageTest, ResetAccumulators)
{
    GeneratedAggsHandleFunctionAverage aggsHandleFunction(1, 0, 1, 0);
    BinaryRowData* result = BinaryRowData::createBinaryRowDataWithMem(1);

    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setLong(0, 0L);
    row->setLong(1, 8L);
    aggsHandleFunction.accumulate(row);
    aggsHandleFunction.getValue(result);
    EXPECT_FALSE(result->isNullAt(0));
    EXPECT_EQ(8L, *result->getLong(0));

    aggsHandleFunction.resetAccumulators();
    aggsHandleFunction.getValue(result);

    EXPECT_TRUE(result->isNullAt(0));

    delete row;
}
