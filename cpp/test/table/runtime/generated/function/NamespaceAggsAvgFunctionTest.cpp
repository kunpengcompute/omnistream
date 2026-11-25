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

#include <memory>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/generated/function/NamespaceAggsAvgFunction.h"
#include "type/data_type.h"

namespace {
constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
}

TEST(NamespaceAggsAvgFunctionTest, AccumulatesRetractsAndWritesValue)
{
    NamespaceAggsAvgFunction<int64_t> function({0}, {LONG_TYPE}, {0, 1}, {LONG_TYPE, LONG_TYPE}, 0, LONG_TYPE);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(2));
    accumulator->setNullAt(0);
    accumulator->setNullAt(1);
    function.setAccumulators(0, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setLong(0, 8);
    function.accumulate(input.get());
    input->setLong(0, 4);
    function.accumulate(input.get());
    function.retract(input.get());

    RowData* result = function.getAccumulators();
    EXPECT_EQ(8, *result->getLong(0));
    EXPECT_EQ(1, *result->getLong(1));

    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    function.updateAggValue(value.get());
    EXPECT_EQ(8, *value->getLong(0));
}

TEST(NamespaceAggsAvgFunctionTest, BecomesNullAfterRetractingLastValue)
{
    NamespaceAggsAvgFunction<int64_t> function({0}, {LONG_TYPE}, {0, 1}, {LONG_TYPE, LONG_TYPE}, 0, LONG_TYPE);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(2));
    accumulator->setNullAt(0);
    accumulator->setNullAt(1);
    function.setAccumulators(0, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setLong(0, 5);
    function.accumulate(input.get());
    function.retract(input.get());

    EXPECT_TRUE(function.getAccumulators()->isNullAt(0));
    EXPECT_TRUE(function.getAccumulators()->isNullAt(1));
    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    function.updateAggValue(value.get());
    EXPECT_TRUE(value->isNullAt(0));
}

TEST(NamespaceAggsAvgFunctionTest, MergesBothAccumulatorSlots)
{
    NamespaceAggsAvgFunction<int64_t> function({0}, {LONG_TYPE}, {0, 1}, {LONG_TYPE, LONG_TYPE}, 0, LONG_TYPE);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(2));
    accumulator->setLong(0, 4);
    accumulator->setLong(1, 1);
    function.setAccumulators(0, accumulator.get());

    auto other = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(2));
    other->setLong(0, 8);
    other->setLong(1, 2);
    function.merge(0, other.get());

    EXPECT_EQ(12, *function.getAccumulators()->getLong(0));
    EXPECT_EQ(3, *function.getAccumulators()->getLong(1));
}
