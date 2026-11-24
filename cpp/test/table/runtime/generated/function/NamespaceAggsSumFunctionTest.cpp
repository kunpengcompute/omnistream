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
#include "table/runtime/generated/function/NamespaceAggsSumFunction.h"
#include "type/data_type.h"

namespace {
constexpr int32_t INT_TYPE = omniruntime::type::DataTypeId::OMNI_INT;
constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
} // namespace

TEST(NamespaceAggsSumFunctionTest, AccumulatesIntRetractsMergesAndWritesValue)
{
    NamespaceAggsSumFunction<int64_t> function({0}, {INT_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setInt(0, 7);
    function.accumulate(input.get());
    input->setInt(0, 2);
    function.retract(input.get());

    auto other = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    other->setLong(0, 4);
    function.merge(0, other.get());

    EXPECT_EQ(9, *function.getAccumulators()->getLong(0));
    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    function.updateAggValue(value.get());
    EXPECT_EQ(9, *value->getLong(0));
}

TEST(NamespaceAggsSumFunctionTest, KeepsAccumulatorAndValueNullForOnlyNullInputs)
{
    NamespaceAggsSumFunction<int64_t> function({0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setNullAt(0);
    function.accumulate(input.get());

    EXPECT_TRUE(function.getAccumulators()->isNullAt(0));
    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    function.updateAggValue(value.get());
    EXPECT_TRUE(value->isNullAt(0));
}
