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
#include <stdexcept>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/generated/NamespaceAggsBasicFunctionFactory.h"
#include "table/runtime/generated/function/NamespaceAggsMinMaxFunction.h"
#include "type/data_type.h"

namespace {
constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
}

TEST(NamespaceAggsMinMaxFunctionTest, TracksMinimumMergesAndWritesValue)
{
    NamespaceAggsMinMaxFunction<int64_t> function(
        {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE, NamespaceAggsBasicFunctionType::MIN);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setLong(0, 8);
    function.accumulate(input.get());
    input->setLong(0, 3);
    function.accumulate(input.get());
    auto other = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    other->setLong(0, 1);
    function.merge(0, other.get());

    EXPECT_EQ(1, *function.getAccumulators()->getLong(0));
    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    function.updateAggValue(value.get());
    EXPECT_EQ(1, *value->getLong(0));
}

TEST(NamespaceAggsMinMaxFunctionTest, TracksMaximumAndRejectsRetract)
{
    NamespaceAggsMinMaxFunction<int64_t> function(
        {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE, NamespaceAggsBasicFunctionType::MAX);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());
    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setLong(0, 4);
    function.accumulate(input.get());
    input->setLong(0, 9);
    function.accumulate(input.get());

    EXPECT_EQ(9, *function.getAccumulators()->getLong(0));
    EXPECT_THROW(function.retract(input.get()), std::runtime_error);
}
