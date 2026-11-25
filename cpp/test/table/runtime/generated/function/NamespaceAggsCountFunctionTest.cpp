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
#include "table/runtime/generated/function/NamespaceAggsCountFunction.h"
#include "type/data_type.h"

namespace {
constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
}

TEST(NamespaceAggsCountFunctionTest, SupportsCountStarRetractAndValueOutput)
{
    NamespaceAggsCountFunction<int64_t> function({}, {}, {0}, {LONG_TYPE}, 0);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());

    function.accumulate(nullptr);
    function.accumulate(nullptr);
    function.retract(nullptr);

    EXPECT_EQ(1, *function.getAccumulators()->getLong(0));
    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    function.updateAggValue(value.get());
    EXPECT_EQ(1, *value->getLong(0));
}

TEST(NamespaceAggsCountFunctionTest, IgnoresNullArgumentAndMerges)
{
    NamespaceAggsCountFunction<int64_t> function({0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setNullAt(0);
    function.accumulate(input.get());
    input->setLong(0, 10);
    function.accumulate(input.get());

    auto other = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    other->setLong(0, 2);
    function.merge(0, other.get());

    EXPECT_EQ(3, *function.getAccumulators()->getLong(0));
}

TEST(NamespaceAggsCountFunctionTest, InsertedCountStarDoesNotWriteResultValue)
{
    NamespaceAggsCountFunction<int64_t> function({}, {}, {0}, {LONG_TYPE}, -1);
    auto accumulator = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    accumulator->setNullAt(0);
    function.setAccumulators(0, accumulator.get());
    function.accumulate(nullptr);

    auto value = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    value->setLong(0, 99);
    function.updateAggValue(value.get());
    EXPECT_EQ(99, *value->getLong(0));
}
