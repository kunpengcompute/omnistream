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
#include <vector>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/generated/function/NamespaceAggsCountFunction.h"
#include "table/runtime/generated/function/NamespaceAggsSumFunction.h"
#include "table/runtime/generated/function/WindowAggsHandleFunction.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "type/data_type.h"

namespace {

constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
constexpr int32_t TIMESTAMP_TYPE = omniruntime::type::DataTypeId::OMNI_TIMESTAMP;

std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> MakeFunctions()
{
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> functions;
    functions.push_back(
        std::make_unique<NamespaceAggsCountFunction<int64_t>>(
            std::vector<int32_t>{},
            std::vector<int32_t>{},
            std::vector<int32_t>{0},
            std::vector<int32_t>{LONG_TYPE},
            0));
    functions.push_back(
        std::make_unique<NamespaceAggsSumFunction<int64_t>>(
            std::vector<int32_t>{0},
            std::vector<int32_t>{LONG_TYPE},
            std::vector<int32_t>{1},
            std::vector<int32_t>{LONG_TYPE},
            1,
            LONG_TYPE));
    return functions;
}

} // namespace

TEST(WindowAggsHandleFunctionTest, CoordinatesFunctionsAndAppendsWindowBounds)
{
    TumblingSliceAssigner assigner(-1, nullptr, 1000, 0);
    WindowAggsHandleFunction function(
        MakeFunctions(), {LONG_TYPE, LONG_TYPE}, {LONG_TYPE, LONG_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE}, &assigner, 2);
    auto accumulator = std::unique_ptr<RowData>(function.createAccumulators());
    function.setAccumulators(5000, accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setLong(0, 7);
    function.accumulate(input.get());

    EXPECT_EQ(accumulator.get(), function.getAccumulators());
    EXPECT_EQ(1, *accumulator->getLong(0));
    EXPECT_EQ(7, *accumulator->getLong(1));

    auto value = std::unique_ptr<RowData>(function.getValue(5000));
    EXPECT_EQ(1, *value->getLong(0));
    EXPECT_EQ(7, *value->getLong(1));
    EXPECT_EQ(4000, *value->getLong(2));
    EXPECT_EQ(5000, *value->getLong(3));
}
// multi aggreagtions are supported. TODO: need to test multi aggreagtions later.
TEST(WindowAggsHandleFunctionTest, DISABLED_RejectsOutputLayoutWithoutTwoWindowFields)
{
    TumblingSliceAssigner assigner(-1, nullptr, 1000, 0);
    EXPECT_THROW(
        WindowAggsHandleFunction(MakeFunctions(), {LONG_TYPE, LONG_TYPE}, {LONG_TYPE, LONG_TYPE}, &assigner, 2),
        std::logic_error);
}
