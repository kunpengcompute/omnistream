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
#include "table/runtime/generated/function/GroupWindowAggsHandleFunction.h"
#include "table/runtime/generated/function/NamespaceAggsCountFunction.h"
#include "table/runtime/generated/function/NamespaceAggsSumFunction.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "type/data_type.h"

namespace {

constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
constexpr int32_t TIMESTAMP_TYPE = omniruntime::type::DataTypeId::OMNI_TIMESTAMP;

std::vector<std::unique_ptr<NamespaceAggsBasicFunction<TimeWindow>>> MakeFunctions()
{
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<TimeWindow>>> functions;
    functions.push_back(
        std::make_unique<NamespaceAggsCountFunction<TimeWindow>>(
            std::vector<int32_t>{},
            std::vector<int32_t>{},
            std::vector<int32_t>{0},
            std::vector<int32_t>{LONG_TYPE},
            0));
    functions.push_back(
        std::make_unique<NamespaceAggsSumFunction<TimeWindow>>(
            std::vector<int32_t>{0},
            std::vector<int32_t>{LONG_TYPE},
            std::vector<int32_t>{1},
            std::vector<int32_t>{LONG_TYPE},
            1,
            LONG_TYPE));
    return functions;
}

} // namespace

TEST(GroupWindowAggsHandleFunctionTest, CoordinatesFunctionsAndAppendsWindowProperties)
{
    GroupWindowAggsHandleFunction<TimeWindow> function(
        MakeFunctions(),
        {LONG_TYPE, LONG_TYPE},
        {TIMESTAMP_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE},
        {LONG_TYPE, LONG_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE},
        2);
    auto accumulator = std::unique_ptr<RowData>(function.createAccumulators());
    function.setAccumulators(TimeWindow(1000, 2000), accumulator.get());

    auto input = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    input->setLong(0, 7);
    function.accumulate(input.get());

    EXPECT_EQ(accumulator.get(), function.getAccumulators());
    auto value = std::unique_ptr<RowData>(function.getValue(TimeWindow(1000, 2000)));
    EXPECT_EQ(1, *value->getLong(0));
    EXPECT_EQ(7, *value->getLong(1));
    EXPECT_EQ(1000, *value->getLong(2));
    EXPECT_EQ(2000, *value->getLong(3));
    EXPECT_EQ(1999, *value->getLong(4));
    EXPECT_EQ(-1, *value->getLong(5));
}

TEST(GroupWindowAggsHandleFunctionTest, RejectsInconsistentOutputLayout)
{
    EXPECT_THROW(
        GroupWindowAggsHandleFunction<TimeWindow>(
            MakeFunctions(),
            {LONG_TYPE, LONG_TYPE},
            {TIMESTAMP_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE, TIMESTAMP_TYPE},
            {LONG_TYPE, LONG_TYPE},
            2),
        std::logic_error);
}
