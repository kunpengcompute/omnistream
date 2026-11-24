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

#include <stdexcept>

#include "table/runtime/generated/NamespaceAggsBasicFunctionFactory.h"
#include "table/runtime/generated/function/NamespaceAggsAvgFunction.h"
#include "table/runtime/generated/function/NamespaceAggsCountFunction.h"
#include "table/runtime/generated/function/NamespaceAggsMinMaxFunction.h"
#include "table/runtime/generated/function/NamespaceAggsSumFunction.h"
#include "type/data_type.h"

namespace {
constexpr int32_t LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
}

TEST(NamespaceAggsBasicFunctionFactoryTest, CreatesSupportedFunctionsCaseInsensitively)
{
    auto count = NamespaceAggsBasicFunctionFactory::create<int64_t>("count()", {}, {}, {0}, {LONG_TYPE}, 0, LONG_TYPE);
    auto sum = NamespaceAggsBasicFunctionFactory::create<int64_t>(
        "LongSumAggFunction", {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE);
    auto min =
        NamespaceAggsBasicFunctionFactory::create<int64_t>("MIN($0)", {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE);
    auto max =
        NamespaceAggsBasicFunctionFactory::create<int64_t>("MAX($0)", {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE);
    auto avg = NamespaceAggsBasicFunctionFactory::create<int64_t>(
        "AVG($0)", {0}, {LONG_TYPE}, {0, 1}, {LONG_TYPE, LONG_TYPE}, 0, LONG_TYPE);

    EXPECT_NE(dynamic_cast<NamespaceAggsCountFunction<int64_t>*>(count.get()), nullptr);
    EXPECT_NE(dynamic_cast<NamespaceAggsSumFunction<int64_t>*>(sum.get()), nullptr);
    EXPECT_NE(dynamic_cast<NamespaceAggsMinMaxFunction<int64_t>*>(min.get()), nullptr);
    EXPECT_NE(dynamic_cast<NamespaceAggsMinMaxFunction<int64_t>*>(max.get()), nullptr);
    EXPECT_NE(dynamic_cast<NamespaceAggsAvgFunction<int64_t>*>(avg.get()), nullptr);
}

TEST(NamespaceAggsBasicFunctionFactoryTest, ReturnsAccumulatorIndexesForFunctionArity)
{
    EXPECT_EQ((std::vector<int32_t>{3}), NamespaceAggsBasicFunctionFactory::getAccIndexes("SUM", 3));
    EXPECT_EQ((std::vector<int32_t>{3, 4}), NamespaceAggsBasicFunctionFactory::getAccIndexes("AVG", 3));
}

TEST(NamespaceAggsBasicFunctionFactoryTest, RejectsUnsupportedFunctionAndWrongSlotCount)
{
    EXPECT_THROW(
        NamespaceAggsBasicFunctionFactory::create<int64_t>("MEDIAN", {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE),
        std::logic_error);
    EXPECT_THROW(
        NamespaceAggsBasicFunctionFactory::create<int64_t>("AVG", {0}, {LONG_TYPE}, {0}, {LONG_TYPE}, 0, LONG_TYPE),
        std::logic_error);
}
