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

#pragma once

#include "NamespaceAggsBasicFunctionFactory.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "table/runtime/generated/function/NamespaceAggsAvgFunction.h"
#include "table/runtime/generated/function/NamespaceAggsCountFunction.h"
#include "table/runtime/generated/function/NamespaceAggsMinMaxFunction.h"
#include "table/runtime/generated/function/NamespaceAggsSumFunction.h"
#include "table/runtime/operators/window/TimeWindow.h"

template <typename N>
std::unique_ptr<NamespaceAggsBasicFunction<N>> NamespaceAggsBasicFunctionFactory::create(
    const std::string& accTypeStr,
    std::vector<int32_t> argIndexes,
    std::vector<int32_t> inputTypeIds,
    std::vector<int32_t> accIndexes,
    std::vector<int32_t> accTypeIds,
    int32_t aggValueIndex,
    int32_t aggValueTypeId)
{
    auto functionType = extractAggFunction(accTypeStr);
    const int32_t accSlot = getAccSlot(functionType);
    if (accSlot != accIndexes.size()) {
        THROW_LOGIC_EXCEPTION("Aggregate accumulator indexes count must be " + std::to_string(accSlot) + ".");
    }

    std::unique_ptr<NamespaceAggsBasicFunction<N>> function;
    switch (functionType) {
        case NamespaceAggsBasicFunctionType::COUNT:
            function = std::make_unique<NamespaceAggsCountFunction<N>>(
                std::move(argIndexes),
                std::move(inputTypeIds),
                std::move(accIndexes),
                std::move(accTypeIds),
                aggValueIndex);
            break;
        case NamespaceAggsBasicFunctionType::SUM:
            function = std::make_unique<NamespaceAggsSumFunction<N>>(
                std::move(argIndexes),
                std::move(inputTypeIds),
                std::move(accIndexes),
                std::move(accTypeIds),
                aggValueIndex,
                aggValueTypeId);
            break;
        case NamespaceAggsBasicFunctionType::MIN:
        case NamespaceAggsBasicFunctionType::MAX:
            function = std::make_unique<NamespaceAggsMinMaxFunction<N>>(
                std::move(argIndexes),
                std::move(inputTypeIds),
                std::move(accIndexes),
                std::move(accTypeIds),
                aggValueIndex,
                aggValueTypeId,
                functionType);
            break;
        case NamespaceAggsBasicFunctionType::AVG:
            function = std::make_unique<NamespaceAggsAvgFunction<N>>(
                std::move(argIndexes),
                std::move(inputTypeIds),
                std::move(accIndexes),
                std::move(accTypeIds),
                aggValueIndex,
                aggValueTypeId);
            break;
        default: THROW_LOGIC_EXCEPTION("Unsupported aggregate function type: " << static_cast<int32_t>(functionType));
    }

    return function;
}

template std::unique_ptr<NamespaceAggsBasicFunction<int64_t>> NamespaceAggsBasicFunctionFactory::create<int64_t>(
    const std::string&,
    std::vector<int32_t>,
    std::vector<int32_t>,
    std::vector<int32_t>,
    std::vector<int32_t>,
    int32_t,
    int32_t);
template std::unique_ptr<NamespaceAggsBasicFunction<TimeWindow>> NamespaceAggsBasicFunctionFactory::create<TimeWindow>(
    const std::string&,
    std::vector<int32_t>,
    std::vector<int32_t>,
    std::vector<int32_t>,
    std::vector<int32_t>,
    int32_t,
    int32_t);
