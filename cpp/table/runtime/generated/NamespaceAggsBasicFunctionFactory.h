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

#include <algorithm>
#include <cctype>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include "common.h"
#include "function/NamespaceAggsBasicFunction.h"

enum class NamespaceAggsBasicFunctionType {
    COUNT,
    SUM,
    MIN,
    MAX,
    AVG
};

class NamespaceAggsBasicFunctionFactory {
public:
    template <typename N>
    static std::unique_ptr<NamespaceAggsBasicFunction<N>> create(
        const std::string& accTypeStr,
        std::vector<int32_t> argIndexes,
        std::vector<int32_t> inputTypeIds,
        std::vector<int32_t> accIndexes,
        std::vector<int32_t> accTypeIds,
        int32_t aggValueIndex,
        int32_t aggValueTypeId);

    static std::vector<int32_t> getAccIndexes(const std::string& accTypeStr, int32_t accStartIndex)
    {
        auto accFunctionType = extractAggFunction(accTypeStr);
        if (accFunctionType == NamespaceAggsBasicFunctionType::AVG) {
            return {accStartIndex, accStartIndex + 1};
        }
        return {accStartIndex};
    }

private:
    static int32_t getAccSlot(NamespaceAggsBasicFunctionType accFunctionType)
    {
        return accFunctionType == NamespaceAggsBasicFunctionType::AVG ? 2 : 1;
    }

    static NamespaceAggsBasicFunctionType extractAggFunction(const std::string& accTypeStr)
    {
        std::regex aggRegex(R"((?:MAX|COUNT|SUM|MIN|AVG))", std::regex_constants::icase);
        std::smatch match;
        if (std::regex_search(accTypeStr, match, aggRegex)) {
            std::string aggType = match.str();
            std::transform(aggType.begin(), aggType.end(), aggType.begin(), [](unsigned char c) {
                return static_cast<char>(std::toupper(c));
            });
            if (aggType == "COUNT") {
                return NamespaceAggsBasicFunctionType::COUNT;
            }
            if (aggType == "SUM") {
                return NamespaceAggsBasicFunctionType::SUM;
            }
            if (aggType == "MIN") {
                return NamespaceAggsBasicFunctionType::MIN;
            }
            if (aggType == "MAX") {
                return NamespaceAggsBasicFunctionType::MAX;
            }
            if (aggType == "AVG") {
                return NamespaceAggsBasicFunctionType::AVG;
            }
        }
        THROW_LOGIC_EXCEPTION("Invalid agg function type: " << accTypeStr);
    }
};
