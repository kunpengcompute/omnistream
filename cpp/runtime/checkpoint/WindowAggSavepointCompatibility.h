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

#include <string>
#include <nlohmann/json.hpp>
#include "FlinkSavepointAdaptorInfo.h"

namespace omnistream {

class WindowAggSavepointCompatibility {
public:
    static FlinkSavepointAdaptorInfo forLocal()
    {
        return {FlinkSavepointAdaptorType::OmniIsCompatible, ""};
    }

    static FlinkSavepointAdaptorInfo forSlicing(const nlohmann::json& description)
    {
        const bool isWindowAggregate = description.value("isWindowAggregate", false);
        const std::string fieldName = isWindowAggregate ? "AccTypes" : "globalAccTypes";
        return evaluateAccumulatorTypes(
            description, fieldName, FlinkSavepointAdaptorType::OmniIsCompatible, "SlicingWindowAgg");
    }

    static FlinkSavepointAdaptorInfo forGroup(const nlohmann::json& description)
    {
        return evaluateAccumulatorTypes(
            description, "AccTypes", FlinkSavepointAdaptorType::OmniIsCompatible, "GroupWindowAgg");
    }

private:
    static bool isRawAccumulator(const std::string& type)
    {
        return type == "RAW" || type.compare(0, 4, "RAW(") == 0;
    }

    static FlinkSavepointAdaptorInfo evaluateAccumulatorTypes(
        const nlohmann::json& description,
        const std::string& fieldName,
        FlinkSavepointAdaptorType supportedType,
        const std::string& operatorType)
    {
        if (!description.contains("aggInfoList") || !description["aggInfoList"].is_object() ||
            !description["aggInfoList"].contains(fieldName) || !description["aggInfoList"][fieldName].is_array()) {
            return {
                FlinkSavepointAdaptorType::None,
                operatorType + " compatible savepoint requires aggInfoList." + fieldName};
        }

        const auto& accumulatorTypes = description["aggInfoList"][fieldName];
        for (const auto& accumulatorType : accumulatorTypes) {
            if (!accumulatorType.is_string()) {
                return {
                    FlinkSavepointAdaptorType::None,
                    operatorType + " compatible savepoint requires string accumulator types"};
            }
            if (isRawAccumulator(accumulatorType.get_ref<const std::string&>())) {
                return {
                    FlinkSavepointAdaptorType::None,
                    operatorType + " compatible savepoint does not support RAW/DataView accumulators"};
            }
        }
        return {supportedType, ""};
    }
};

} // namespace omnistream
