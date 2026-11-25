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

#ifndef FLINK_TNEL_LOGICTYPEUTILS_H
#define FLINK_TNEL_LOGICTYPEUTILS_H

#include <string>
#include <limits>

#include <nlohmann/json.hpp>

#include "core/include/common.h"

class LogicTypeUtils {
public:
    static bool startsWith(const std::string& value, const std::string& prefix)
    {
        return value.size() >= prefix.size() && value.compare(0, prefix.size(), prefix) == 0;
    }

    static std::string stripFlinkTypeExtras(const std::string& flinkType)
    {
        std::string basicStrippedType = flinkType;
        eraseSuffix(basicStrippedType, " *PROCTIME*");
        eraseSuffix(basicStrippedType, " NOT NULL");
        return basicStrippedType;
    }

    static bool isNotNullType(const std::string& flinkType)
    {
        std::string typeWithoutTimeAttribute = flinkType;
        eraseSuffix(typeWithoutTimeAttribute, " *PROCTIME*");
        static const std::string suffix = " NOT NULL";
        return typeWithoutTimeAttribute.size() >= suffix.size() &&
               typeWithoutTimeAttribute.compare(
                   typeWithoutTimeAttribute.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    static nlohmann::json optionsFromFlinkType(const std::string& basicStrippedType)
    {
        nlohmann::json options = nlohmann::json::object();
        if (startsWith(basicStrippedType, "TIMESTAMP")) {
            options["precision"] = parseFirstIntOption(basicStrippedType, 3);
        } else if (startsWith(basicStrippedType, "TIME")) {
            options["precision"] = parseFirstIntOption(basicStrippedType, 0);
        } else if (
            startsWith(basicStrippedType, "VARCHAR") || startsWith(basicStrippedType, "CHAR") ||
            startsWith(basicStrippedType, "BINARY") || startsWith(basicStrippedType, "VARBINARY") ||
            startsWith(basicStrippedType, "BYTES")) {
            options["length"] = parseFirstIntOption(basicStrippedType, std::numeric_limits<int>::max());
        }
        return options;
    }

private:
    static void eraseSuffix(std::string& value, const std::string& suffix)
    {
        if (value.size() >= suffix.size() && value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0) {
            value.erase(value.size() - suffix.size());
        }
    }

    static int parseFirstIntOption(const std::string& typeName, int defaultValue)
    {
        size_t openParen = typeName.find('(');
        size_t closeParen = typeName.find(')', openParen == std::string::npos ? 0 : openParen);
        if (openParen == std::string::npos || closeParen == std::string::npos || closeParen <= openParen + 1) {
            return defaultValue;
        }
        try {
            int value = std::stoi(typeName.substr(openParen + 1, closeParen - openParen - 1));
            if (value < 0) {
                INFO_RELEASE(
                    "Warning: Unexpected negative precision " << value << " in type string " << typeName
                                                              << ", falling back to default " << defaultValue);
                return defaultValue;
            }
            return value;
        } catch (...) {
            return defaultValue;
        }
    }
};

#endif
