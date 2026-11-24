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

#ifndef OMNIFLINK_TYPE_UTIL_H
#define OMNIFLINK_TYPE_UTIL_H

#include <cstdint>
#include <string>
#include "../types/logical/CharType.h"
#include "../types/logical/VarCharType.h"
#include "../types/logical/RowType.h"
#include "../types/logical/LogicalType.h"
#include "../types/logical/LogicalTypeRoot.h"

class TypeUtil {
public:
    // Helper function for debugging DataType
    static std::string TypeToString(int32_t id)
    {
        switch (id) {
            case LogicalTypeRoot::BOOLEAN_TYPE_ID: return "Bool";
            case LogicalTypeRoot::DOUBLE_TYPE_ID: return "Double";
            case LogicalTypeRoot::DATE_TYPE_ID: return "Date32";
            case LogicalTypeRoot::INTEGER_TYPE_ID: return "Int32";
            case LogicalTypeRoot::SMALLINT_TYPE_ID: return "Int16";
            case LogicalTypeRoot::BIGINT_TYPE_ID: return "Int64";
            case LogicalTypeRoot::VARCHAR_TYPE_ID: return "String";
            case LogicalTypeRoot::CHAR_TYPE_ID: return "Char";
            case LogicalTypeRoot::DECIMAL_TYPE_ID: return "Decimal";
            case LogicalTypeRoot::INVALID_TYPE_ID: return "Invalid";
            default: return "UNKNOWN";
        }
    }

    static bool IsStringType(int32_t id)
    {
        return id == LogicalTypeRoot::CHAR_TYPE_ID || id == LogicalTypeRoot::VARCHAR_TYPE_ID;
    }

    static bool IsDecimalType(int32_t id)
    {
        return id == LogicalTypeRoot::DECIMAL_TYPE_ID;
    }
};

#endif
