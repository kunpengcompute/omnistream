/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <algorithm>
#include <mutex>
#include <stdexcept>

#include "CharType.h"
#include "LogicTypeUtils.h"
#include "LogicalType.h"
#include "RawType.h"
#include "VarCharType.h"
#include "TimestampWithoutTimeZoneType.h"
#include "TimeWithoutTimeZoneType.h"
#include "TimestampWithTimeZoneType.h"
#include "TimestampWithLocalTimeZoneType.h"

using namespace omniruntime::type;

LogicalType::LogicalType(int typeId, bool isNullable) : isNullable_(isNullable), typeId_(typeId)
{
}

LogicalType::LogicalType(bool isNullable, int typeId, const std::string& typeName)
    : isNullable_(isNullable),
      typeId_(typeId),
      typeName_(typeName)
{
}

int LogicalType::getTypeId() const
{
    return typeId_;
}

bool LogicalType::isNullable() const
{
    return isNullable_;
}

nlohmann::json LogicalType::toJson() const
{
    nlohmann::json result;
    result["nullable"] = isNullable_;
    result["type"] = typeName_;

    return result;
}

std::unordered_map<std::string, omniruntime::type::DataTypeId> LogicalType::nameToIdMap;

DataTypeId LogicalType::flinkTypeToOmniTypeId(const std::string& flinkType)
{
    buildNameToIdMap();
    // Deal with tailing "NOT NULL" and " *PROCTIME*"
    std::string basicStrippedType = LogicTypeUtils::stripFlinkTypeExtras(flinkType);

    // Typename has fixed format
    auto it = nameToIdMap.find(basicStrippedType);
    if (it != nameToIdMap.end()) {
        return it->second;
    }
    // TIMESTAMP TYPE
    std::string typeStr;
    if (LogicTypeUtils::startsWith(basicStrippedType, "TIMESTAMP_LTZ") ||
        LogicTypeUtils::startsWith(basicStrippedType, "TIMESTAMP_WITH_LOCAL_TIME_ZONE(")) {
        return DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    } else {
        typeStr = basicStrippedType.substr(basicStrippedType.find(')') + 1);
        it = nameToIdMap.find(typeStr);
        if (it != nameToIdMap.end()) {
            return it->second;
        }
        if (basicStrippedType.substr(0, 9) == "TIMESTAMP") {
            return DataTypeId::OMNI_TIMESTAMP;
        }
    }

    // DECIMAL TYPE
    if (basicStrippedType.substr(0, 7) == "DECIMAL") {
        size_t closeParen = basicStrippedType.find(')', 8);

        std::string numbers = basicStrippedType.substr(8, closeParen - 8);

        size_t commaPos = numbers.find(',');

        int precision = std::stoi(numbers.substr(0, commaPos));
        if (precision < 19) {
            return OMNI_DECIMAL64;
        } else {
            return OMNI_DECIMAL128;
        }
    }

    // Typename has '(' format
    typeStr = basicStrippedType.substr(0, basicStrippedType.find('('));
    it = nameToIdMap.find(typeStr);
    if (it != nameToIdMap.end()) {
        return it->second;
    }
    // Typename contains '<' format
    typeStr = basicStrippedType.substr(0, basicStrippedType.find('<'));
    it = nameToIdMap.find(typeStr);
    if (it != nameToIdMap.end()) {
        return it->second;
    }

    // INTERVAL TYPE
    if (basicStrippedType.substr(0, 13) == "INTERVAL YEAR" || basicStrippedType.substr(0, 14) == "INTERVAL MONTH") {
        return DataTypeId::OMNI_INTERVAL_MONTHS;
    } else if (basicStrippedType.substr(0, 8) == "INTERVAL") {
        return DataTypeId::OMNI_INTERVAL_DAY_TIME;
    }

    return DataTypeId::OMNI_INVALID;
}

std::pair<int32_t, int32_t> LogicalType::parseDecimalPrecisionScale(const std::string& flinkType)
{
    std::string basicStrippedType = LogicTypeUtils::stripFlinkTypeExtras(flinkType);
    if (basicStrippedType.substr(0, 7) == "DECIMAL") {
        size_t closeParen = basicStrippedType.find(')', 8);
        if (closeParen == std::string::npos) {
            return {DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE};
        }
        std::string numbers = basicStrippedType.substr(8, closeParen - 8);
        size_t commaPos = numbers.find(',');
        if (commaPos == std::string::npos) {
            return {DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE};
        }
        try {
            int32_t precision = std::stoi(numbers.substr(0, commaPos));
            int32_t scale = std::stoi(numbers.substr(commaPos + 1));
            return {precision, scale};
        } catch (...) {
            return {DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE};
        }
    }
    return {DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE};
}

LogicalType* LogicalType::flinkTypeToOmniType(const std::string& flinkType)
{
    buildNameToIdMap();
    std::string basicStrippedType = LogicTypeUtils::stripFlinkTypeExtras(flinkType);
    nlohmann::json options = LogicTypeUtils::optionsFromFlinkType(basicStrippedType);
    const bool isNullable = !LogicTypeUtils::isNotNullType(flinkType);
    // Nullability is encoded as a suffix in Flink's type string. Preserve it in
    // the options consumed by every BasicLogicalType construction branch.
    options["nullable"] = isNullable;

    if (LogicTypeUtils::startsWith(basicStrippedType, "RAW(")) {
        return parseRawType(flinkType, basicStrippedType, isNullable);
    }

    auto it = nameToIdMap.find(basicStrippedType);
    if (it != nameToIdMap.end()) {
        return BasicLogicalType::getTypeBy(it->second, options);
    }

    std::string typeStr;
    if (LogicTypeUtils::startsWith(basicStrippedType, "TIMESTAMP_LTZ") ||
        LogicTypeUtils::startsWith(basicStrippedType, "TIMESTAMP_WITH_LOCAL_TIME_ZONE(")) {
        return BasicLogicalType::getTypeBy(DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE, options);
    } else {
        typeStr = basicStrippedType.substr(basicStrippedType.find(')') + 1);
        it = nameToIdMap.find(typeStr);
        if (it != nameToIdMap.end()) {
            return BasicLogicalType::getTypeBy(it->second, options);
        }
        if (LogicTypeUtils::startsWith(basicStrippedType, "TIMESTAMP")) {
            return BasicLogicalType::getTypeBy(DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE, options);
        }
    }

    if (LogicTypeUtils::startsWith(basicStrippedType, "DECIMAL")) {
        size_t closeParen = basicStrippedType.find(')', 8);
        std::string numbers = basicStrippedType.substr(8, closeParen - 8);
        size_t commaPos = numbers.find(',');
        int precision = std::stoi(numbers.substr(0, commaPos));
        return BasicLogicalType::getTypeBy(precision < 19 ? OMNI_DECIMAL64 : OMNI_DECIMAL128, options);
    }

    typeStr = basicStrippedType.substr(0, basicStrippedType.find('('));
    it = nameToIdMap.find(typeStr);
    if (it != nameToIdMap.end()) {
        return BasicLogicalType::getTypeBy(it->second, options);
    }

    typeStr = basicStrippedType.substr(0, basicStrippedType.find('<'));
    it = nameToIdMap.find(typeStr);
    if (it != nameToIdMap.end()) {
        return BasicLogicalType::getTypeBy(it->second, options);
    }

    if (LogicTypeUtils::startsWith(basicStrippedType, "INTERVAL YEAR") ||
        LogicTypeUtils::startsWith(basicStrippedType, "INTERVAL MONTH")) {
        return BasicLogicalType::getTypeBy(DataTypeId::OMNI_INTERVAL_MONTHS, options);
    } else if (LogicTypeUtils::startsWith(basicStrippedType, "INTERVAL")) {
        return BasicLogicalType::getTypeBy(DataTypeId::OMNI_INTERVAL_DAY_TIME, options);
    }

    return BasicLogicalType::getTypeBy(DataTypeId::OMNI_INVALID, options);
}

LogicalType* LogicalType::parseRawType(
    const std::string& flinkType, const std::string& basicStrippedType, bool isNullable)
{
    std::vector<std::string> arguments;
    bool inQuote = false;
    std::string current;
    for (size_t i = basicStrippedType.find('(') + 1; i < basicStrippedType.size(); ++i) {
        const char ch = basicStrippedType[i];
        if (ch == '\'') {
            if (inQuote) {
                arguments.push_back(current);
                current.clear();
            }
            inQuote = !inQuote;
        } else if (inQuote) {
            current.push_back(ch);
        }
    }
    if (arguments.size() != 2 || arguments[0].empty()) {
        throw std::invalid_argument("Invalid Flink RAW type: " + flinkType);
    }
    return new omnistream::RawType(isNullable, arguments[0], arguments[1]);
}

bool LogicalType::isSharedLogicalType(const LogicalType* logicalType)
{
    return logicalType == nullptr || logicalType == BasicLogicalType::BOOLEAN ||
           logicalType == BasicLogicalType::INTEGER || logicalType == BasicLogicalType::BIGINT ||
           logicalType == BasicLogicalType::VARCHAR || logicalType == BasicLogicalType::DOUBLE ||
           logicalType == BasicLogicalType::DATE || logicalType == BasicLogicalType::TIME_WITHOUT_TIME_ZONE ||
           logicalType == BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE ||
           logicalType == BasicLogicalType::TIMESTAMP_WITH_TIME_ZONE ||
           logicalType == BasicLogicalType::TIMESTAMP_WITH_LOCAL_TIME_ZONE ||
           logicalType == BasicLogicalType::TIMESTAMP || logicalType == BasicLogicalType::INTERVAL_YEAR_MONTH ||
           logicalType == BasicLogicalType::INTERVAL_DAY_TIME || logicalType == BasicLogicalType::INVALID_TYPE;
}

void LogicalType::buildNameToIdMap()
{
    static std::once_flag flag;
    std::call_once(flag, []() {
        nameToIdMap["BIGINT"] = DataTypeId::OMNI_LONG;
        nameToIdMap["BOOLEAN"] = DataTypeId::OMNI_BOOLEAN;
        nameToIdMap["DATE"] = DataTypeId::OMNI_DATE32;
        nameToIdMap["DOUBLE"] = DataTypeId::OMNI_DOUBLE;
        nameToIdMap["FLOAT"] = DataTypeId::OMNI_INT;
        nameToIdMap["INT"] = DataTypeId::OMNI_INT;
        nameToIdMap["INTEGER"] = DataTypeId::OMNI_INT;
        nameToIdMap["SMALLINT"] = DataTypeId::OMNI_SHORT;
        nameToIdMap["TINYINT"] = DataTypeId::OMNI_SHORT;
        nameToIdMap["ARRAY"] = DataTypeId::OMNI_ARRAY;
        nameToIdMap["BINARY"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["CHAR"] = DataTypeId::OMNI_CHAR;
        nameToIdMap["DECIMAL"] = DataTypeId::OMNI_DECIMAL64;
        nameToIdMap["MAP"] = DataTypeId::OMNI_MAP;
        nameToIdMap["MULTISET"] = DataTypeId::OMNI_MULTISET;
        nameToIdMap["RAW"] = DataTypeId::OMNI_CONTAINER;
        nameToIdMap["ROW"] = DataTypeId::OMNI_CONTAINER;
        nameToIdMap["SYMBOL"] = DataTypeId::OMNI_CONTAINER;
        nameToIdMap["BYTES"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["STRING"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["VARBINARY"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["VARCHAR"] = DataTypeId::OMNI_VARCHAR;
        nameToIdMap["TIME"] = DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP"] = DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;

        nameToIdMap[" WITH TIME ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE;
        nameToIdMap[" WITH LOCAL TIME ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;

        nameToIdMap["INVALID"] = DataTypeId::OMNI_INVALID;

        // These are the extra LogicalTypeRoot typeName
        nameToIdMap["TIME_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITHOUT_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP;
        nameToIdMap["TIMESTAMP_WITH_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE;
        nameToIdMap["TIMESTAMP_WITH_LOCAL_TIME_ZONE"] = DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        nameToIdMap["INTERVAL_YEAR_MONTH"] = DataTypeId::OMNI_INTERVAL_MONTHS;
        nameToIdMap["INTERVAL_DAY_TIME"] = DataTypeId::OMNI_INTERVAL_DAY_TIME;
    });
}

BasicLogicalType* BasicLogicalType::BOOLEAN = new BasicLogicalType(true, DataTypeId::OMNI_BOOLEAN, "BOOLEAN");
BasicLogicalType* BasicLogicalType::INTEGER = new BasicLogicalType(true, DataTypeId::OMNI_INT, "INTEGER");
BasicLogicalType* BasicLogicalType::BIGINT = new BasicLogicalType(true, DataTypeId::OMNI_LONG, "BIGINT");
BasicLogicalType* BasicLogicalType::VARCHAR = new VarCharType(true, std::numeric_limits<int>::max());
BasicLogicalType* BasicLogicalType::DOUBLE = new BasicLogicalType(true, DataTypeId::OMNI_DOUBLE, "DOUBLE");
BasicLogicalType* BasicLogicalType::DATE = new BasicLogicalType(true, DataTypeId::OMNI_DATE32, "DATE");
BasicLogicalType* BasicLogicalType::TIME_WITHOUT_TIME_ZONE = new TimeWithoutTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE = new TimestampWithoutTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITH_TIME_ZONE = new TimestampWithTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP_WITH_LOCAL_TIME_ZONE = new TimestampWithLocalTimeZoneType(true);
BasicLogicalType* BasicLogicalType::TIMESTAMP = new TimestampWithLocalTimeZoneType(true);
BasicLogicalType* BasicLogicalType::INTERVAL_YEAR_MONTH =
    new BasicLogicalType(true, DataTypeId::OMNI_INTERVAL_MONTHS, "INTERVAL_YEAR_MONTH");
BasicLogicalType* BasicLogicalType::INTERVAL_DAY_TIME =
    new BasicLogicalType(true, DataTypeId::OMNI_INTERVAL_DAY_TIME, "INTERVAL_DAY_TIME");
BasicLogicalType* BasicLogicalType::INVALID_TYPE = new BasicLogicalType(true, DataTypeId::OMNI_INVALID, "UNRESOLVED");

LogicalType* BasicLogicalType::getTypeBy(DataTypeId typeId, const nlohmann::json& options)
{
    LogicalType* type = nullptr;
    bool nullable = options.value("nullable", true);
    switch (typeId) {
        case DataTypeId::OMNI_BOOLEAN: {
            if (nullable == BasicLogicalType::BOOLEAN->isNullable()) {
                type = BasicLogicalType::BOOLEAN;
            } else {
                type = new BasicLogicalType(nullable, typeId, "BOOLEAN");
            }
            break;
        }
        case DataTypeId::OMNI_INT: {
            if (nullable == BasicLogicalType::INTEGER->isNullable()) {
                type = BasicLogicalType::INTEGER;
            } else {
                type = new BasicLogicalType(nullable, typeId, "INT");
            }
            break;
        }
        case DataTypeId::OMNI_LONG: {
            if (nullable == BasicLogicalType::BIGINT->isNullable()) {
                type = BasicLogicalType::BIGINT;
            } else {
                type = new BasicLogicalType(nullable, typeId, "BIGINT");
            }
            break;
        }
        case DataTypeId::OMNI_DOUBLE: {
            if (nullable == BasicLogicalType::DOUBLE->isNullable()) {
                type = BasicLogicalType::DOUBLE;
            } else {
                type = new BasicLogicalType(nullable, typeId, "DOUBLE");
            }
            break;
        }
        case DataTypeId::OMNI_DATE32: {
            if (nullable == BasicLogicalType::DATE->isNullable()) {
                type = BasicLogicalType::DATE;
            } else {
                type = new BasicLogicalType(nullable, typeId, "DATE");
            }
            break;
        }
        case DataTypeId::OMNI_CHAR:
        case DataTypeId::OMNI_VARCHAR:
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DataTypeId::OMNI_DECIMAL64:
        case DataTypeId::OMNI_DECIMAL128:
        case DataTypeId::OMNI_INTERVAL_MONTHS:
        case DataTypeId::OMNI_INTERVAL_DAY_TIME:
        case DataTypeId::OMNI_CONTAINER: {
            type = BasicLogicalType::getTypeBy(nullable, typeId, options);
            break;
        }
        case DataTypeId::OMNI_INVALID: {
            if (nullable == BasicLogicalType::INVALID_TYPE->isNullable()) {
                type = BasicLogicalType::INVALID_TYPE;
            } else {
                type = new BasicLogicalType(nullable, typeId, "UNRESOLVED");
            }
            break;
        }
        default: THROW_LOGIC_EXCEPTION("Unsupported DataTypeId : " << typeId << " in inputRowType.");
    }

    return type;
}

LogicalType* BasicLogicalType::getTypeBy(std::optional<bool> nullable, DataTypeId typeId, const nlohmann::json& options)
{
    const bool isNullable = nullable.value_or(true);
    LogicalType* type = nullptr;
    switch (typeId) {
        case DataTypeId::OMNI_BOOLEAN: {
            type = new BasicLogicalType(isNullable, DataTypeId::OMNI_BOOLEAN, "BOOLEAN");
            break;
        }
        case DataTypeId::OMNI_INT: {
            type = new BasicLogicalType(isNullable, DataTypeId::OMNI_INT, "INTEGER");
            break;
        }
        case DataTypeId::OMNI_LONG: {
            type = new BasicLogicalType(isNullable, DataTypeId::OMNI_LONG, "BIGINT");
            break;
        }
        case DataTypeId::OMNI_CHAR: {
            int length = options.value("length", 1);
            type = new CharType(isNullable, length);
            break;
        }
        case DataTypeId::OMNI_VARCHAR: {
            int length = options.value("length", std::numeric_limits<int>::max());
            type = new VarCharType(isNullable, length);
            break;
        }
        case DataTypeId::OMNI_DOUBLE: {
            type = new BasicLogicalType(isNullable, DataTypeId::OMNI_DOUBLE, "DOUBLE");
            break;
        }
        case DataTypeId::OMNI_DATE32: {
            type = new BasicLogicalType(isNullable, DataTypeId::OMNI_DATE32, "DATE");
            break;
        }
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
            int precision = options.value("precision", 0);
            type = new TimeWithoutTimeZoneType(isNullable, precision);
            break;
        }
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE: {
            int precision = options.value("precision", 0);
            type = new TimestampWithoutTimeZoneType(isNullable, precision);
            break;
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE: {
            int precision = options.value("precision", 0);
            type = new TimestampWithTimeZoneType(isNullable, precision);
            break;
        }
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            int precision = options.value("precision", 0);
            type = new TimestampWithLocalTimeZoneType(isNullable, precision);
            break;
        }
        case DataTypeId::OMNI_DECIMAL64: {
            type = new BasicLogicalType(isNullable, typeId, "DECIMAL64");
            break;
        }
        case DataTypeId::OMNI_DECIMAL128: {
            type = new BasicLogicalType(isNullable, typeId, "DECIMAL128");
            break;
        }
        case DataTypeId::OMNI_INTERVAL_MONTHS: {
            type = new BasicLogicalType(isNullable, typeId, "INTERVAL_MONTHS");
            break;
        }
        case DataTypeId::OMNI_INTERVAL_DAY_TIME: {
            type = new BasicLogicalType(isNullable, typeId, "INTERVAL_DAY_TIME");
            break;
        }
        case DataTypeId::OMNI_INVALID: {
            type = new BasicLogicalType(isNullable, typeId, "UNRESOLVED");
            break;
        }
        case DataTypeId::OMNI_CONTAINER: {
            const std::string typeName = options.value("type", "");
            if (typeName == "RAW") {
                const std::string className = options.value("class", "");
                const std::string serializerString = options.value("serializer", "");
                type = new omnistream::RawType(isNullable, className, serializerString);
                break;
            }
            THROW_LOGIC_EXCEPTION(
                "OMNI_CONTAINER requires RAW metadata; ROW must be constructed explicitly, typeName=" << typeName);
        }
        default: THROW_LOGIC_EXCEPTION("Unsupported DataTypeId : " << typeId << " in inputRowType.");
    }

    return type;
}
