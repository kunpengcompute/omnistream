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

#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <limits>
#include <regex>
#include <stdexcept>
#include <string>
#include <tuple>
#include "core/api/common/serialization/DeserializationSchema.h"
#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"

class JsonRowDataDeserializationSchema : public DeserializationSchema {
public:
    /**
     * @param flinkCompatibleConversions When true, scalar conversions mirror Flink's
     *        JsonToRowDataConverters (numeric/string fallbacks, strict TIME parsing). Debezium
     *        enables this so that the CDC source follows Flink semantics for the supported types,
     *        while other JSON sources keep the legacy behaviour.
     */
    explicit JsonRowDataDeserializationSchema(
        const nlohmann::json& opDescriptionJSON, bool enableFlinkCompatibleConversions = false)
        : fieldNames(opDescriptionJSON.at("outputNames").get<std::vector<std::string>>()),
          flinkCompatibleConversions(enableFlinkCompatibleConversions)
    {
        auto outputTypes = opDescriptionJSON.at("outputTypes").get<std::vector<std::string>>();
        std::regex pattern(R"(DECIMAL\d*\((\d+),\s*(\d+)\))");
        std::smatch match;
        for (std::string type : outputTypes) {
            fieldTypes.push_back(LogicalType::flinkTypeToOmniTypeId(type));
            if (std::regex_search(type, match, pattern)) {
                decimalScales.push_back(std::stoi(match[2].str()));
            } else {
                decimalScales.push_back(0);
            }
        }
    }

    void Open() override
    {
    }

    void deserialize(const uint8_t* message, size_t length, Collector* out) override
    {
        auto* vectorBatch = deserializeSingleRecord(message, length);
        if (vectorBatch != nullptr) {
            out->collect(vectorBatch);
        }
    }

    void* deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec) override
    {
        int rowSize = static_cast<int>(messageVec.size());
        int colSize = static_cast<int>(fieldNames.size());
        auto* vectorBatch = createBatch(rowSize, fieldTypes);
        nlohmann::json node;
        for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            node = nlohmann::json::parse(
                std::string_view(reinterpret_cast<const char*>(messageVec[rowIndex]), lengthVec[rowIndex]));
            for (int colIndex = 0; colIndex < colSize; colIndex++) {
                setColValue(rowIndex, colIndex, vectorBatch, node);
            }
        }
        return vectorBatch;
    }

    void setColValue(int rowIndex, int colIndex, omnistream::VectorBatch* vectorBatch, nlohmann::json& node)
    {
        auto field = node.find(fieldNames[colIndex]);
        setColValue(rowIndex, colIndex, vectorBatch, field == node.end() ? nullptr : &*field);
    }

    void setColValue(int rowIndex, int colIndex, omnistream::VectorBatch* vectorBatch, const nlohmann::json* fieldIt)
    {
        auto type = fieldTypes[colIndex];
        if (fieldIt == nullptr || fieldIt->is_null()) {
            vectorBatch->Get(colIndex)->SetNull(rowIndex);
            return;
        }

        switch (type) {
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                bool value = flinkCompatibleConversions ? convertToBoolean(*fieldIt) : fieldIt->get<bool>();
                vectorBatch->SetValueAt(colIndex, rowIndex, value);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_INT: {
                int32_t value = flinkCompatibleConversions ? convertToInt(*fieldIt) : fieldIt->get<int32_t>();
                vectorBatch->SetValueAt(colIndex, rowIndex, value);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DATE32: {
                // Flink JSON format serializes DATE as an ISO-8601 string (e.g. "2024-01-01"),
                // so parse the string form; keep numeric epoch-days support for compatibility.
                int32_t epochDays;
                if (fieldIt->is_number()) {
                    epochDays = fieldIt->get<int32_t>();
                } else if (fieldIt->is_string()) {
                    auto result = omniruntime::type::util::fromDateString(fieldIt->get<std::string>());
                    if (result.hasError()) {
                        throw std::runtime_error("invalid date value: " + fieldIt->dump());
                    }
                    epochDays = result.value();
                } else {
                    throw std::runtime_error("invalid date value: " + fieldIt->dump());
                }
                vectorBatch->SetValueAt(colIndex, rowIndex, epochDays);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_LONG: {
                int64_t value = flinkCompatibleConversions ? convertToLong(*fieldIt) : fieldIt->get<int64_t>();
                vectorBatch->SetValueAt(colIndex, rowIndex, value);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<double>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
                long value = flinkCompatibleConversions
                                 ? convertToTimeMillisOfDay(*fieldIt)
                                 : TimestampData::stringToMillisOfDay(fieldIt->get<std::string>());
                vectorBatch->SetValueAt(colIndex, rowIndex, value);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP: {
                vectorBatch->SetValueAt(
                    colIndex, rowIndex, TimestampData::stringToEpochMillis(fieldIt->get<std::string>()));
                break;
            }
            case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE): {
                const TimestampData timeString = TimestampData::fromLocalTimeString(fieldIt->get<std::string>());
                vectorBatch->SetValueAt(colIndex, rowIndex, timeString.getMillisecond());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_CHAR:
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                auto stringVec = reinterpret_cast<
                    omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    vectorBatch->Get(colIndex));
                std::string value;
                if (fieldIt->is_string()) {
                    value = fieldIt->get<std::string>();
                } else {
                    value = fieldIt->dump(); // 支持数值类型隐式转换
                }
                std::string_view strView(value.data(), value.size());
                stringVec->SetValue(rowIndex, strView);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
                std::string valueStr;
                if (fieldIt->is_string()) {
                    valueStr = fieldIt->get<std::string>();
                } else {
                    valueStr = fieldIt->dump();
                }
                int32_t scale = decimalScales[colIndex];
                bool negative = false;
                if (!valueStr.empty() && valueStr[0] == '-') {
                    negative = true;
                    valueStr = valueStr.substr(1);
                }
                std::string intPart, fracPart;
                size_t dotPos = valueStr.find('.');
                if (dotPos != std::string::npos) {
                    intPart = valueStr.substr(0, dotPos);
                    fracPart = valueStr.substr(dotPos + 1);
                } else {
                    intPart = valueStr;
                    fracPart = "";
                }
                if (static_cast<int32_t>(fracPart.length()) < scale) {
                    fracPart += std::string(scale - fracPart.length(), '0');
                } else if (static_cast<int32_t>(fracPart.length()) > scale) {
                    fracPart = fracPart.substr(0, scale);
                }
                long unscaledValue = std::stol(intPart + fracPart);
                if (negative) {
                    unscaledValue = -unscaledValue;
                }
                vectorBatch->SetValueAt(colIndex, rowIndex, unscaledValue);
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                std::string valueStr;
                if (fieldIt->is_string()) {
                    valueStr = fieldIt->get<std::string>();
                } else {
                    valueStr = fieldIt->dump();
                }
                int32_t scale = decimalScales[colIndex];
                bool negative = false;
                if (!valueStr.empty() && valueStr[0] == '-') {
                    negative = true;
                    valueStr = valueStr.substr(1);
                }
                std::string intPart, fracPart;
                size_t dotPos = valueStr.find('.');
                if (dotPos != std::string::npos) {
                    intPart = valueStr.substr(0, dotPos);
                    fracPart = valueStr.substr(dotPos + 1);
                } else {
                    intPart = valueStr;
                    fracPart = "";
                }
                if (static_cast<int32_t>(fracPart.length()) < scale) {
                    fracPart += std::string(scale - fracPart.length(), '0');
                } else if (static_cast<int32_t>(fracPart.length()) > scale) {
                    fracPart = fracPart.substr(0, scale);
                }
                std::string unscaledStr = intPart + fracPart;
                if (negative) {
                    unscaledStr = "-" + unscaledStr;
                }
                Decimal128 decimalValue{std::string_view(unscaledStr)};
                vectorBatch->SetValueAt(colIndex, rowIndex, decimalValue);
                break;
            }
            default: std::runtime_error("DataType not supported yet!");
        }
    }

    bool isEndOfStream(const void* nextElement) override
    {
        return false;
    }

private:
    omnistream::VectorBatch* deserializeSingleRecord(const uint8_t* message, size_t length)
    {
        std::vector<const uint8_t*> messageVec{message};
        std::vector<size_t> lengthVec{length};
        return reinterpret_cast<omnistream::VectorBatch*>(deserialize(messageVec, lengthVec));
    }

    static std::string trimSpaces(const std::string& value)
    {
        const char* whitespace = " \t\r\n";
        size_t start = value.find_first_not_of(whitespace);
        if (start == std::string::npos) {
            return "";
        }
        size_t end = value.find_last_not_of(whitespace);
        return value.substr(start, end - start + 1);
    }

    static long long parseIntegerText(const std::string& value)
    {
        std::string text = trimSpaces(value);
        if (text.empty()) {
            throw std::runtime_error("Failed to parse integer from an empty value");
        }
        size_t parsed = 0;
        long long parsedValue = std::stoll(text, &parsed);
        if (parsed != text.size()) {
            throw std::runtime_error("Failed to parse integer from '" + value + "'");
        }
        return parsedValue;
    }

    static bool convertToBoolean(const nlohmann::json& node)
    {
        if (node.is_boolean()) {
            return node.get<bool>();
        }
        if (node.is_string()) {
            std::string text = trimSpaces(node.get<std::string>());
            std::transform(text.begin(), text.end(), text.begin(),
                [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return text == "true";
        }
        // Flink uses jsonNode.asText(): non-string scalars are never "true".
        return false;
    }

    static int32_t convertToInt(const nlohmann::json& node)
    {
        if (node.is_number_integer()) {
            long long value = node.get<long long>();
            if (value >= std::numeric_limits<int32_t>::min() &&
                value <= std::numeric_limits<int32_t>::max()) {
                return static_cast<int32_t>(value);
            }
            throw std::runtime_error("Integer value out of range: " + node.dump());
        }
        if (node.is_number_unsigned()) {
            unsigned long long value = node.get<unsigned long long>();
            if (value <= static_cast<unsigned long long>(std::numeric_limits<int32_t>::max())) {
                return static_cast<int32_t>(value);
            }
            throw std::runtime_error("Integer value out of range: " + node.dump());
        }
        if (node.is_number_float()) {
            double value = node.get<double>();
            if (std::isfinite(value) &&
                value >= static_cast<double>(std::numeric_limits<int32_t>::min()) &&
                value <= static_cast<double>(std::numeric_limits<int32_t>::max())) {
                return static_cast<int32_t>(value);
            }
            throw std::runtime_error("Integer value out of range: " + node.dump());
        }
        if (node.is_string()) {
            long long value = parseIntegerText(node.get<std::string>());
            if (value >= std::numeric_limits<int32_t>::min() &&
                value <= std::numeric_limits<int32_t>::max()) {
                return static_cast<int32_t>(value);
            }
            throw std::runtime_error("Integer value out of range: " + node.dump());
        }
        throw std::runtime_error("Cannot convert JSON value to INTEGER: " + node.dump());
    }

    static int64_t convertToLong(const nlohmann::json& node)
    {
        if (node.is_number_integer()) {
            return node.get<int64_t>();
        }
        if (node.is_number_unsigned()) {
            unsigned long long value = node.get<unsigned long long>();
            if (value <= static_cast<unsigned long long>(std::numeric_limits<int64_t>::max())) {
                return static_cast<int64_t>(value);
            }
            throw std::runtime_error("Bigint value out of range: " + node.dump());
        }
        if (node.is_number_float()) {
            double value = node.get<double>();
            if (std::isfinite(value) && value >= -9223372036854775808.0 && value < 9223372036854775808.0) {
                return static_cast<int64_t>(value);
            }
            throw std::runtime_error("Bigint value out of range: " + node.dump());
        }
        if (node.is_string()) {
            return static_cast<int64_t>(parseIntegerText(node.get<std::string>()));
        }
        throw std::runtime_error("Cannot convert JSON value to BIGINT: " + node.dump());
    }

    static long convertToTimeMillisOfDay(const nlohmann::json& node)
    {
        if (!node.is_string()) {
            throw std::runtime_error("Cannot convert JSON value to TIME: " + node.dump());
        }
        std::string text = trimSpaces(node.get<std::string>());
        // Mirrors Flink's SQL_TIME_FORMAT: HH:mm:ss with an optional fractional second.
        if (text.size() < 8) {
            throw std::runtime_error("Failed to parse time string: " + text);
        }
        auto isDigit = [](char c) { return c >= '0' && c <= '9'; };
        if (!isDigit(text[0]) || !isDigit(text[1]) || text[2] != ':' ||
            !isDigit(text[3]) || !isDigit(text[4]) || text[5] != ':' ||
            !isDigit(text[6]) || !isDigit(text[7])) {
            throw std::runtime_error("Failed to parse time string: " + text);
        }
        if (text.size() > 8) {
            if (text[8] != '.') {
                throw std::runtime_error("Failed to parse time string: " + text);
            }
            for (size_t i = 9; i < text.size(); ++i) {
                if (!isDigit(text[i])) {
                    throw std::runtime_error("Failed to parse time string: " + text);
                }
            }
        }
        const int hour = (text[0] - '0') * 10 + (text[1] - '0');
        const int minute = (text[3] - '0') * 10 + (text[4] - '0');
        const int second = (text[6] - '0') * 10 + (text[7] - '0');
        if (hour > 23 || minute > 59 || second > 59) {
            throw std::runtime_error("Failed to parse time string: " + text);
        }
        // Flink's JsonToRowDataConverters.convertToTime parses SQL_TIME_FORMAT (which accepts an
        // optional fractional second) and then returns localTime.toSecondOfDay() * 1000. Fractional
        // seconds are intentionally truncated, so keep this behavior for Flink compatibility.
        return static_cast<long>(hour * 3600 + minute * 60 + second) * 1000;
    }

    std::vector<std::string> fieldNames;
    std::vector<omniruntime::type::DataTypeId> fieldTypes;
    std::vector<int32_t> decimalScales;
    bool flinkCompatibleConversions = false;
};
