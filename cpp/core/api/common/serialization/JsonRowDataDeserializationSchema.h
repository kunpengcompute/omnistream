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

#include <tuple>
#include <regex>
#include "core/api/common/serialization/DeserializationSchema.h"
#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"

class JsonRowDataDeserializationSchema : public DeserializationSchema {
public:
    explicit JsonRowDataDeserializationSchema(const nlohmann::json& opDescriptionJSON)
        : fieldNames(opDescriptionJSON.at("outputNames").get<std::vector<std::string>>())
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
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<bool>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_INT: {
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<int32_t>());
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
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<int64_t>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                vectorBatch->SetValueAt(colIndex, rowIndex, fieldIt->get<double>());
                break;
            }
            case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
                vectorBatch->SetValueAt(
                    colIndex, rowIndex, TimestampData::stringToMillisOfDay(fieldIt->get<std::string>()));
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

    std::vector<std::string> fieldNames;
    std::vector<omniruntime::type::DataTypeId> fieldTypes;
    std::vector<int32_t> decimalScales;
};
