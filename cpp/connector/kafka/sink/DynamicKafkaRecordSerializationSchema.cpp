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

#include "DynamicKafkaRecordSerializationSchema.h"
#include "data/vectorbatch/VectorBatch.h"
#include <algorithm>
#include <regex>

namespace {
bool IsDecimalType(const std::string& inputType)
{
    return inputType.rfind("DECIMAL", 0) == 0;
}

bool IsJsonDecimalNumber(const std::string& value)
{
    size_t index = 0;
    if (index < value.size() && value[index] == '-') {
        ++index;
    }
    if (index == value.size()) {
        return false;
    }

    if (value[index] == '0') {
        ++index;
    } else {
        if (value[index] < '1' || value[index] > '9') {
            return false;
        }
        while (index < value.size() && value[index] >= '0' && value[index] <= '9') {
            ++index;
        }
    }

    if (index == value.size()) {
        return true;
    }
    if (value[index] != '.') {
        return false;
    }
    ++index;
    if (index == value.size()) {
        return false;
    }
    while (index < value.size() && value[index] >= '0' && value[index] <= '9') {
        ++index;
    }
    return index == value.size();
}
} // namespace

DynamicKafkaRecordSerializationSchema::DynamicKafkaRecordSerializationSchema(
    std::vector<std::string>& inputFields, std::vector<std::string>& inputTypes)
    : inputFields_(inputFields),
      inputTypes_(inputTypes)
{
    std::regex pattern(R"(DECIMAL(?:64|128)?\s*\((\d+)\s*,\s*(\d+)\))");
    std::smatch match;

    for (size_t i = 0; i < inputTypes.size(); ++i) {
        const std::string& inputType = inputTypes[i];
        serializedFieldNames_.emplace_back(nlohmann::ordered_json(inputFields[i]).dump());
        if (std::regex_search(inputType, match, pattern)) {
            int precision = std::stoi(match[1].str());
            int scale = std::stoi(match[2].str());
            decimalInfo.emplace_back(precision, scale);
        } else {
            if (IsDecimalType(inputType)) {
                INFO_RELEASE("Invalid decimal input type: " + inputType);
            }
            decimalInfo.emplace_back(-1, -1);
        }
    }
}

void DynamicKafkaRecordSerializationSchema::RowToJson(RowData* row)
{
    for (size_t i = 0; i < inputTypes_.size(); ++i) {
        if (inputTypes_[i] == "BIGINT") {
            j[inputFields_[i]] = *row->getLong(i);
        } else if (inputTypes_[i].find("TIMESTAMP") != std::string::npos) {
            oss.str("");
            oss.clear();
            timeBuffer[0] = '\0';
            auto millis = *row->getLong(i);
            int milliseconds = millis % 1000;
            time_t seconds = millis / 1000;
            // 转换为 tm 结构体（UTC时间）
            struct tm timeinfo;
            gmtime_r(&seconds, &timeinfo);
            strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &timeinfo);
            oss << timeBuffer << "." << std::setw(3) << std::setfill('0') // 强制3位宽度，不足补零
                << milliseconds;
            j[inputFields_[i]] = omnistream::VectorBatch::RemoveTrailingZeros(oss.str());
        } else if (inputTypes_[i] == "INTEGER") {
            j[inputFields_[i]] = *row->getInt(i);
        } else if (inputTypes_[i] == "DOUBLE") {
            j[inputFields_[i]] = *row->getLong(i);
        } else if (inputTypes_[i] == "BOOLEAN") {
            j[inputFields_[i]] = *row->getInt(i);
        } else if (
            inputTypes_[i] == "STRING" || inputTypes_[i] == "VARCHAR" || inputTypes_[i] == "VARCHAR(2147483647)") {
            BinaryStringData* stringRowData = row->getString(i);
            std::string strValue = std::string(stringRowData->toString()->begin(), stringRowData->toString()->end());
            j[inputFields_[i]] = strValue;
        } else {
            LOG("RowToJson(RowData) Data type not supported: " << inputTypes_[i]);
            throw std::runtime_error("RowToJson(RowData) Data type not supported");
        };
    }
}

void DynamicKafkaRecordSerializationSchema::RowToJson(omnistream::VectorBatch* input, int rowIndex)
{
    input->convertToJson(j, rowIndex, decimalInfo, inputTypes_, inputFields_);
}

std::string DynamicKafkaRecordSerializationSchema::dumpJsonWithExactDecimals() const
{
    std::string jsonStr = "{";

    for (size_t i = 0; i < inputFields_.size(); ++i) {
        if (i != 0) {
            jsonStr += ',';
        }

        jsonStr += serializedFieldNames_[i];
        jsonStr += ':';

        const auto& value = j.at(inputFields_[i]);
        if (!IsDecimalType(inputTypes_[i]) || value.is_null()) {
            jsonStr += value.dump();
            continue;
        }

        if (!value.is_string()) {
            throw std::runtime_error("Decimal JSON value is not represented by an exact decimal string");
        }
        const auto& decimalValue = value.get_ref<const std::string&>();
        if (!IsJsonDecimalNumber(decimalValue)) {
            throw std::runtime_error("Invalid decimal JSON number: " + decimalValue);
        }
        // nlohmann::json only has double for non-integral numbers. Writing the validated
        // decimal token directly avoids both JSON quotes and precision loss for DECIMAL128.
        jsonStr += omnistream::VectorBatch::RemoveTrailingZeros(decimalValue);
    }

    jsonStr += '}';
    return jsonStr;
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(RowData* consumedRow)
{
    j.clear();
    RowToJson(consumedRow);
    std::string jsonStr = j.dump();
    return {nullptr, jsonStr.data()};
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(omnistream::VectorBatch* input, int rowIndex)
{
    j.clear();
    RowToJson(input, rowIndex);
    std::string jsonStr = dumpJsonWithExactDecimals();
    return {nullptr, jsonStr.data()};
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(String* element)
{
    return {nullptr, element->data()};
}

GenericRowData DynamicKafkaRecordSerializationSchema::createProjectedRow(
    RowData& sourceRow, RowKind kind, std::vector<FieldGetter>& getters)
{
    GenericRowData projectedRow(getters.size());
    for (size_t i = 0; i < getters.size(); ++i) {
        void* fieldPtr = getters[i].getFieldOrNull(&sourceRow);
        if (fieldPtr != nullptr) {
            projectedRow.setField(i, *static_cast<long*>(fieldPtr));
        } else {
            // 处理 nullptr 的情况，例如设置默认值
            projectedRow.setField(i, 0L); // 假设设置默认值为 0
        }
    }
    return projectedRow;
}

void to_json(nlohmann::json& j, const RowKind& kind)
{
    switch (kind) {
        case RowKind::INSERT: j = "INSERT"; break;
        case RowKind::UPDATE_BEFORE: j = "UPDATE_BEFORE"; break;
        case RowKind::UPDATE_AFTER: j = "UPDATE_AFTER"; break;
        case RowKind::DELETE: j = "DELETE"; break;
        default: j = "UNKNOWN";
    }
}

void to_json(nlohmann::json& j, const Row& row)
{
    j["kind"] = row.getKind();
    j["arity"] = row.getFieldByPosition().size();
}

KeyValueByteContainer DynamicKafkaRecordSerializationSchema::Serialize(Row* row)
{
    nlohmann::json j = *row;
    std::string jsonStr = j.dump();
    return {nullptr, jsonStr.data()};
}
