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

#include "DynamicKafkaRecordSerializationSchema.h"
#include "data/vectorbatch/VectorBatch.h"
#include "table/types/logical/LogicalType.h"
#include "OmniOperatorJIT/core/src/type/Conversions.h"
#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"
#include <algorithm>
#include <cmath>

using namespace omniruntime::type;

DynamicKafkaRecordSerializationSchema::DynamicKafkaRecordSerializationSchema(
    std::vector<std::string>& inputFields, std::vector<std::string>& inputTypes)
    : inputFields_(inputFields),
      inputTypes_(inputTypes)
{
    std::regex pattern(R"(DECIMAL\d+\((\d+),\s*(\d+)\))");
    std::smatch match;

    for (const std::string& inputType : inputTypes) {
        if (std::regex_search(inputType, match, pattern)) {
            int precision = std::stoi(match[1].str());
            int scale = std::stoi(match[2].str());
            decimalInfo.emplace_back(precision, scale);
        } else {
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

DynamicKafkaRecordSerializationSchema::DynamicKafkaRecordSerializationSchema(const nlohmann::json& description)
    : keyProjection(description.at("keyProjection").get<std::vector<int>>()),
      valueProjection(description.at("valueProjection").get<std::vector<int>>()),
      keyFieldNames(description.at("keyFieldNames").get<std::vector<std::string>>()),
      valueFieldNames(description.at("valueFieldNames").get<std::vector<std::string>>())
{
    for (const auto& type : description.at("physicalTypes")) {
        const auto& name = type.get_ref<const std::string&>();
        physicalTypes.push_back(LogicalType::flinkTypeToOmniTypeId(name));
        decimalInfo.push_back(LogicalType::parseDecimalPrecisionScale(name));
    }
}

std::string DynamicKafkaRecordSerializationSchema::serializeProjection(
    omnistream::VectorBatch* input,
    int rowIndex,
    const std::vector<int>& projection,
    const std::vector<std::string>& names)
{
    using namespace omniruntime::vec;
    std::string object = "{";
    for (size_t i = 0; i < projection.size(); ++i) {
        if (i != 0) {
            object += ',';
        }
        int column = projection[i];
        auto* vector = input->Get(column);
        object += nlohmann::json(names[i]).dump();
        object += ':';
        if (vector->IsNull(rowIndex)) {
            object += "null";
            continue;
        }
        nlohmann::json value;
        switch (physicalTypes[column]) {
            case OMNI_LONG: value = static_cast<Vector<int64_t>*>(vector)->GetValue(rowIndex); break;
            case OMNI_INT: value = static_cast<Vector<int32_t>*>(vector)->GetValue(rowIndex); break;
            case OMNI_DOUBLE: {
                double number = static_cast<Vector<double>*>(vector)->GetValue(rowIndex);
                auto formatted = omniruntime::type::util::Converter<OMNI_VARCHAR>::tryCast(number).value();
                object += std::isfinite(number) ? formatted : nlohmann::json(formatted).dump();
                continue;
            }
            case OMNI_BOOLEAN: value = static_cast<Vector<bool>*>(vector)->GetValue(rowIndex); break;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                value = vector->GetEncoding() == OMNI_FLAT
                            ? static_cast<Vector<LargeStringContainer<std::string_view>>*>(vector)->GetValue(rowIndex)
                            : static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>>*>(vector)
                                  ->GetValue(rowIndex);
                break;
            case OMNI_DATE32:
                value = omniruntime::type::util::ToIso8601(static_cast<Vector<int32_t>*>(vector)->GetValue(rowIndex));
                break;
            case OMNI_TIME_WITHOUT_TIME_ZONE: value = input->TransformOnlyTime(column, rowIndex); break;
            case OMNI_TIMESTAMP:
            case OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                value = omnistream::VectorBatch::RemoveTrailingZeros(input->TransformTime(column, rowIndex));
                break;
            case OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = omnistream::VectorBatch::RemoveTrailingZeros(input->TransformTime(column, rowIndex)) + "Z";
                break;
            case OMNI_DECIMAL64: object += input->transformDecimal64(column, rowIndex, decimalInfo); continue;
            case OMNI_DECIMAL128: object += input->transformDecimal128(column, rowIndex, decimalInfo); continue;
            default: throw std::runtime_error("Unsupported upsert-kafka JSON field type");
        }
        object += value.dump();
    }
    object += '}';
    return object;
}

DynamicKafkaRecordSerializationSchema::UpsertRecord DynamicKafkaRecordSerializationSchema::serializeUpsert(
    omnistream::VectorBatch* input, int rowIndex)
{
    UpsertRecord record;
    record.key = serializeProjection(input, rowIndex, keyProjection, keyFieldNames);
    const auto kind = input->getRowKind(rowIndex);
    if (kind != RowKind::DELETE && kind != RowKind::UPDATE_BEFORE) {
        record.value = serializeProjection(input, rowIndex, valueProjection, valueFieldNames);
    }
    return record;
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
    std::string jsonStr = j.dump();
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
