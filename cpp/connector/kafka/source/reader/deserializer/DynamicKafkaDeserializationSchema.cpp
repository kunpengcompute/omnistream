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

#include "DynamicKafkaDeserializationSchema.h"

#include <algorithm>
#include "table/types/logical/LogicalType.h"

namespace {
void project(
    const nlohmann::json& object,
    const std::vector<int>& projection,
    const std::vector<std::string>& names,
    std::vector<const nlohmann::json*>& fields)
{
    if (!object.is_object()) {
        throw std::runtime_error("Expected a JSON object in upsert-kafka record");
    }
    for (size_t i = 0; i < projection.size(); ++i) {
        auto field = object.find(names[i]);
        fields[projection[i]] = field == object.end() ? nullptr : &*field;
    }
}

} // namespace

DynamicKafkaDeserializationSchema::DynamicKafkaDeserializationSchema(const nlohmann::json& description)
    : rowDeserializer(description),
      keyProjection(description.at("keyProjection").get<std::vector<int>>()),
      valueProjection(description.at("valueProjection").get<std::vector<int>>()),
      keyNames(description.at("keyFieldNames").get<std::vector<std::string>>()),
      valueNames(description.at("valueFieldNames").get<std::vector<std::string>>())
{
    for (const auto& type : description.at("outputTypes")) {
        const auto& name = type.get_ref<const std::string&>();
        types.push_back(LogicalType::flinkTypeToOmniTypeId(name));
    }
}

void DynamicKafkaDeserializationSchema::deserialize(RdKafka::Message* record, Collector* out)
{
    deserialize(std::vector<RdKafka::Message*>{record}, out);
}

void DynamicKafkaDeserializationSchema::deserialize(std::vector<RdKafka::Message*> records, Collector* out)
{
    records.erase(
        std::remove_if(
            records.begin(), records.end(), [](const RdKafka::Message* record) { return record->key() == nullptr; }),
        records.end());
    if (records.empty()) {
        return;
    }
    std::unique_ptr<omnistream::VectorBatch> batch(omnistream::VectorBatch::CreateVectorBatch(records.size(), types));
    std::vector<const nlohmann::json*> fields(types.size());
    for (size_t row = 0; row < records.size(); ++row) {
        const auto* record = records[row];
        auto key = nlohmann::json::parse(*record->key());
        bool tombstone = record->payload() == nullptr;
        nlohmann::json value;
        std::fill(fields.begin(), fields.end(), nullptr);
        project(key, keyProjection, keyNames, fields);
        if (!tombstone) {
            auto* bytes = static_cast<const char*>(record->payload());
            value = nlohmann::json::parse(std::string_view(bytes, record->len()));
            project(value, valueProjection, valueNames, fields);
        }
        for (size_t column = 0; column < types.size(); ++column) {
            rowDeserializer.setColValue(row, column, batch.get(), fields[column]);
        }
        batch->setTimestamp(row, record->timestamp().timestamp);
        batch->setRowKind(row, tombstone ? RowKind::DELETE : RowKind::INSERT);
    }
    out->collect(batch.release());
}
