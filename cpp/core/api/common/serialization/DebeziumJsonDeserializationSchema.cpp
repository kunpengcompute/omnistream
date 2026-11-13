/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "DebeziumJsonDeserializationSchema.h"

#include <memory>
#include <stdexcept>
#include <string_view>

namespace {

constexpr const char* OP_READ = "r";   // snapshot read
constexpr const char* OP_CREATE = "c"; // insert
constexpr const char* OP_UPDATE = "u"; // update
constexpr const char* OP_DELETE = "d"; // delete

constexpr const char* REPLICA_IDENTITY_EXCEPTION =
    "The \"before\" field of %s message is null, "
    "if you are using Debezium Postgres Connector, "
    "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

std::string replicaIdentityError(const std::string& op)
{
    std::string message = REPLICA_IDENTITY_EXCEPTION;
    const std::string placeholder = "%s";
    const auto pos = message.find(placeholder);
    if (pos != std::string::npos) {
        message.replace(pos, placeholder.size(), op);
    }
    return message;
}

bool isStringType(omniruntime::type::DataTypeId type)
{
    return type == omniruntime::type::DataTypeId::OMNI_CHAR ||
           type == omniruntime::type::DataTypeId::OMNI_VARCHAR;
}

bool isTimestampLtzType(omniruntime::type::DataTypeId type)
{
    return type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
}

} // namespace

DebeziumJsonDeserializationSchema::DebeziumJsonDeserializationSchema(const nlohmann::json& opDescriptionJSON)
    : rowDeserializer(opDescriptionJSON, true)
{
    outputNames = opDescriptionJSON.at("outputNames").get<std::vector<std::string>>();
    auto outputTypes = opDescriptionJSON.at("outputTypes").get<std::vector<std::string>>();
    for (const auto& type : outputTypes) {
        fieldTypes.push_back(LogicalType::flinkTypeToOmniTypeId(type));
    }
    schemaInclude = opDescriptionJSON.value("debeziumSchemaInclude", false);
    ignoreParseErrors = opDescriptionJSON.value("debeziumIgnoreParseErrors", false);
    parseMetadata(opDescriptionJSON);
}

bool DebeziumJsonDeserializationSchema::resolveMetaKind(const std::string& key, MetaKind& kind)
{
    if (key == "schema") {
        kind = MetaKind::SCHEMA;
    } else if (key == "ingestion-timestamp") {
        kind = MetaKind::INGESTION_TIMESTAMP;
    } else if (key == "source.timestamp") {
        kind = MetaKind::SOURCE_TIMESTAMP;
    } else if (key == "source.database") {
        kind = MetaKind::SOURCE_DATABASE;
    } else if (key == "source.schema") {
        kind = MetaKind::SOURCE_SCHEMA;
    } else if (key == "source.table") {
        kind = MetaKind::SOURCE_TABLE;
    } else {
        return false;
    }
    return true;
}

void DebeziumJsonDeserializationSchema::parseMetadata(const nlohmann::json& description)
{
    metadataColumns.clear();
    int metadataCount = 0;
    if (description.contains("debeziumMetadata") && description.at("debeziumMetadata").is_array()) {
        metadataCount = static_cast<int>(description.at("debeziumMetadata").size());
        int outputIndex = static_cast<int>(outputNames.size()) - metadataCount;
        for (const auto& item : description.at("debeziumMetadata")) {
            const std::string key = item.get<std::string>();
            MetaKind kind;
            if (!resolveMetaKind(key, kind)) {
                // Unsupported metadata must never reach the native runtime: the planner is expected
                // to fall back to Flink. Fail fast instead of silently misinterpreting a metadata
                // column as a physical one.
                throw std::runtime_error("Unsupported Debezium metadata key: " + key);
            }
            if (outputIndex >= 0 && outputIndex < static_cast<int>(fieldTypes.size())) {
                metadataColumns.push_back({kind, outputIndex});
            }
            ++outputIndex;
        }
    }
    physicalColumnCount = static_cast<int>(outputNames.size()) - metadataCount;
    if (physicalColumnCount < 0) {
        physicalColumnCount = 0;
    }
}

bool DebeziumJsonDeserializationSchema::readLongField(
    const nlohmann::json& object, const char* key, int64_t& value)
{
    if (!object.is_object()) {
        return false;
    }
    auto it = object.find(key);
    if (it == object.end() || it->is_null()) {
        return false;
    }
    if (it->is_number_integer() || it->is_number_unsigned()) {
        value = it->get<int64_t>();
        return true;
    }
    if (it->is_string()) {
        // Mirrors Flink's Long.parseLong(jsonNode.asText().trim()).
        std::string text = it->get<std::string>();
        try {
            size_t parsed = 0;
            long long parsedValue = std::stoll(text, &parsed);
            if (parsed == text.size()) {
                value = static_cast<int64_t>(parsedValue);
                return true;
            }
        } catch (const std::exception&) {
            return false;
        }
    }
    return false;
}

bool DebeziumJsonDeserializationSchema::readStringField(
    const nlohmann::json& object, const char* key, std::string& value)
{
    if (!object.is_object()) {
        return false;
    }
    auto it = object.find(key);
    if (it == object.end() || it->is_null()) {
        return false;
    }
    if (it->is_string()) {
        value = it->get<std::string>();
        return true;
    }
    if (it->is_number() || it->is_boolean()) {
        // Mirrors Flink's convertToString which uses jsonNode.asText() for scalars.
        value = it->dump();
        return true;
    }
    return false;
}

void DebeziumJsonDeserializationSchema::setStringValue(
    omnistream::VectorBatch* batch, int colIndex, int rowIndex, const std::string& value)
{
    auto stringVec = reinterpret_cast<
        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(colIndex));
    std::string_view strView(value.data(), value.size());
    stringVec->SetValue(rowIndex, strView);
}

void DebeziumJsonDeserializationSchema::setNull(
    omnistream::VectorBatch* batch, int colIndex, int rowIndex)
{
    batch->Get(colIndex)->SetNull(rowIndex);
}

void DebeziumJsonDeserializationSchema::fillMetadata(
    const nlohmann::json& schemaRoot,
    const nlohmann::json& payload,
    omnistream::VectorBatch* batch,
    int rowIndex)
{
    for (const auto& meta : metadataColumns) {
        const auto type = fieldTypes[meta.outputIndex];
        switch (meta.kind) {
            case MetaKind::SCHEMA: {
                auto it = schemaRoot.find("schema");
                if (it == schemaRoot.end() || it->is_null()) {
                    setNull(batch, meta.outputIndex, rowIndex);
                } else if (it->is_string()) {
                    setStringValue(batch, meta.outputIndex, rowIndex, it->get<std::string>());
                } else {
                    setStringValue(batch, meta.outputIndex, rowIndex, it->dump());
                }
                break;
            }
            case MetaKind::INGESTION_TIMESTAMP: {
                int64_t timestamp = 0;
                if (isTimestampLtzType(type) && readLongField(payload, "ts_ms", timestamp)) {
                    batch->SetValueAt<int64_t>(meta.outputIndex, rowIndex, timestamp);
                } else {
                    setNull(batch, meta.outputIndex, rowIndex);
                }
                break;
            }
            case MetaKind::SOURCE_TIMESTAMP: {
                int64_t timestamp = 0;
                auto sourceIt = payload.find("source");
                if (isTimestampLtzType(type) && sourceIt != payload.end() && !sourceIt->is_null() &&
                    readLongField(*sourceIt, "ts_ms", timestamp)) {
                    batch->SetValueAt<int64_t>(meta.outputIndex, rowIndex, timestamp);
                } else {
                    setNull(batch, meta.outputIndex, rowIndex);
                }
                break;
            }
            case MetaKind::SOURCE_DATABASE:
            case MetaKind::SOURCE_SCHEMA:
            case MetaKind::SOURCE_TABLE: {
                const char* property = "db";
                if (meta.kind == MetaKind::SOURCE_SCHEMA) {
                    property = "schema";
                } else if (meta.kind == MetaKind::SOURCE_TABLE) {
                    property = "table";
                }
                std::string value;
                auto sourceIt = payload.find("source");
                if (isStringType(type) && sourceIt != payload.end() && !sourceIt->is_null() &&
                    readStringField(*sourceIt, property, value)) {
                    setStringValue(batch, meta.outputIndex, rowIndex, value);
                } else {
                    setNull(batch, meta.outputIndex, rowIndex);
                }
                break;
            }
            default: break;
        }
    }
}

void DebeziumJsonDeserializationSchema::collectRows(
    const nlohmann::json& root,
    const nlohmann::json& payload,
    std::vector<EmittedRow>& rows,
    bool hasTimestamp,
    int64_t timestamp,
    Collector* out)
{
    if (rows.empty()) {
        return;
    }
    auto* batch = omnistream::VectorBatch::CreateVectorBatch(static_cast<int>(rows.size()), fieldTypes);
    for (size_t i = 0; i < rows.size(); ++i) {
        const EmittedRow& emitted = rows[i];
        const int rowIndex = static_cast<int>(i);
        for (int col = 0; col < physicalColumnCount; ++col) {
            // Flink sets the offending field to null and keeps the row when ignore-parse-errors
            // is enabled; only failures while building the envelope drop the whole message.
            try {
                rowDeserializer.setColValue(rowIndex, col, batch, *emitted.rowJson);
            } catch (const std::exception&) {
                if (!ignoreParseErrors) {
                    throw;
                }
                setNull(batch, col, rowIndex);
            }
        }
        fillMetadata(root, payload, batch, rowIndex);
        batch->setRowKind(i, emitted.rowKind);
        if (hasTimestamp) {
            batch->setTimestamp(i, timestamp);
        }
    }
    out->collect(batch);
}

void DebeziumJsonDeserializationSchema::validateRow(nlohmann::json& rowJson)
{
    if (physicalColumnCount <= 0) {
        return;
    }
    std::vector<omniruntime::type::DataTypeId> physicalTypes(
        fieldTypes.begin(), fieldTypes.begin() + physicalColumnCount);
    std::unique_ptr<omnistream::VectorBatch> batch(
        omnistream::VectorBatch::CreateVectorBatch(1, physicalTypes));
    for (int col = 0; col < physicalColumnCount; ++col) {
        rowDeserializer.setColValue(0, col, batch.get(), rowJson);
    }
}

void DebeziumJsonDeserializationSchema::processMessage(
    const uint8_t* message, size_t length, bool hasTimestamp, int64_t timestamp, Collector* out)
{
    if (message == nullptr || length == 0) {
        // skip tombstone messages
        return;
    }
    std::string_view raw(reinterpret_cast<const char*>(message), length);
    try {
        nlohmann::json root = nlohmann::json::parse(raw);
        const nlohmann::json* schemaRoot = &root;
        nlohmann::json* payload = &root;
        if (schemaInclude) {
            auto payloadIt = root.find("payload");
            if (payloadIt == root.end() || payloadIt->is_null()) {
                throw std::runtime_error("the \"payload\" field is missing");
            }
            payload = &*payloadIt;
        }

        auto opIt = payload->find("op");
        if (opIt == payload->end() || opIt->is_null()) {
            throw std::runtime_error("the \"op\" field is missing");
        }
        const std::string op = opIt->is_string() ? opIt->get<std::string>() : opIt->dump();

        nlohmann::json* before = nullptr;
        nlohmann::json* after = nullptr;
        auto extractRow = [&](const char* name, nlohmann::json*& target) {
            auto it = payload->find(name);
            if (it == payload->end() || it->is_null()) {
                return;
            }
            if (!it->is_object()) {
                // Flink's row converter only accepts JSON objects. With ignore-parse-errors the
                // malformed side degrades to null, otherwise the whole message fails.
                if (!ignoreParseErrors) {
                    throw std::runtime_error(
                        std::string("the \"") + name + "\" field is not a JSON object");
                }
                return;
            }
            target = &*it;
        };
        extractRow("before", before);
        extractRow("after", after);

        std::vector<EmittedRow> rows;
        if (op == OP_CREATE || op == OP_READ) {
            if (after == nullptr) {
                throw std::runtime_error("the \"after\" field is null for op \"" + op + "\"");
            }
            rows.push_back({after, RowKind::INSERT});
        } else if (op == OP_UPDATE) {
            if (before == nullptr) {
                throw std::runtime_error(replicaIdentityError("UPDATE"));
            }
            if (after == nullptr) {
                throw std::runtime_error("the \"after\" field is null for op \"u\"");
            }
            rows.push_back({before, RowKind::UPDATE_BEFORE});
            rows.push_back({after, RowKind::UPDATE_AFTER});
        } else if (op == OP_DELETE) {
            if (before == nullptr) {
                throw std::runtime_error(replicaIdentityError("DELETE"));
            }
            rows.push_back({before, RowKind::DELETE});
        } else {
            throw std::runtime_error(
                "unknown \"op\" value \"" + op + "\". The Debezium JSON message is '" + std::string(raw) + "'");
        }

        if (!ignoreParseErrors) {
            // Flink converts both the before and after rows before applying the op. Validate the
            // side that is not emitted so that malformed unused values fail the message as well.
            if (before != nullptr && !(op == OP_UPDATE || op == OP_DELETE)) {
                validateRow(*before);
            }
            if (after != nullptr && !(op == OP_CREATE || op == OP_READ || op == OP_UPDATE)) {
                validateRow(*after);
            }
        }

        collectRows(*schemaRoot, *payload, rows, hasTimestamp, timestamp, out);
    } catch (const std::exception& e) {
        if (!ignoreParseErrors) {
            throw std::runtime_error(
                "Corrupt Debezium JSON message '" + std::string(raw) + "'. " + std::string(e.what()));
        }
    }
}

void DebeziumJsonDeserializationSchema::deserialize(
    const uint8_t* message, size_t length, Collector* out)
{
    processMessage(message, length, false, 0, out);
}

void DebeziumJsonDeserializationSchema::deserialize(
    std::vector<const uint8_t*>& messageVec,
    std::vector<size_t>& lengthVec,
    std::vector<int64_t>& timeVec,
    Collector* out)
{
    const size_t count = messageVec.size();
    const bool hasTimestamps = timeVec.size() == count;
    for (size_t i = 0; i < count; ++i) {
        int64_t timestamp = 0;
        if (hasTimestamps) {
            timestamp = timeVec[i];
        }
        processMessage(messageVec[i], lengthVec[i], hasTimestamps, timestamp, out);
    }
}
