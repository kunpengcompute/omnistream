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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/api/common/serialization/DeserializationSchema.h"
#include "core/api/common/serialization/JsonRowDataDeserializationSchema.h"
#include "table/data/RowKind.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/types/logical/LogicalType.h"

/**
 * Deserialization schema from Debezium JSON to native VectorBatch.
 *
 * This is the native counterpart of Flink's
 * org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema. It parses the
 * Debezium envelope ({@code before}/{@code after}/{@code op}), derives the RowKind and emits
 * the physical columns plus the requested format metadata columns, keeping the behaviour
 * identical to Flink 1.16.
 */
class DebeziumJsonDeserializationSchema : public DeserializationSchema {
public:
    explicit DebeziumJsonDeserializationSchema(const nlohmann::json& opDescriptionJSON);

    ~DebeziumJsonDeserializationSchema() override = default;

    void Open() override
    {
    }

    void deserialize(const uint8_t* message, size_t length, Collector* out) override;

    void deserialize(
        std::vector<const uint8_t*>& messageVec,
        std::vector<size_t>& lengthVec,
        std::vector<int64_t>& timeVec,
        Collector* out) override;

    bool isEndOfStream(const void* nextElement) override
    {
        return false;
    }

private:
    enum class MetaKind {
        SCHEMA,
        INGESTION_TIMESTAMP,
        SOURCE_TIMESTAMP,
        SOURCE_DATABASE,
        SOURCE_SCHEMA,
        SOURCE_TABLE
    };

    struct MetaColumn {
        MetaKind kind;
        int outputIndex;
    };

    struct EmittedRow {
        nlohmann::json* rowJson;
        RowKind rowKind;
    };

    void parseMetadata(const nlohmann::json& description);

    static bool resolveMetaKind(const std::string& key, MetaKind& kind);

    void processMessage(const uint8_t* message, size_t length, bool hasTimestamp, int64_t timestamp, Collector* out);

    void collectRows(
        const nlohmann::json& root,
        const nlohmann::json& payload,
        std::vector<EmittedRow>& rows,
        bool hasTimestamp,
        int64_t timestamp,
        Collector* out);

    void validateRow(nlohmann::json& rowJson);

    void fillMetadata(
        const nlohmann::json& schemaRoot,
        const nlohmann::json& payload,
        omnistream::VectorBatch* batch,
        int rowIndex);

    static void setStringValue(
        omnistream::VectorBatch* batch, int colIndex, int rowIndex, const std::string& value);

    static void setNull(omnistream::VectorBatch* batch, int colIndex, int rowIndex);

    static bool readLongField(const nlohmann::json& object, const char* key, int64_t& value);

    static bool readStringField(const nlohmann::json& object, const char* key, std::string& value);

    JsonRowDataDeserializationSchema rowDeserializer;
    std::vector<std::string> outputNames;
    std::vector<omniruntime::type::DataTypeId> fieldTypes;
    std::vector<MetaColumn> metadataColumns;
    int physicalColumnCount = 0;
    bool schemaInclude = false;
    bool ignoreParseErrors = false;
};
