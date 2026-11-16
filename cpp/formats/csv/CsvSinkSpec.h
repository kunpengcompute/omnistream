/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/ MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// CSV sink spec: the Java adapter serializes the consumed schema, the
// non-partition projection and the resolved CSV options into a "csvSpec" JSON
// object; the native side parses and validates it here before opening a file.

#ifndef OMNISTREAM_FORMATS_CSV_CSV_SINK_SPEC_H
#define OMNISTREAM_FORMATS_CSV_CSV_SINK_SPEC_H

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/include/common.h"     // THROW_RUNTIME_ERROR
#include "formats/csv/CsvEncoding.h" // CsvFormatOptions

namespace omnistream {
namespace csv {

// Stable logical-type enumeration carried in csvSpec.fields[].logicalType.
// Names mirror the Flink logical type identifiers used by the adapter.
enum class CsvLogicalType : std::uint8_t {
    UNKNOWN = 0,
    BIGINT,
    INTEGER,
    BOOLEAN,
    VARCHAR,   // CHAR / VARCHAR / STRING
    TIMESTAMP, // TIMESTAMP WITHOUT TIME ZONE, p <= 3
    DECIMAL,
    DOUBLE,        // not admitted; parsed for diagnostics
    TIMESTAMP_LTZ, // p <= 3
    DATE,          // not admitted; parsed for diagnostics
    TIME           // not admitted; parsed for diagnostics
};

inline CsvLogicalType parseLogicalType(const std::string& name)
{
    if (name == "BIGINT") return CsvLogicalType::BIGINT;
    if (name == "INTEGER" || name == "INT") return CsvLogicalType::INTEGER;
    if (name == "BOOLEAN") return CsvLogicalType::BOOLEAN;
    if (name == "VARCHAR" || name == "CHAR" || name == "STRING") return CsvLogicalType::VARCHAR;
    if (name == "TIMESTAMP") return CsvLogicalType::TIMESTAMP;
    if (name == "DECIMAL") return CsvLogicalType::DECIMAL;
    if (name == "DOUBLE") return CsvLogicalType::DOUBLE;
    if (name == "TIMESTAMP_LTZ") return CsvLogicalType::TIMESTAMP_LTZ;
    if (name == "DATE") return CsvLogicalType::DATE;
    if (name == "TIME") return CsvLogicalType::TIME;
    return CsvLogicalType::UNKNOWN;
}

struct CsvField {
    std::string name; // optional (CSV has no header); kept for diagnostics.
    CsvLogicalType logicalType = CsvLogicalType::UNKNOWN;
    int32_t precision = 0; // TIMESTAMP / DECIMAL precision.
    int32_t scale = 0;     // DECIMAL scale.
    bool nullable = true;
};

// Logical types admitted by the native CSV encoder; others are rejected at
// spec validation rather than silently degrading output.
inline bool isSupportedLogicalType(CsvLogicalType t)
{
    switch (t) {
        case CsvLogicalType::BIGINT:
        case CsvLogicalType::INTEGER:
        case CsvLogicalType::BOOLEAN:
        case CsvLogicalType::VARCHAR:
        case CsvLogicalType::TIMESTAMP:
        case CsvLogicalType::TIMESTAMP_LTZ:
        case CsvLogicalType::DECIMAL: return true;
        default: return false;
    }
}

// Parse a single-character CSV option: only a one-byte, non-NUL ASCII value can
// be represented faithfully by the native `char` option, so anything else is
// rejected instead of being silently truncated to its first byte.
inline char parseAsciiCsvCharacter(const nlohmann::json& value, const std::string& optionName)
{
    const std::string s = value.get<std::string>();
    if (s.size() != 1 || s[0] == '\0' || static_cast<unsigned char>(s[0]) > 0x7F) {
        THROW_RUNTIME_ERROR(
            "csvSpec " << optionName << " must be a single non-NUL ASCII character, got " << s.size() << " byte(s)");
    }
    return s[0];
}

struct CsvSinkSpec {
    std::string connector;
    std::string format;
    std::vector<CsvField> fields; // indexed by input column position.
    std::vector<int32_t> nonPartitionIndexes;
    CsvFormatOptions options;

    // Parse and validate the "csvSpec" JSON object. Throws on any mismatch so
    // that an unsupported configuration fails at writer creation, never partway
    // through a file.
    static CsvSinkSpec fromJson(const nlohmann::json& spec)
    {
        CsvSinkSpec s;
        if (!spec.is_object()) {
            THROW_RUNTIME_ERROR("csvSpec must be a JSON object");
        }
        s.connector = spec.value("connector", "");
        s.format = spec.value("format", "");
        if (s.connector != "filesystem") {
            THROW_RUNTIME_ERROR("csvSpec connector must be 'filesystem', got '" << s.connector << "'");
        }
        if (s.format != "csv") {
            THROW_RUNTIME_ERROR("csvSpec format must be 'csv', got '" << s.format << "'");
        }
        if (!spec.contains("fields") || !spec["fields"].is_array()) {
            THROW_RUNTIME_ERROR("csvSpec.fields must be a JSON array");
        }
        const auto& fieldsArr = spec["fields"];
        for (const auto& f : fieldsArr) {
            CsvField cf;
            cf.name = f.value("name", "");
            // The adapter serializes the name as "type" and nullability as
            // "isNull"; accept the csvSpec names too for forward compatibility.
            std::string lt = f.value("logicalType", "");
            if (lt.empty()) {
                lt = f.value("type", "");
            }
            cf.logicalType = parseLogicalType(lt);
            if (cf.logicalType == CsvLogicalType::UNKNOWN) {
                THROW_RUNTIME_ERROR("csvSpec field '" << cf.name << "' has unknown logicalType '" << lt << "'");
            }
            if (!isSupportedLogicalType(cf.logicalType)) {
                THROW_RUNTIME_ERROR(
                    "csvSpec field '" << cf.name << "' uses logical type not yet supported by native CSV");
            }
            if (f.contains("isNull")) {
                cf.nullable = f["isNull"].get<bool>();
            } else if (f.contains("nullable")) {
                cf.nullable = f["nullable"].get<bool>();
            }
            if (f.contains("precision")) {
                cf.precision = f["precision"].get<int32_t>();
            }
            if (f.contains("scale")) {
                cf.scale = f["scale"].get<int32_t>();
            }
            const bool isTimestamp =
                cf.logicalType == CsvLogicalType::TIMESTAMP || cf.logicalType == CsvLogicalType::TIMESTAMP_LTZ;
            if (isTimestamp && (cf.precision < 0 || cf.precision > 3)) {
                THROW_RUNTIME_ERROR("csvSpec TIMESTAMP/TIMESTAMP_LTZ precision must be in [0,3], got " << cf.precision);
            }
            if (cf.logicalType == CsvLogicalType::DECIMAL) {
                if (cf.precision < 1 || cf.precision > 38) {
                    THROW_RUNTIME_ERROR("csvSpec DECIMAL precision out of range: " << cf.precision);
                }
                if (cf.scale < 0 || cf.scale > cf.precision) {
                    THROW_RUNTIME_ERROR("csvSpec DECIMAL scale out of range: " << cf.scale);
                }
            }
            s.fields.push_back(std::move(cf));
        }

        if (!spec.contains("nonPartitionIndexes") || !spec["nonPartitionIndexes"].is_array()) {
            THROW_RUNTIME_ERROR("csvSpec.nonPartitionIndexes must be a JSON array");
        }
        s.nonPartitionIndexes = spec["nonPartitionIndexes"].get<std::vector<int32_t>>();
        std::set<int32_t> seen;
        for (int32_t idx : s.nonPartitionIndexes) {
            if (idx < 0 || idx >= static_cast<int32_t>(s.fields.size())) {
                THROW_RUNTIME_ERROR(
                    "csvSpec nonPartitionIndex " << idx << " out of range (fields=" << s.fields.size() << ")");
            }
            if (!seen.insert(idx).second) {
                THROW_RUNTIME_ERROR("csvSpec duplicate nonPartitionIndex " << idx);
            }
        }

        if (spec.contains("csvOptions") && spec["csvOptions"].is_object()) {
            const auto& o = spec["csvOptions"];
            if (o.contains("fieldDelimiter")) {
                s.options.fieldDelimiter = parseAsciiCsvCharacter(o["fieldDelimiter"], "fieldDelimiter");
            }
            if (o.contains("disableQuoteCharacter")) {
                s.options.disableQuoteCharacter = o["disableQuoteCharacter"].get<bool>();
            }
            if (o.contains("quoteCharacter")) {
                s.options.quoteCharacter = parseAsciiCsvCharacter(o["quoteCharacter"], "quoteCharacter");
            }
            if (o.contains("escapeCharacter") && !o["escapeCharacter"].is_null()) {
                s.options.hasEscapeCharacter = true;
                s.options.escapeCharacter = parseAsciiCsvCharacter(o["escapeCharacter"], "escapeCharacter");
            }
            if (o.contains("nullLiteral") && !o["nullLiteral"].is_null()) {
                s.options.hasNullLiteral = true;
                s.options.nullLiteral = o["nullLiteral"].get<std::string>();
            }
        }
        s.options.validate();
        return s;
    }
};

} // namespace csv
} // namespace omnistream

#endif // OMNISTREAM_FORMATS_CSV_CSV_SINK_SPEC_H
