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

#include "JsonRowDataDeserializationSchema.h"

#include <charconv>
#include <cmath>
#include <limits>
#include <string_view>
#include <utility>

#include "table/data/TimestampData.h"

namespace {
template <typename Integer>
Integer parseIntegerText(std::string_view text)
{
    while (!text.empty() && static_cast<unsigned char>(text.front()) <= ' ') {
        text.remove_prefix(1);
    }
    while (!text.empty() && static_cast<unsigned char>(text.back()) <= ' ') {
        text.remove_suffix(1);
    }
    if (!text.empty() && text.front() == '+') {
        text.remove_prefix(1);
        if (!text.empty() && text.front() == '-') {
            throw std::runtime_error("Invalid integer sign");
        }
    }
    if (text.empty()) {
        throw std::runtime_error("Empty integer text");
    }
    Integer result{};
    const auto end = text.data() + text.size();
    const auto parsed = std::from_chars(text.data(), end, result);
    if (parsed.ec != std::errc{} || parsed.ptr != end) {
        throw std::runtime_error("Invalid or out-of-range integer text");
    }
    return result;
}

template <typename Integer>
Integer readInteger(const nlohmann::json& value)
{
    constexpr auto min = std::numeric_limits<Integer>::min();
    constexpr auto max = std::numeric_limits<Integer>::max();
    if (value.is_number_unsigned()) {
        const auto number = value.get<uint64_t>();
        if (number <= static_cast<uint64_t>(max)) {
            return static_cast<Integer>(number);
        }
    } else if (value.is_number_integer()) {
        const auto number = value.get<int64_t>();
        if (number >= min && number <= max) {
            return static_cast<Integer>(number);
        }
    } else if (value.is_number_float()) {
        const auto number = value.get<double>();
        const auto upper = static_cast<double>(max);
        if (std::isfinite(number) && number >= static_cast<double>(min) && number <= upper) {
            // Java's double-to-long conversion saturates at Long.MAX_VALUE.
            return number == upper ? max : static_cast<Integer>(number);
        }
    } else if (value.is_string()) {
        return parseIntegerText<Integer>(value.get_ref<const std::string&>());
    }
    throw std::runtime_error("Expected an integer in range or numeric text");
}
} // namespace

void JsonRowDataDeserializationSchema::deserialize(const uint8_t* message, size_t length, Collector* out)
{
    auto batch = deserializeBatch({message}, {length});
    if (batch) {
        out->collect(batch.release());
    }
}

void* JsonRowDataDeserializationSchema::deserialize(
    std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec)
{
    return deserializeBatch(messageVec, lengthVec).release();
}

void JsonRowDataDeserializationSchema::deserialize(
    std::vector<const uint8_t*>& messageVec,
    std::vector<size_t>& lengthVec,
    std::vector<int64_t>& timeVec,
    Collector* out)
{
    auto batch = deserializeBatch(messageVec, lengthVec, timeVec);
    if (batch) {
        out->collect(batch.release());
    }
}

std::unique_ptr<omnistream::VectorBatch> JsonRowDataDeserializationSchema::deserializeBatch(
    const std::vector<const uint8_t*>& messages,
    const std::vector<size_t>& lengths,
    const std::vector<int64_t>& timestamps)
{
    // Keep the original message index so skipped payloads do not shift Kafka timestamps.
    std::vector<std::pair<size_t, nlohmann::json>> records;
    records.reserve(messages.size());
    for (size_t index = 0; index < messages.size(); ++index) {
        if (messages[index] == nullptr) {
            continue;
        }
        auto record = nlohmann::json::parse(
            std::string_view(reinterpret_cast<const char*>(messages[index]), lengths[index]),
            nullptr,
            !ignoreParseErrors);
        if (record.is_null()) {
            continue;
        }
        if (!record.is_object()) {
            if (ignoreParseErrors) {
                continue;
            }
            throw std::runtime_error("Kafka JSON payload must be an object");
        }
        records.emplace_back(index, std::move(record));
    }
    if (records.empty()) {
        return nullptr;
    }

    std::unique_ptr<omnistream::VectorBatch> batch(createBatch(static_cast<int>(records.size()), fieldTypes));
    const bool hasTimestamps = timestamps.size() == messages.size();
    for (size_t row = 0; row < records.size(); ++row) {
        const auto& [index, record] = records[row];
        if (hasTimestamps) {
            batch->setTimestamp(row, timestamps[index]);
        }
        for (size_t column = 0; column < fieldNames.size(); ++column) {
            const auto field = record.find(fieldNames[column]);
            if (field == record.end() && failOnMissingField) {
                throw std::runtime_error("Missing JSON field '" + fieldNames[column] + "'");
            }
            if (field == record.end() || field->is_null()) {
                batch->Get(column)->SetNull(row);
                continue;
            }
            try {
                setColValue(static_cast<int>(row), static_cast<int>(column), *batch, *field);
            } catch (const nlohmann::json::exception& error) {
                handleFieldError(row, column, *batch, error);
            } catch (const std::invalid_argument& error) {
                handleFieldError(row, column, *batch, error);
            } catch (const std::runtime_error& error) {
                handleFieldError(row, column, *batch, error);
            }
        }
    }
    return batch;
}

void JsonRowDataDeserializationSchema::handleFieldError(
    int row, int column, omnistream::VectorBatch& batch, const std::exception& error)
{
    if (!ignoreParseErrors) {
        throw std::runtime_error("Failed to deserialize JSON field '" + fieldNames[column] + "': " + error.what());
    }
    batch.Get(column)->SetNull(row);
}

void JsonRowDataDeserializationSchema::setColValue(
    int row, int column, omnistream::VectorBatch& batch, const nlohmann::json& value)
{
    using omniruntime::type::DataTypeId;
    switch (fieldTypes[column]) {
        case DataTypeId::OMNI_BOOLEAN: batch.SetValueAt(column, row, value.get<bool>()); break;
        case DataTypeId::OMNI_INT: batch.SetValueAt(column, row, readInteger<int32_t>(value)); break;
        case DataTypeId::OMNI_LONG: batch.SetValueAt(column, row, readInteger<int64_t>(value)); break;
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            batch.SetValueAt(column, row, TimestampData::stringToMillisOfDay(value.get<std::string>()));
            break;
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP:
            batch.SetValueAt(column, row, TimestampData::stringToEpochMillis(value.get<std::string>()));
            break;
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            batch.SetValueAt(
                column, row, TimestampData::fromLocalTimeString(value.get<std::string>()).getMillisecond());
            break;
        case DataTypeId::OMNI_CHAR:
        case DataTypeId::OMNI_VARCHAR: {
            auto* vector =
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    batch.Get(column));
            const std::string text = value.is_string() ? value.get<std::string>() : value.dump();
            std::string_view view(text);
            vector->SetValue(row, view);
            break;
        }
        default: throw std::logic_error("Unsupported JSON field type");
    }
}
