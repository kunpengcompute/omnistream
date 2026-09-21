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

// CSV row encoder for the native filesystem sink. One encoder is built per
// writer from a CsvSinkSpec; encode() produces the field content of one CSV
// record (without the terminator, which the FileWriter appends).

#ifndef OMNISTREAM_FORMATS_CSV_CSV_ROW_ENCODER_H
#define OMNISTREAM_FORMATS_CSV_CSV_ROW_ENCODER_H

#include <cstdint>
#include <string>

#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/type/decimal128.h"
#include "formats/csv/CsvEncoding.h"
#include "formats/csv/CsvSinkSpec.h"

namespace omnistream {
namespace csv {

class CsvRowEncoder {
public:
    explicit CsvRowEncoder(CsvSinkSpec spec) : spec_(std::move(spec))
    {
    }

    // Encode one row of `batch` as CSV field content (no record terminator).
    // Validates column/type consistency and that the row is an INSERT.
    std::string encode(omnistream::VectorBatch* batch, int32_t rowId) const
    {
        if (batch == nullptr) {
            THROW_RUNTIME_ERROR("CsvRowEncoder: batch is null");
        }
        if (rowId < 0 || rowId >= batch->GetRowCount()) {
            THROW_RUNTIME_ERROR("CsvRowEncoder: rowId " << rowId << " out of range [0," << batch->GetRowCount() << ")");
        }
        if (batch->getRowKind(static_cast<size_t>(rowId)) != RowKind::INSERT) {
            THROW_RUNTIME_ERROR("CsvRowEncoder: only INSERT rows are supported by the CSV sink");
        }

        const auto& indexes = spec_.nonPartitionIndexes;
        const auto& fields = spec_.fields;
        std::string out;
        for (size_t p = 0; p < indexes.size(); ++p) {
            if (p > 0) {
                out.push_back(spec_.options.fieldDelimiter);
            }
            int32_t colId = indexes[p];
            if (colId >= batch->GetVectorCount()) {
                THROW_RUNTIME_ERROR(
                    "CsvRowEncoder: column " << colId << " >= vector count " << batch->GetVectorCount());
            }
            if (static_cast<size_t>(colId) >= fields.size()) {
                THROW_RUNTIME_ERROR("CsvRowEncoder: column " << colId << " has no spec field");
            }
            const CsvField& field = fields[static_cast<size_t>(colId)];
            omniruntime::vec::BaseVector* vec = batch->Get(colId);

            if (vec->IsNull(rowId)) {
                appendNullField(out, spec_.options);
                continue;
            }
            appendValue(out, vec, rowId, field);
        }
        return out;
    }

private:
    CsvSinkSpec spec_;

    static std::string readStringView(omniruntime::vec::BaseVector* vec, int32_t row)
    {
        if (vec->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
            auto* casted =
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    vec);
            return std::string(casted->GetValue(row));
        }
        if (vec->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            auto* casted = reinterpret_cast<omniruntime::vec::Vector<
                omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(vec);
            return std::string(casted->GetValue(row));
        }
        THROW_RUNTIME_ERROR("CsvRowEncoder: unsupported string encoding for CSV output");
    }

    // Format an unscaled DECIMAL value (sign handled separately) with the JDK
    // BigDecimal.toString() rules from CsvEncoding.h.
    static std::string formatUnscaledDecimal(std::string digits, int32_t scale)
    {
        std::string sign;
        if (!digits.empty() && digits[0] == '-') {
            sign = "-";
            digits.erase(0, 1);
        }
        return sign + formatDecimal(std::move(digits), scale);
    }

    void appendValue(std::string& out, omniruntime::vec::BaseVector* vec, int32_t row, const CsvField& field) const
    {
        using omniruntime::type::DataTypeId;
        DataTypeId phys = vec->GetTypeId();

        switch (field.logicalType) {
            case CsvLogicalType::BIGINT: {
                if (phys != DataTypeId::OMNI_LONG) {
                    THROW_RUNTIME_ERROR("CsvRowEncoder BIGINT expects OMNI_LONG, got " << static_cast<int>(phys));
                }
                int64_t v = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vec)->GetValue(row);
                appendRawField(out, std::to_string(v));
                return;
            }
            case CsvLogicalType::INTEGER: {
                if (phys != DataTypeId::OMNI_INT) {
                    THROW_RUNTIME_ERROR("CsvRowEncoder INTEGER expects OMNI_INT, got " << static_cast<int>(phys));
                }
                int32_t v = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(vec)->GetValue(row);
                appendRawField(out, std::to_string(v));
                return;
            }
            case CsvLogicalType::BOOLEAN: {
                if (phys != DataTypeId::OMNI_BOOLEAN) {
                    THROW_RUNTIME_ERROR("CsvRowEncoder BOOLEAN expects OMNI_BOOLEAN, got " << static_cast<int>(phys));
                }
                bool v = reinterpret_cast<omniruntime::vec::Vector<bool>*>(vec)->GetValue(row);
                appendRawField(out, v ? "true" : "false");
                return;
            }
            case CsvLogicalType::VARCHAR: {
                if (phys != DataTypeId::OMNI_CHAR && phys != DataTypeId::OMNI_VARCHAR) {
                    THROW_RUNTIME_ERROR(
                        "CsvRowEncoder VARCHAR expects OMNI_CHAR/VARCHAR, got " << static_cast<int>(phys));
                }
                appendQuotedField(out, readStringView(vec, row), spec_.options);
                return;
            }
            case CsvLogicalType::TIMESTAMP: {
                // A TIMESTAMP column may reach the sink either as epoch millis in an OMNI_LONG
                // vector (calc/source chains) or as a timestamp vector produced by the native
                // streaming join (OMNI_TIMESTAMP / OMNI_TIMESTAMP_WITHOUT_TIME_ZONE). All of them
                // store epoch millis in a Vector<int64_t>, exactly like WriteToFileInternal does.
                if (phys != DataTypeId::OMNI_LONG && phys != DataTypeId::OMNI_TIMESTAMP &&
                    phys != DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE &&
                    phys != DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE) {
                    THROW_RUNTIME_ERROR(
                        "CsvRowEncoder TIMESTAMP expects OMNI_LONG/OMNI_TIMESTAMP/"
                        "OMNI_TIMESTAMP_WITHOUT_TIME_ZONE, got "
                        << static_cast<int>(phys));
                }
                int64_t millis = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vec)->GetValue(row);
                appendQuotedField(out, formatTimestamp(millis, field.precision), spec_.options);
                return;
            }
            case CsvLogicalType::TIMESTAMP_LTZ: {
                if (phys != DataTypeId::OMNI_LONG && phys != DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                    THROW_RUNTIME_ERROR(
                        "CsvRowEncoder TIMESTAMP_LTZ expects "
                        "OMNI_LONG/OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE, got "
                        << static_cast<int>(phys));
                }
                int64_t millis = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vec)->GetValue(row);
                appendQuotedField(out, formatTimestampLtz(millis, field.precision), spec_.options);
                return;
            }
            case CsvLogicalType::DECIMAL: {
                if (phys == DataTypeId::OMNI_DECIMAL64) {
                    int64_t unscaled = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vec)->GetValue(row);
                    appendQuotedField(out, formatUnscaledDecimal(std::to_string(unscaled), field.scale), spec_.options);
                } else if (phys == DataTypeId::OMNI_DECIMAL128) {
                    omniruntime::type::Decimal128 d =
                        reinterpret_cast<omniruntime::vec::Vector<omniruntime::type::Decimal128>*>(vec)->GetValue(row);
                    appendQuotedField(out, formatUnscaledDecimal(d.ToString(), field.scale), spec_.options);
                } else {
                    THROW_RUNTIME_ERROR(
                        "CsvRowEncoder DECIMAL expects OMNI_DECIMAL64/128, got " << static_cast<int>(phys));
                }
                return;
            }
            default: THROW_RUNTIME_ERROR("CsvRowEncoder: logical type not supported at encode time");
        }
    }
};

} // namespace csv
} // namespace omnistream

#endif // OMNISTREAM_FORMATS_CSV_CSV_ROW_ENCODER_H
