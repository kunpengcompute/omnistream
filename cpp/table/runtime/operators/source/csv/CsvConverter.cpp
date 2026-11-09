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
#include "CsvConverter.h"
#include <cstdint>
#include <string>
#include <stdexcept>
#include <string_view>
#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"

using namespace omniruntime::type;

namespace omnistream {
namespace csv {

namespace {
bool isDefaultNullLiteral(const std::string& value)
{
    return value == "null";
}

int32_t parseDateToEpochDays(const std::string& value)
{
    auto result = omniruntime::type::util::fromDateString(std::string_view(value.data(), value.size()));
    if (result.hasError()) {
        throw std::invalid_argument("invalid date literal: " + value);
    }
    return result.value();
}

CsvNode* getProjectedNodeOrNull(const CsvRow& csvRow, int csvFieldIndex)
{
    const auto& nodes = csvRow.getNodes();
    if (csvFieldIndex < 0 || static_cast<size_t>(csvFieldIndex) >= nodes.size()) {
        return nullptr;
    }

    return nodes[csvFieldIndex].get();
}

bool isCsvNullValue(const std::string& value, const CsvSchema& schema)
{
    if (schema.hasNullValue()) {
        return value == schema.getNullValue();
    }

    return isDefaultNullLiteral(value);
}

// Aligns with Flink DecimalData.fromBigDecimal: significant digits of the unscaled
// integer must be <= precision (0 has precision 1). Throws out_of_range on overflow.
void EnsureDecimalPrecision(const std::string& unscaledAbs, int32_t precision)
{
    if (unscaledAbs.empty()) {
        throw std::invalid_argument("empty unscaled decimal");
    }
    if (precision <= 0) {
        return;
    }
    size_t start = 0;
    while (start + 1 < unscaledAbs.size() && unscaledAbs[start] == '0') {
        ++start;
    }
    if (static_cast<int32_t>(unscaledAbs.size() - start) > precision) {
        throw std::out_of_range("decimal precision overflow");
    }
}

std::string BuildUnscaledAbs(const std::string& value, int32_t scale, bool& negative)
{
    std::string valueStr = value;
    negative = false;
    if (!valueStr.empty() && valueStr[0] == '-') {
        negative = true;
        valueStr = valueStr.substr(1);
    }
    std::string intPart;
    std::string fracPart;
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
    std::string unscaledAbs = intPart + fracPart;
    if (unscaledAbs.empty()) {
        throw std::invalid_argument("invalid decimal literal: " + value);
    }
    return unscaledAbs;
}

long parseDecimal64ToUnscaledLong(const std::string& value, int32_t precision, int32_t scale)
{
    bool negative = false;
    std::string unscaledAbs = BuildUnscaledAbs(value, scale, negative);
    EnsureDecimalPrecision(unscaledAbs, precision);
    long unscaledValue = std::stol(unscaledAbs);
    if (negative) {
        unscaledValue = -unscaledValue;
    }
    return unscaledValue;
}

Decimal128 parseDecimal128ToValue(const std::string& value, int32_t precision, int32_t scale)
{
    bool negative = false;
    std::string unscaledAbs = BuildUnscaledAbs(value, scale, negative);
    EnsureDecimalPrecision(unscaledAbs, precision);
    if (negative) {
        unscaledAbs = "-" + unscaledAbs;
    }
    return Decimal128(std::string_view(unscaledAbs));
}

std::int64_t parseCsvBigInt(std::string_view value)
{
    // Flink trims characters up to and including U+0020 before parsing.
    while (!value.empty() && static_cast<unsigned char>(value.front()) <= ' ') {
        value.remove_prefix(1);
    }
    while (!value.empty() && static_cast<unsigned char>(value.back()) <= ' ') {
        value.remove_suffix(1);
    }

    std::size_t parsedLength = 0;
    const auto result = std::stoll(std::string(value), &parsedLength, 10);
    if (parsedLength != value.size()) {
        throw std::invalid_argument("Invalid CSV BIGINT value");
    }
    return result;
}

} // namespace

BinaryRowData* CsvConverter::convert(const CsvRow& csvRow)
{
    // Original implementation uses `GenericRowData`
    BinaryRowData* rowData = BinaryRowData::createBinaryRowDataWithMem(csvRow.getNodes().size());
    const CsvSchema schema = csvRow.getSchema();
    for (size_t i = 0; i < csvRow.getNodes().size(); i++) {
        CsvNode* node = csvRow.getNodes()[i].get();
        omniruntime::type::DataTypeId type = node->getType();
        std::string value = node->getValue();
        if (isCsvNullValue(value, schema)) {
            LOG("CsvConverter: Detected null value for column " << i << ", setting it as null in BinaryRowData.");
            rowData->setNullAt(i);
            continue;
        }

        if (type == omniruntime::type::DataTypeId::OMNI_INT) {
            LOG("CsvConverter: Converting value '" << value << "' to int for column " << i);
            try {
                rowData->setInt(i, std::stoi(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid integer value '" << value << "' for column " << i
                                                            << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Integer value '" << value << "' out of range for column " << i
                                                    << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_BOOLEAN) {
            LOG("CsvConverter: Converting value '" << value << "' to boolean for column " << i);
            try {
                std::string trimmed = value;
                trimmed.erase(trimmed.begin(), std::find_if(trimmed.begin(), trimmed.end(), [](unsigned char ch) {
                                  return !::isspace(ch);
                              }));
                trimmed.erase(
                    std::find_if(trimmed.rbegin(), trimmed.rend(), [](unsigned char ch) { return !::isspace(ch); })
                        .base(),
                    trimmed.end());
                std::transform(trimmed.begin(), trimmed.end(), trimmed.begin(), [](unsigned char ch) {
                    return static_cast<char>(::tolower(ch));
                });
                if (trimmed == "true") {
                    rowData->setBool(i, true);
                } else {
                    rowData->setBool(i, false); // 不能识别的也默认false，不报错
                }
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid boolean value '" << value << "' for column " << i
                                                            << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_LONG) {
            LOG("CsvConverter: Converting value '" << value << "' to long for column " << i);
            try {
                rowData->setLong(i, parseCsvBigInt(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid long value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Long value '" << value << "' out of range for column " << i
                                                 << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DECIMAL64) {
            LOG("CsvConverter: Converting value '" << value << "' to decimal64 for column " << i);
            try {
                int32_t precision = schema.getPrecisionAtIdx(i);
                int32_t scale = schema.getScaleAtIdx(i);
                rowData->setLong(i, parseDecimal64ToUnscaledLong(value, precision, scale));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid decimal64 value '" << value << "' for column " << i
                                                              << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Decimal64 value '" << value << "' out of range for column " << i
                                                      << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DECIMAL128) {
            LOG("CsvConverter: Converting value '" << value << "' to decimal128 for column " << i);
            try {
                int32_t precision = schema.getPrecisionAtIdx(i);
                int32_t scale = schema.getScaleAtIdx(i);
                Decimal128 decimalValue = parseDecimal128ToValue(value, precision, scale);
                rowData->setDecimal128(i, decimalValue.LowBits(), decimalValue.HighBits());
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid decimal128 value '" << value << "' for column " << i
                                                               << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Decimal128 value '" << value << "' out of range for column " << i
                                                       << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DOUBLE) {
            LOG("CsvConverter: Converting value '" << value << "' to double for column " << i);
            try {
                rowData->setDouble(i, std::stod(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid double value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            } catch (const std::out_of_range& e) {
                LOG("CsvConverter: Double value '" << value << "' out of range for column " << i
                                                   << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_DATE32) {
            LOG("CsvConverter: Converting value '" << value << "' to date for column " << i);
            try {
                rowData->setInt(i, parseDateToEpochDays(value));
            } catch (const std::invalid_argument& e) {
                LOG("CsvConverter: Invalid date value '" << value << "' for column " << i << ", setting it as null.");
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to time for column " << i);
            try {
                static int milliSec = 0;
                rowData->setTimestamp(i, TimestampData::fromTimeString(value), milliSec);
            } catch (...) {
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_VARCHAR) {
            LOG("CsvConverter: Converting value '" << value << "' to string for column " << i);
            std::string_view sv = value;
            rowData->setStringView(i, sv);
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to timestamp for column " << i);
            try {
                static int milliSec = 3;
                rowData->setTimestamp(
                    i, TimestampData::fromString(value), milliSec); // 并没有读取毫秒精度以上的那部分小数点后的数据
            } catch (...) {
                rowData->setNullAt(i);
            }
        } else if (type == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            LOG("CsvConverter: Converting value '" << value << "' to timestamp with local time zone for column " << i);
            try {
                static int milliSec = 3;
                rowData->setTimestamp(i, TimestampData::fromLocalTimeString(value), milliSec);
            } catch (...) {
                rowData->setNullAt(i);
            }
        } else {
            throw std::runtime_error("Unsupported type: " + type);
        }
    }
    return rowData;
}

/**
 * @brief Convert csv rows to vector batch using the first row's schema as the target schema
 * @param csvRows
 * @return VectorBatch
 */
omnistream::VectorBatch* CsvConverter::convert(std::vector<CsvRow>& csvRows)
{
    // Create a dummy oneMap that directly uses the csv field index as the project field index
    std::vector<int> oneMap;
    oneMap.resize(csvRows[0].getSchema().getArity());
    for (int i = 0; i < csvRows[0].getSchema().getArity(); i++) {
        oneMap[i] = i;
    }
    return convert(csvRows, oneMap);
}

namespace {
// NOTE: Vector<T>::SetNull for string vectors must be called on the concrete type,
// otherwise LargeStringContainer::SetNull() is skipped and offsets[index+1] stays
// uninitialized. A trailing null row then yields a negative length downstream
// (Calc reads endOffset - startOffset), which crashes in SetValue/memcpy_s.
inline void SetNullAt(
    omnistream::VectorBatch* vectorBatch, size_t colIndex, size_t rowIndex, omniruntime::type::DataTypeId type)
{
    auto* vec = vectorBatch->Get(colIndex);
    if (type == omniruntime::type::DataTypeId::OMNI_VARCHAR || type == omniruntime::type::DataTypeId::OMNI_CHAR) {
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(vec)
            ->SetNull(rowIndex);
    } else {
        vec->SetNull(rowIndex);
    }
}
} // namespace

/**
 * @brief Convert csv rows to vector batch using the given `oneMap`
 * @param csvRows
 * @param oneMap mapping from project field index to csv field index
 * @return VectorBatch
 */

omnistream::VectorBatch* CsvConverter::convert(std::vector<CsvRow>& csvRows, std::vector<int>& oneMap)
{
    // create new vectorbatch
    std::vector<DataTypeId> targetTypes = csvRows[0].getSchema().getTypes();
    std::vector<DataTypeId> newVecBatchTypes;
    for (size_t i = 0; i < oneMap.size(); i++) {
        newVecBatchTypes.push_back(targetTypes[oneMap[i]]);
    }
    auto vectorBatch = omnistream::VectorBatch::CreateVectorBatch(csvRows.size(), newVecBatchTypes);
    for (size_t i = 0; i < oneMap.size(); i++) {
        if (newVecBatchTypes[i] == DataTypeId::OMNI_DECIMAL64) {
            int32_t precision = csvRows[0].getSchema().getPrecisionAtIdx(oneMap[i]);
            int32_t scale = csvRows[0].getSchema().getScaleAtIdx(oneMap[i]);
            if (precision <= 0) {
                precision = DECIMAL64_DEFAULT_PRECISION;
            }
            vectorBatch->Get(i)->SetDataType(std::make_shared<Decimal64DataType>(precision, scale));
        } else if (newVecBatchTypes[i] == DataTypeId::OMNI_DECIMAL128) {
            int32_t precision = csvRows[0].getSchema().getPrecisionAtIdx(oneMap[i]);
            int32_t scale = csvRows[0].getSchema().getScaleAtIdx(oneMap[i]);
            if (precision <= 0) {
                precision = DECIMAL128_DEFAULT_PRECISION;
            }
            vectorBatch->Get(i)->SetDataType(std::make_shared<Decimal128DataType>(precision, scale));
        }
    }
    // Put data
    for (size_t rowIndex = 0; rowIndex < csvRows.size(); rowIndex++) {
        CsvRow csvRow = csvRows[rowIndex];
        const CsvSchema schema = csvRow.getSchema();
        for (size_t colIndex = 0; colIndex < oneMap.size(); colIndex++) {
            int csvFieldIndex = oneMap[colIndex];
            CsvNode* node = getProjectedNodeOrNull(csvRow, csvFieldIndex);
            if (node == nullptr) {
                LOG("CsvConverter: Missing projected CSV field " << csvFieldIndex << " for column " << colIndex
                                                                 << " in row " << rowIndex
                                                                 << ", setting it as null in VectorBatch.");
                SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                continue;
            }

            omniruntime::type::DataTypeId nodeType = node->getType();
            std::string nodeValue = node->getValue();
            if (nodeType != targetTypes[csvFieldIndex]) {
                throw std::runtime_error("CsvNode mismatch.");
            }

            if (isCsvNullValue(nodeValue, schema)) {
                LOG("CsvConverter: Detected null value for column " << colIndex << " in row " << rowIndex
                                                                    << ", setting it as null in VectorBatch.");
                SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                continue;
            }

            switch (nodeType) {
                case (omniruntime::type::DataTypeId::OMNI_INT): {
                    try {
                        LOG("CsvConverter: Converting value '" << nodeValue << "' to integer for column " << colIndex
                                                               << " in row " << rowIndex);
                        vectorBatch->SetValueAt(colIndex, rowIndex, std::stoi(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        LOG("CsvConverter: Invalid integer value '" << nodeValue << "' for column " << colIndex
                                                                    << " in row " << rowIndex
                                                                    << ", setting it as null.");
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    } catch (const std::out_of_range& e) {
                        LOG("CsvConverter: Integer value '" << nodeValue << "' out of range for column " << colIndex
                                                            << " in row " << rowIndex << ", setting it as null.");
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    }
                    break;
                }
                case (omniruntime::type::DataTypeId::OMNI_BOOLEAN): {
                    try {
                        LOG("CsvConverter: Converting value '" << nodeValue << "' to boolean for column " << colIndex
                                                               << " in row " << rowIndex);
                        bool boolVal = false;
                        std::string trimmed = nodeValue;
                        trimmed.erase(
                            trimmed.begin(), std::find_if(trimmed.begin(), trimmed.end(), [](unsigned char ch) {
                                return !::isspace(ch);
                            }));
                        trimmed.erase(
                            std::find_if(
                                trimmed.rbegin(), trimmed.rend(), [](unsigned char ch) { return !::isspace(ch); })
                                .base(),
                            trimmed.end());
                        std::transform(trimmed.begin(), trimmed.end(), trimmed.begin(), [](unsigned char ch) {
                            return static_cast<char>(::tolower(ch));
                        });
                        if (trimmed == "true") {
                            boolVal = true;
                        }
                        vectorBatch->SetValueAt(colIndex, rowIndex, boolVal);
                    } catch (const std::invalid_argument& e) {
                        LOG("CsvConverter: Invalid boolean value '" << nodeValue << "' for column " << colIndex
                                                                    << " in row " << rowIndex
                                                                    << ", setting it as null.");
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG: {
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, parseCsvBigInt(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        // Not a valid number
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    } catch (const std::out_of_range& e) {
                        // Number is out of range for long
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DECIMAL64: {
                    try {
                        int32_t precision = schema.getPrecisionAtIdx(csvFieldIndex);
                        int32_t scale = schema.getScaleAtIdx(csvFieldIndex);
                        vectorBatch->SetValueAt(
                            colIndex, rowIndex, parseDecimal64ToUnscaledLong(nodeValue, precision, scale));
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                    try {
                        int32_t precision = schema.getPrecisionAtIdx(csvFieldIndex);
                        int32_t scale = schema.getScaleAtIdx(csvFieldIndex);
                        Decimal128 decimalValue = parseDecimal128ToValue(nodeValue, precision, scale);
                        vectorBatch->SetValueAt(colIndex, rowIndex, decimalValue);
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP: {
                    try {
                        vectorBatch->SetValueAt(
                            colIndex, rowIndex, TimestampData::fromString(nodeValue).getMillisecond());
                    } catch (...) {
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                    try {
                        vectorBatch->SetValueAt(
                            colIndex, rowIndex, TimestampData::fromLocalTimeString(nodeValue).getMillisecond());
                    } catch (...) {
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, std::stod(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    } catch (const std::out_of_range& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DATE32: {
                    try {
                        vectorBatch->SetValueAt(colIndex, rowIndex, parseDateToEpochDays(nodeValue));
                    } catch (const std::invalid_argument& e) {
                        vectorBatch->Get(colIndex)->SetNull(rowIndex);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
                    try {
                        vectorBatch->SetValueAt(
                            colIndex, rowIndex, TimestampData::fromTimeString(nodeValue).getMillisecond());
                    } catch (...) {
                        SetNullAt(vectorBatch, colIndex, rowIndex, newVecBatchTypes[colIndex]);
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
                    auto stringVec = reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        vectorBatch->Get(colIndex));
                    std::string_view strView(nodeValue.data(), nodeValue.size());
                    stringVec->SetValue(rowIndex, strView);
                    break;
                }
                default: std::runtime_error("DataType not supported yet!");
            }
        }
    }
    return vectorBatch;
}

} // namespace csv
} // namespace omnistream
