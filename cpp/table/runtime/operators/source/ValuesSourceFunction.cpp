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

#include "ValuesSourceFunction.h"

#include <charconv>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "OmniOperatorJIT/core/src/type/TimestampConversion.h"
#include "OmniOperatorJIT/core/src/type/decimal128.h"
#include "table/data/TimestampData.h"
#include "types/logical/LogicTypeUtils.h"

using namespace omniruntime::type;

namespace omnistream {

namespace {

// Aligns with Flink DecimalData.fromBigDecimal: significant digits of the unscaled
// integer must be <= precision (0 has precision 1). A precision overflow produces
// a null value instead of raising an error.
bool IsPrecisionWithinLimit(const std::string& unscaledAbs, int32_t precision)
{
    if (unscaledAbs.empty()) {
        throw std::invalid_argument("empty unscaled decimal");
    }
    if (precision <= 0) {
        return true;
    }
    size_t start = 0;
    while (start + 1 < unscaledAbs.size() && unscaledAbs[start] == '0') {
        ++start;
    }
    return static_cast<int32_t>(unscaledAbs.size() - start) <= precision;
}

// Increments a decimal digit string by one, propagating the carry and prepending
// a '1' when every digit is '9'.
void IncrementUnscaled(std::string& digits)
{
    size_t index = digits.size();
    while (index > 0) {
        --index;
        if (digits[index] != '9') {
            ++digits[index];
            return;
        }
        digits[index] = '0';
    }
    digits.insert(digits.begin(), '1');
}

// Returns true when the string is non-empty and contains only '0'..'9'.
bool IsDigitsOnly(const std::string_view digits)
{
    if (digits.empty()) {
        return false;
    }
    for (const char c : digits) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    return true;
}

// Expands a non-negative decimal literal given in plain or scientific ('e'/'E')
// notation into canonical plain notation (digits with at most one '.'), e.g.
// "1.0E-7" -> "0.00000010", "1E+2" -> "100". Returns an empty string when the
// literal is not a valid decimal number.
std::string NormalizeDecimalLiteral(const std::string& value)
{
    std::string mantissa = value;
    int64_t exponent = 0;
    const size_t ePos = value.find_first_of("eE");
    if (ePos != std::string::npos) {
        mantissa = value.substr(0, ePos);
        const std::string expStr = value.substr(ePos + 1);
        size_t idx = 0;
        if (idx < expStr.size() && (expStr[idx] == '+' || expStr[idx] == '-')) {
            ++idx;
        }
        if (idx >= expStr.size() || !IsDigitsOnly(std::string_view(expStr).substr(idx))) {
            return "";
        }
        try {
            exponent = std::stoll(expStr);
        } catch (...) {
            return "";
        }
        if (exponent > 10000 || exponent < -10000) {
            return "";
        }
    }
    if (mantissa.find('.') != std::string::npos && mantissa.find('.', mantissa.find('.') + 1) != std::string::npos) {
        return "";
    }
    const size_t dotPos = mantissa.find('.');
    const std::string intPart = dotPos == std::string::npos ? mantissa : mantissa.substr(0, dotPos);
    const std::string fracPart = dotPos == std::string::npos ? "" : mantissa.substr(dotPos + 1);
    if (!IsDigitsOnly(intPart) || (!fracPart.empty() && !IsDigitsOnly(fracPart))) {
        return "";
    }
    const std::string integerPart = intPart.empty() ? "0" : intPart;
    if (exponent >= 0) {
        const size_t e = static_cast<size_t>(exponent);
        if (fracPart.size() > e) {
            return integerPart + fracPart.substr(0, e) + "." + fracPart.substr(e);
        }
        return integerPart + fracPart + std::string(e - fracPart.size(), '0');
    }
    const size_t k = static_cast<size_t>(-exponent);
    if (integerPart.size() > k) {
        return integerPart.substr(0, integerPart.size() - k) + "." + integerPart.substr(integerPart.size() - k) +
               fracPart;
    }
    return "0." + std::string(k - integerPart.size(), '0') + integerPart + fracPart;
}

std::string BuildUnscaledAbs(const std::string& value, int32_t scale, bool& negative)
{
    if (scale < 0) {
        throw std::invalid_argument("invalid decimal scale for literal: " + value);
    }
    std::string valueStr = value;
    negative = false;
    if (!valueStr.empty() && valueStr[0] == '-') {
        negative = true;
        valueStr = valueStr.substr(1);
    } else if (!valueStr.empty() && valueStr[0] == '+') {
        valueStr = valueStr.substr(1);
    }
    valueStr = NormalizeDecimalLiteral(valueStr);
    if (valueStr.empty()) {
        throw std::invalid_argument("invalid decimal literal: " + value);
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
    if (!IsDigitsOnly(intPart) || (!fracPart.empty() && !IsDigitsOnly(fracPart))) {
        throw std::invalid_argument("invalid decimal literal: " + value);
    }
    // Round the literal to the target scale with half-up rounding, matching
    // Flink DecimalData.fromBigDecimal(bd, precision, scale) which uses
    // bd.setScale(scale, RoundingMode.HALF_UP).
    bool roundUp = false;
    std::string keptFrac;
    if (static_cast<int32_t>(fracPart.length()) > scale) {
        roundUp = fracPart[scale] >= '5';
        keptFrac = fracPart.substr(0, scale);
    } else {
        keptFrac = fracPart;
    }
    if (static_cast<int32_t>(keptFrac.length()) < scale) {
        keptFrac += std::string(scale - keptFrac.length(), '0');
    }
    std::string unscaledAbs = intPart + keptFrac;
    if (roundUp) {
        IncrementUnscaled(unscaledAbs);
    }
    if (unscaledAbs.empty()) {
        throw std::invalid_argument("invalid decimal literal: " + value);
    }
    return unscaledAbs;
}

std::optional<long> ParseDecimal64ToUnscaledLong(const std::string& value, int32_t precision, int32_t scale)
{
    bool negative = false;
    std::string unscaledAbs = BuildUnscaledAbs(value, scale, negative);
    if (!IsPrecisionWithinLimit(unscaledAbs, precision)) {
        return std::nullopt;
    }
    long unscaledValue = 0;
    const char* begin = unscaledAbs.data();
    const char* end = begin + unscaledAbs.size();
    const auto [ptr, ec] = std::from_chars(begin, end, unscaledValue);
    if (ec == std::errc::result_out_of_range) {
        return std::nullopt;
    }
    if (ec != std::errc{} || ptr != end) {
        throw std::invalid_argument("invalid decimal literal: " + value);
    }
    if (negative) {
        unscaledValue = -unscaledValue;
    }
    return unscaledValue;
}

std::optional<Decimal128> ParseDecimal128ToValue(const std::string& value, int32_t precision, int32_t scale)
{
    bool negative = false;
    std::string unscaledAbs = BuildUnscaledAbs(value, scale, negative);
    if (!IsPrecisionWithinLimit(unscaledAbs, precision)) {
        return std::nullopt;
    }
    if (!IsDigitsOnly(unscaledAbs)) {
        throw std::invalid_argument("invalid decimal literal: " + value);
    }
    if (negative) {
        unscaledAbs = "-" + unscaledAbs;
    }
    return Decimal128(std::string_view(unscaledAbs));
}

// Some Flink type strings map to a native type that would silently store a value
// of the wrong kind (FLOAT -> OMNI_INT) or be treated as VARCHAR (BYTES / BINARY /
// VARBINARY). Reject them up-front so failures surface at operator construction
// instead of while running.
void RejectMisMappedType(const std::string& typeStr)
{
    std::string base = LogicTypeUtils::stripFlinkTypeExtras(typeStr);
    if (base == "FLOAT" || base == "BYTES" || LogicTypeUtils::startsWith(base, "BINARY") ||
        LogicTypeUtils::startsWith(base, "VARBINARY")) {
        throw std::runtime_error("unsupported VALUES output type for OmniStream native: " + typeStr);
    }
}

std::string CellToString(const nlohmann::json& cell)
{
    if (cell.is_string()) {
        return cell.get<std::string>();
    }
    if (cell.is_boolean()) {
        return cell.get<bool>() ? "true" : "false";
    }
    if (cell.is_number()) {
        return cell.dump();
    }
    throw std::invalid_argument("unsupported VALUES literal json cell type");
}

} // namespace

ValuesSourceFunction::ValuesSourceFunction(const nlohmann::json& config)
    : tuples_(config.contains("tuples") ? config["tuples"] : nlohmann::json::array()),
      isRunning_(true),
      emitted_(false)
{
    if (!config.contains("outputTypes") || !config["outputTypes"].is_array()) {
        throw std::invalid_argument("VALUES source description missing outputTypes");
    }
    for (const auto& typeJson : config["outputTypes"]) {
        std::string typeStr = typeJson.get<std::string>();
        DataTypeId typeId = LogicalType::flinkTypeToOmniTypeId(typeStr);
        if (typeId == DataTypeId::OMNI_INVALID) {
            throw std::runtime_error("unsupported VALUES output type: " + typeStr);
        }
        RejectMisMappedType(typeStr);
        if (!IsSupportedType(typeId)) {
            throw std::runtime_error("unsupported VALUES output type for OmniStream native: " + typeStr);
        }
        fieldTypes_.push_back(typeId);
        auto [precision, scale] = LogicalType::parseDecimalPrecisionScale(typeStr);
        precisions_.push_back(precision);
        scales_.push_back(scale);
    }
}

bool ValuesSourceFunction::IsSupportedType(DataTypeId typeId)
{
    switch (typeId) {
        case DataTypeId::OMNI_BOOLEAN:
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_DATE32:
        case DataTypeId::OMNI_LONG:
        case DataTypeId::OMNI_DOUBLE:
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DataTypeId::OMNI_CHAR:
        case DataTypeId::OMNI_VARCHAR:
        case DataTypeId::OMNI_DECIMAL64:
        case DataTypeId::OMNI_DECIMAL128:
            return true;
        default:
            return false;
    }
}

void ValuesSourceFunction::run(SourceContext* ctx)
{
    int taskId = this->getRuntimeContext()->getIndexOfThisSubtask();
    if (taskId > 0) {
        return;
    }
    while (isRunning_.load() && !reachedEnd()) {
        omnistream::VectorBatch* batch = nextRecord();
        if (batch == nullptr) {
            break;
        }
        ctx->collect(batch);
    }
}

void ValuesSourceFunction::cancel()
{
    isRunning_.store(false);
}

void ValuesSourceFunction::close()
{
    AbstractRichFunction::close();
}

bool ValuesSourceFunction::reachedEnd() const
{
    return emitted_ || tuples_.empty();
}

omnistream::VectorBatch* ValuesSourceFunction::nextRecord()
{
    if (reachedEnd()) {
        return nullptr;
    }
    emitted_ = true;
    return convert(tuples_, fieldTypes_, precisions_, scales_);
}

omnistream::VectorBatch* ValuesSourceFunction::convert(
    const nlohmann::json& tuples,
    const std::vector<DataTypeId>& fieldTypes,
    const std::vector<int32_t>& precisions,
    const std::vector<int32_t>& scales)
{
    if (tuples.empty()) {
        return new omnistream::VectorBatch(0);
    }
    size_t rowCount = tuples.size();
    size_t colCount = fieldTypes.size();
    std::unique_ptr<omnistream::VectorBatch> vectorBatch(
        omnistream::VectorBatch::CreateVectorBatch(static_cast<int>(rowCount), fieldTypes));
    for (size_t i = 0; i < colCount; ++i) {
        if (fieldTypes[i] == DataTypeId::OMNI_DECIMAL64) {
            int32_t precision = precisions[i] > 0 ? precisions[i] : DECIMAL64_DEFAULT_PRECISION;
            int32_t scale = scales[i];
            vectorBatch->Get(static_cast<int>(i))
                ->SetDataType(std::make_shared<Decimal64DataType>(precision, scale));
        } else if (fieldTypes[i] == DataTypeId::OMNI_DECIMAL128) {
            int32_t precision = precisions[i] > 0 ? precisions[i] : DECIMAL128_DEFAULT_PRECISION;
            int32_t scale = scales[i];
            vectorBatch->Get(static_cast<int>(i))
                ->SetDataType(std::make_shared<Decimal128DataType>(precision, scale));
        }
    }

    for (size_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        const nlohmann::json& row = tuples[rowIndex];
        if (!row.is_array() || row.size() != colCount) {
            throw std::runtime_error("VALUES tuple row width mismatch");
        }
        for (size_t colIndex = 0; colIndex < colCount; ++colIndex) {
            const nlohmann::json& cell = row[colIndex];
            if (cell.is_null()) {
                vectorBatch->Get(static_cast<int>(colIndex))->SetNull(static_cast<int32_t>(rowIndex));
                continue;
            }
            std::string value = CellToString(cell);
            DataTypeId type = fieldTypes[colIndex];
            switch (type) {
                case DataTypeId::OMNI_BOOLEAN: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex), static_cast<int>(rowIndex), value == "true");
                    break;
                }
                case DataTypeId::OMNI_INT: {
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), std::stoi(value));
                    break;
                }
                case DataTypeId::OMNI_DATE32: {
                    auto result =
                        omniruntime::type::util::fromDateString(std::string_view(value.data(), value.size()));
                    if (result.hasError()) {
                        throw std::invalid_argument("invalid date literal: " + value);
                    }
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), result.value());
                    break;
                }
                case DataTypeId::OMNI_LONG: {
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), std::stoll(value));
                    break;
                }
                case DataTypeId::OMNI_DOUBLE: {
                    vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), std::stod(value));
                    break;
                }
                case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex), static_cast<int>(rowIndex), TimestampData::stringToMillisOfDay(value));
                    break;
                }
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex), static_cast<int>(rowIndex), TimestampData::stringToEpochMillis(value));
                    break;
                }
                case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                    vectorBatch->SetValueAt(
                        static_cast<int>(colIndex),
                        static_cast<int>(rowIndex),
                        TimestampData::fromLocalTimeString(value).getMillisecond());
                    break;
                }
                case DataTypeId::OMNI_CHAR:
                case DataTypeId::OMNI_VARCHAR: {
                    auto stringVec = reinterpret_cast<
                        omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                        vectorBatch->Get(static_cast<int>(colIndex)));
                    std::string_view strView(value.data(), value.size());
                    stringVec->SetValue(static_cast<int>(rowIndex), strView);
                    break;
                }
                case DataTypeId::OMNI_DECIMAL64: {
                    int32_t precision = precisions[colIndex] > 0 ? precisions[colIndex] : DECIMAL64_DEFAULT_PRECISION;
                    auto unscaled = ParseDecimal64ToUnscaledLong(value, precision, scales[colIndex]);
                    if (unscaled.has_value()) {
                        vectorBatch->SetValueAt(static_cast<int>(colIndex), static_cast<int>(rowIndex), *unscaled);
                    } else {
                        vectorBatch->Get(static_cast<int>(colIndex))->SetNull(static_cast<int32_t>(rowIndex));
                    }
                    break;
                }
                case DataTypeId::OMNI_DECIMAL128: {
                    int32_t precision = precisions[colIndex] > 0 ? precisions[colIndex] : DECIMAL128_DEFAULT_PRECISION;
                    auto decimalValue = ParseDecimal128ToValue(value, precision, scales[colIndex]);
                    if (decimalValue.has_value()) {
                        vectorBatch->SetValueAt(
                            static_cast<int>(colIndex), static_cast<int>(rowIndex), *decimalValue);
                    } else {
                        vectorBatch->Get(static_cast<int>(colIndex))->SetNull(static_cast<int32_t>(rowIndex));
                    }
                    break;
                }
                default:
                    throw std::runtime_error("VALUES: unsupported native type " + std::to_string(type));
            }
        }
    }
    return vectorBatch.release();
}

} // namespace omnistream
