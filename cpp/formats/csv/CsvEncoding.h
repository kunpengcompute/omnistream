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

// CSV encoding primitives for the native filesystem sink. Quoting and type
// formatting reproduce the byte contract of Flink's filesystem CSV sink
// (Jackson 2.12.4 CsvGenerator, loose quoting, default features).

#ifndef OMNISTREAM_FORMATS_CSV_CSV_ENCODING_H
#define OMNISTREAM_FORMATS_CSV_CSV_ENCODING_H

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <string>

#include "core/include/common.h" // THROW_RUNTIME_ERROR

namespace omnistream {
namespace csv {

// CSV format options. Defaults match Flink CsvFormatOptions / Jackson CsvSchema
// defaults: separator ',', quote '"', no escape char, no null literal (null
// renders as the empty field), line separator '\n'.
struct CsvFormatOptions {
    char fieldDelimiter = ',';
    char quoteCharacter = '"';
    bool disableQuoteCharacter = false; // rejected by the native sink.
    bool hasEscapeCharacter = false;
    char escapeCharacter = '\0'; // valid only when hasEscapeCharacter is true.
    bool hasNullLiteral = false;
    std::string nullLiteral; // valid only when hasNullLiteral is true.

    // Mirror CsvFormatOptions defaults + buildCsvSchema semantics so that the
    // serialized options resolve to the same effective CsvSchema Jackson would
    // build. Throws on unsupported combinations.
    void validate() const
    {
        if (disableQuoteCharacter) {
            THROW_RUNTIME_ERROR("CSV native sink does not support 'disable-quote-character'");
        }
        if (fieldDelimiter == '\0') {
            THROW_RUNTIME_ERROR("CSV field-delimiter must be a non-null character");
        }
        if (quoteCharacter == '\0') {
            THROW_RUNTIME_ERROR("CSV quote-character must be set when quoting is enabled");
        }
    }
};

// Append one field applying Jackson 2.12.4 loose quoting: quote when the value
// exceeds 24 bytes, contains a byte below max(separator, quote, '\n') + 1, or
// contains the control-escape char; inside quotes the quote and the configured
// escape char are doubled. Used for strings, decimals and temporal text; typed
// number/boolean values go through appendRawField instead.
inline void appendQuotedField(std::string& out, const std::string& value, const CsvFormatOptions& opts)
{
    unsigned char sep = static_cast<unsigned char>(opts.fieldDelimiter);
    unsigned char quote = static_cast<unsigned char>(opts.quoteCharacter);
    unsigned char lf = static_cast<unsigned char>('\n');
    unsigned char minSafe = std::max({sep, quote, lf}) + 1;
    unsigned char controlEscape =
        opts.hasEscapeCharacter ? static_cast<unsigned char>(opts.escapeCharacter) : static_cast<unsigned char>('\\');
    unsigned char quoteEscape = quote; // ESCAPE_QUOTE_CHAR_WITH_ESCAPE_CHAR defaults to false.

    bool needQuote = false;
    if (value.size() > 24) {
        needQuote = true;
    } else {
        for (char c : value) {
            unsigned char uc = static_cast<unsigned char>(c);
            if (uc < minSafe || uc == quoteEscape || uc == controlEscape) {
                needQuote = true;
                break;
            }
        }
    }

    if (!needQuote) {
        out.append(value);
        return;
    }
    out.push_back(opts.quoteCharacter);
    for (char c : value) {
        if (c == opts.quoteCharacter) {
            out.push_back(opts.quoteCharacter); // double the quote (RFC 4180 style)
        } else if (opts.hasEscapeCharacter && c == opts.escapeCharacter) {
            out.push_back(opts.escapeCharacter); // Jackson doubles the escape char itself
        }
        out.push_back(c); // all other bytes (incl. CR/LF/TAB) verbatim
    }
    out.push_back(opts.quoteCharacter);
}

// Append a raw (unquoted) field for numberNode/booleanNode-typed values.
inline void appendRawField(std::string& out, const std::string& value)
{
    out.append(value);
}

// Append a null field. Jackson writes the configured null literal verbatim and
// unquoted; when no null literal is configured it writes the empty field.
inline void appendNullField(std::string& out, const CsvFormatOptions& opts)
{
    if (opts.hasNullLiteral) {
        out.append(opts.nullLiteral);
    }
    // else: empty field (zero bytes).
}

// Format a DECIMAL like BigDecimal(unscaled, scale).stripTrailingZeros().toString()
// as Flink does: drop trailing zeros (adjusting the scale), then print plain
// when scale >= 0 and the adjusted exponent >= -6, else in scientific form.
// `digits` has no sign prefix; `scale` is >= 0.
inline std::string formatDecimal(std::string digits, int32_t scale)
{
    if (digits.empty()) {
        return digits;
    }
    // stripTrailingZeros(): an all-zero value renders as "0".
    size_t origLen = digits.size();
    size_t lastNonZero = digits.find_last_not_of('0');
    if (lastNonZero == std::string::npos) {
        return "0";
    }
    int32_t stripped = static_cast<int32_t>(origLen - (lastNonZero + 1));
    digits.erase(lastNonZero + 1);
    scale -= stripped;

    if (scale == 0) {
        return digits;
    }
    int len = static_cast<int>(digits.size());
    long adjusted = -static_cast<long>(scale) + (len - 1);

    if (scale >= 0 && adjusted >= -6) {
        if (scale >= len) {
            return "0." + std::string(static_cast<size_t>(scale - len), '0') + digits;
        }
        return digits.substr(0, static_cast<size_t>(len - scale)) + "." +
               digits.substr(static_cast<size_t>(len - scale));
    }
    std::string out;
    out.push_back(digits[0]);
    if (len > 1) {
        out.push_back('.');
        out.append(digits.substr(1));
    }
    if (adjusted != 0) {
        out.push_back('E');
        if (adjusted > 0) {
            out.push_back('+');
        }
        out.append(std::to_string(adjusted));
    }
    return out;
}

// Format a TIMESTAMP(p<=3) millisecond value as "yyyy-MM-dd HH:mm:ss[.fff]"
// (SQL_TIMESTAMP_FORMAT), with floor division for negative values.
inline std::string formatTimestamp(int64_t millis, int32_t precision)
{
    // Floor division: seconds = floor(millis / 1000), fraction in [0, 999].
    int64_t seconds = millis / 1000;
    int64_t fraction = millis - seconds * 1000;
    if (fraction < 0) {
        seconds -= 1;
        fraction += 1000;
    }

    time_t t = static_cast<time_t>(seconds);
    struct tm tmResult;
    struct tm* tmp = gmtime_r(&t, &tmResult);
    if (tmp == nullptr) {
        THROW_RUNTIME_ERROR("CSV timestamp out of representable range: millis=" << millis);
    }
    char buffer[32];
    size_t len = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tmp);
    if (len == 0) {
        THROW_RUNTIME_ERROR("CSV timestamp strftime failed for millis=" << millis);
    }
    std::string out(buffer, len);

    if (precision > 0) {
        // Millisecond part, trailing zeros trimmed, matching Jackson.
        char fbuf[8];
        snprintf(fbuf, sizeof(fbuf), "%03lld", static_cast<long long>(fraction));
        std::string fracStr(fbuf);
        size_t last = fracStr.find_last_not_of('0');
        if (last != std::string::npos) {
            out.push_back('.');
            out.append(fracStr, 0, last + 1);
        }
        // All-zero fraction: emit nothing (not even the dot).
    }
    return out;
}

// Format a TIMESTAMP_LTZ(p<=3) millisecond value as the UTC instant plus 'Z'
// (SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT), independent of the session zone.
inline std::string formatTimestampLtz(int64_t millis, int32_t precision)
{
    return formatTimestamp(millis, precision) + "Z";
}

} // namespace csv
} // namespace omnistream

#endif // OMNISTREAM_FORMATS_CSV_CSV_ENCODING_H
