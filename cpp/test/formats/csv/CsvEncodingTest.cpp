// Unit tests for the CSV encoding primitives (CsvEncoding.h).
// Validates the Jackson 2.12.4 byte contract used by the native filesystem
// CSV sink: quoting triggers, quote-doubling, BigDecimal.toString()-style
// decimals, and SQL_TIMESTAMP_FORMAT timestamps.

#include <gtest/gtest.h>
#include <string>
#include "formats/csv/CsvEncoding.h"

using omnistream::csv::CsvFormatOptions;
using omnistream::csv::appendQuotedField;
using omnistream::csv::appendNullField;
using omnistream::csv::formatDecimal;
using omnistream::csv::formatTimestamp;
using omnistream::csv::formatTimestampLtz;

static std::string quote(const std::string& v, const CsvFormatOptions& opts = CsvFormatOptions{})
{
    std::string out;
    appendQuotedField(out, v, opts);
    return out;
}

TEST(CsvEncodingTest, JacksonLooseQuoting)
{
    EXPECT_EQ(quote("hello"), "hello");
    // space (0x20 < 0x2D) triggers quoting
    EXPECT_EQ(quote("hello world"), "\"hello world\"");
    // comma triggers quoting
    EXPECT_EQ(quote("a,b"), "\"a,b\"");
    // quote char is doubled inside quotes
    EXPECT_EQ(quote("a\"b"), "\"a\"\"b\"");
    // CR/LF/TAB trigger quoting; bytes emitted verbatim inside quotes
    EXPECT_EQ(quote("a\rb"), "\"a\rb\"");
    EXPECT_EQ(quote("a\nb"), "\"a\nb\"");
    EXPECT_EQ(quote("a\tb"), "\"a\tb\"");
    // backslash triggers quoting, emitted verbatim
    EXPECT_EQ(quote("a\\b"), "\"a\\b\"");
    // '-' (0x2D) and digits do not trigger quoting
    EXPECT_EQ(quote("a-b"), "a-b");
    EXPECT_EQ(quote("-5"), "-5");
    // length > 24 forces quoting (Jackson _cfgMaxQuoteCheckChars = 24)
    EXPECT_EQ(quote(std::string(25, 'a')), "\"" + std::string(25, 'a') + "\"");
    EXPECT_EQ(quote(std::string(24, 'a')), std::string(24, 'a'));
}

TEST(CsvEncodingTest, EscapeCharacterIsDoubledWhenConfigured)
{
    CsvFormatOptions opts;
    opts.hasEscapeCharacter = true;
    opts.escapeCharacter = '\\';
    // Jackson writes the escape char twice inside quotes (observed: vanilla emits
    // `"back\\slash"` for csv.escape-character = '\')
    EXPECT_EQ(quote("back\\slash", opts), "\"back\\\\slash\"");
    // the quote char is still doubled, not backslash-escaped
    // (ESCAPE_QUOTE_CHAR_WITH_ESCAPE_CHAR defaults to false)
    EXPECT_EQ(quote("a\"b", opts), "\"a\"\"b\"");
    // regression guard: without an escape character the byte stays verbatim
    EXPECT_EQ(quote("back\\slash"), "\"back\\slash\"");
}

TEST(CsvEncodingTest, NullField)
{
    CsvFormatOptions opts;
    std::string nul;
    appendNullField(nul, opts);
    EXPECT_EQ(nul, ""); // no null-literal -> empty field
    CsvFormatOptions withNull;
    withNull.hasNullLiteral = true;
    withNull.nullLiteral = "\\N";
    std::string nul2;
    appendNullField(nul2, withNull);
    EXPECT_EQ(nul2, "\\N");
}

TEST(CsvEncodingTest, BigDecimalToString)
{
    // Flink renders DECIMAL as BigDecimal(unscaled, scale).stripTrailingZeros()
    // .toString(): trailing zeros drop and the scale shrinks; plain when the
    // adjusted exponent (=-scale + len-1) >= -6, else scientific.
    EXPECT_EQ(formatDecimal("1", 7), "1E-7"); // 0.0000001
    EXPECT_EQ(formatDecimal("1", 6), "0.000001");
    EXPECT_EQ(formatDecimal("1", 4), "0.0001");
    EXPECT_EQ(formatDecimal("1", 3), "0.001");
    EXPECT_EQ(formatDecimal("12345", 2), "123.45");
    EXPECT_EQ(formatDecimal("0", 2), "0");             // 0.00 -> 0 (stripped)
    EXPECT_EQ(formatDecimal("500", 2), "5");           // 5.00 -> 5 (stripped)
    EXPECT_EQ(formatDecimal("10000", 2), "1E+2");      // 100.00 -> 1E+2 (stripped)
    EXPECT_EQ(formatDecimal("550", 2), "5.5");         // -5.50 -> 5.5 (sign by caller)
    EXPECT_EQ(formatDecimal("10", 2), "0.1");          // 0.10 -> 0.1 (stripped)
    EXPECT_EQ(formatDecimal("1230000", 2), "1.23E+4"); // 12300.00 -> 1.23E+4
    EXPECT_EQ(formatDecimal("123", 0), "123");
}

TEST(CsvEncodingTest, SqlTimestampFormat)
{
    // date + ' ' + time, 0..3 fractional digits, trailing zeros trimmed
    EXPECT_EQ(formatTimestamp(0, 3), "1970-01-01 00:00:00");
    EXPECT_EQ(formatTimestamp(123, 3), "1970-01-01 00:00:00.123");
    EXPECT_EQ(formatTimestamp(120, 3), "1970-01-01 00:00:00.12");
    EXPECT_EQ(formatTimestamp(100, 3), "1970-01-01 00:00:00.1");
    // negative millis use floor division
    EXPECT_EQ(formatTimestamp(-1, 3), "1969-12-31 23:59:59.999");
    EXPECT_EQ(formatTimestamp(0, 0), "1970-01-01 00:00:00");
    EXPECT_EQ(formatTimestamp(1000, 3), "1970-01-01 00:00:01");
}

TEST(CsvEncodingTest, SqlTimestampLtzFormat)
{
    // TIMESTAMP_LTZ renders the UTC instant plus a literal 'Z'; the text is the
    // same as SQL_TIMESTAMP_FORMAT and is independent of table.local-time-zone.
    EXPECT_EQ(formatTimestampLtz(0, 3), "1970-01-01 00:00:00Z");
    EXPECT_EQ(formatTimestampLtz(123, 3), "1970-01-01 00:00:00.123Z");
    EXPECT_EQ(formatTimestampLtz(120, 3), "1970-01-01 00:00:00.12Z");
    EXPECT_EQ(formatTimestampLtz(100, 3), "1970-01-01 00:00:00.1Z");
    EXPECT_EQ(formatTimestampLtz(-1, 3), "1969-12-31 23:59:59.999Z");
    // vanilla baseline: source 2025-01-15 10:00:00.123 in +08:00
    // -> 2025-01-15 02:00:00.123Z (verified against stock Flink output)
    EXPECT_EQ(formatTimestampLtz(1736906400123LL, 3), "2025-01-15 02:00:00.123Z");
}
