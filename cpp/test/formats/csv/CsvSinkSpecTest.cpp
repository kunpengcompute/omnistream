// Unit tests for CsvSinkSpec::fromJson (CsvSinkSpec.h): every admitted shape must
// parse and every shape the encoder cannot reproduce must throw.

#include <gtest/gtest.h>

#include <string>

#include "formats/csv/CsvSinkSpec.h"

using nlohmann::json;
using omnistream::csv::CsvLogicalType;
using omnistream::csv::CsvSinkSpec;

namespace {

json column(const std::string& name, const std::string& type, int precision = -1, int scale = -1)
{
    json c = {{"name", name}, {"type", type}, {"isNull", true}};
    if (precision >= 0) {
        c["precision"] = precision;
    }
    if (scale >= 0) {
        c["scale"] = scale;
    }
    return c;
}

// Minimal INSERT_ONLY filesystem/csv spec that the native encoder accepts.
json baseSpec()
{
    json spec = {
        {"connector", "filesystem"},
        {"format", "csv"},
        {"fields", json::array({column("id", "BIGINT"), column("name", "VARCHAR", 2000)})},
        {"nonPartitionIndexes", json::array({0, 1})},
        {"csvOptions", {{"fieldDelimiter", ","}, {"quoteCharacter", "\""}}},
    };
    return spec;
}

// Returns the exception message, or "" when fn() did not throw. Lets us assert
// both that a rejection happened and which option/field it named.
template <typename F>
std::string thrownBy(F&& fn)
{
    try {
        fn();
    } catch (const std::runtime_error& e) {
        return std::string(e.what());
    }
    return "";
}

void expectThrowsContaining(const json& spec, const std::string& needle)
{
    std::string msg = thrownBy([&spec]() { CsvSinkSpec::fromJson(spec); });
    EXPECT_NE(msg, "") << "expected a rejection mentioning '" << needle << "'";
    EXPECT_NE(msg.find(needle), std::string::npos) << "rejection '" << msg << "' does not mention '" << needle << "'";
}

} // namespace

TEST(CsvSinkSpecTest, AcceptsSupportedSpec)
{
    CsvSinkSpec spec = CsvSinkSpec::fromJson(baseSpec());
    EXPECT_EQ(spec.connector, "filesystem");
    EXPECT_EQ(spec.format, "csv");
    ASSERT_EQ(spec.fields.size(), 2u);
    EXPECT_EQ(spec.fields[0].logicalType, CsvLogicalType::BIGINT);
    EXPECT_EQ(spec.fields[1].logicalType, CsvLogicalType::VARCHAR);
    EXPECT_TRUE(spec.fields[1].nullable);
    EXPECT_EQ(spec.nonPartitionIndexes, (std::vector<int32_t>{0, 1}));
    EXPECT_EQ(spec.options.fieldDelimiter, ',');
    EXPECT_EQ(spec.options.quoteCharacter, '"');
    EXPECT_FALSE(spec.options.hasEscapeCharacter);
    EXPECT_FALSE(spec.options.hasNullLiteral);
}

TEST(CsvSinkSpecTest, AcceptsSpecWithOrWithoutCsvSpecVersion)
{
    // csvSpecVersion is ignored whether present, absent or bogus.
    json without = baseSpec();
    EXPECT_EQ(thrownBy([&without]() { CsvSinkSpec::fromJson(without); }), "");

    json with = baseSpec();
    with["csvSpecVersion"] = 1;
    EXPECT_EQ(thrownBy([&with]() { CsvSinkSpec::fromJson(with); }), "");

    json bogus = baseSpec();
    bogus["csvSpecVersion"] = "not-a-number";
    EXPECT_EQ(thrownBy([&bogus]() { CsvSinkSpec::fromJson(bogus); }), "");
}

TEST(CsvSinkSpecTest, AcceptsAdmittedScalarTypes)
{
    json spec = baseSpec();
    spec["fields"] = json::array({
        column("i", "INTEGER"),
        column("b", "BOOLEAN"),
        column("s", "STRING"),
        column("c", "CHAR", 8),
        column("ts", "TIMESTAMP", 3),
        column("ltz", "TIMESTAMP_LTZ", 3),
        column("d", "DECIMAL", 10, 2),
    });
    spec["nonPartitionIndexes"] = json::array({0, 1, 2, 3, 4, 5, 6});
    CsvSinkSpec parsed = CsvSinkSpec::fromJson(spec);
    ASSERT_EQ(parsed.fields.size(), 7u);
    EXPECT_EQ(parsed.fields[4].logicalType, CsvLogicalType::TIMESTAMP);
    EXPECT_EQ(parsed.fields[4].precision, 3);
    EXPECT_EQ(parsed.fields[5].logicalType, CsvLogicalType::TIMESTAMP_LTZ);
    EXPECT_EQ(parsed.fields[5].precision, 3);
    EXPECT_EQ(parsed.fields[6].logicalType, CsvLogicalType::DECIMAL);
    EXPECT_EQ(parsed.fields[6].precision, 10);
    EXPECT_EQ(parsed.fields[6].scale, 2);
}

TEST(CsvSinkSpecTest, RejectsNestedAndUnadmittedTypes)
{
    for (const char* type : {"ARRAY", "ROW", "MAP", "MULTISET"}) {
        json spec = baseSpec();
        spec["fields"] = json::array({column("f", type)});
        spec["nonPartitionIndexes"] = json::array({0});
        expectThrowsContaining(spec, "unknown logicalType");
    }
    // DOUBLE / DATE / TIME are recognized by the enum but not admitted by the native
    // encoder: they must be rejected, not encoded.
    for (const char* type : {"DOUBLE", "DATE", "TIME"}) {
        json spec = baseSpec();
        spec["fields"] = json::array({column("f", type)});
        spec["nonPartitionIndexes"] = json::array({0});
        expectThrowsContaining(spec, "not yet supported");
    }
}

TEST(CsvSinkSpecTest, RejectsTimestampAndLtzPrecisionAboveThree)
{
    json spec = baseSpec();
    spec["fields"] = json::array({column("ts", "TIMESTAMP", 9)});
    spec["nonPartitionIndexes"] = json::array({0});
    expectThrowsContaining(spec, "precision must be in [0,3]");

    // TIMESTAMP_LTZ(p>=4) must fall back, not fail at encode time.
    json ltz = baseSpec();
    ltz["fields"] = json::array({column("ltz", "TIMESTAMP_LTZ", 9)});
    ltz["nonPartitionIndexes"] = json::array({0});
    expectThrowsContaining(ltz, "precision must be in [0,3]");

    json ok = baseSpec();
    ok["fields"] = json::array({column("ts", "TIMESTAMP", 0), column("ltz", "TIMESTAMP_LTZ", 0)});
    ok["nonPartitionIndexes"] = json::array({0, 1});
    EXPECT_EQ(thrownBy([&ok]() { CsvSinkSpec::fromJson(ok); }), "");
}

TEST(CsvSinkSpecTest, RejectsInvalidDecimalPrecisionAndScale)
{
    json badPrecision = baseSpec();
    badPrecision["fields"] = json::array({column("d", "DECIMAL", 0, 0)});
    badPrecision["nonPartitionIndexes"] = json::array({0});
    expectThrowsContaining(badPrecision, "DECIMAL precision out of range");

    json badScale = baseSpec();
    badScale["fields"] = json::array({column("d", "DECIMAL", 10, 11)});
    badScale["nonPartitionIndexes"] = json::array({0});
    expectThrowsContaining(badScale, "DECIMAL scale out of range");
}

TEST(CsvSinkSpecTest, RejectsNonAsciiCsvOptions)
{
    json wideDelimiter = baseSpec();
    wideDelimiter["csvOptions"]["fieldDelimiter"] = "，"; // full-width comma: 3 UTF-8 bytes
    expectThrowsContaining(wideDelimiter, "fieldDelimiter");

    json nonAsciiQuote = baseSpec();
    nonAsciiQuote["csvOptions"]["quoteCharacter"] = "é"; // 2 UTF-8 bytes
    expectThrowsContaining(nonAsciiQuote, "quoteCharacter");

    json emptyDelimiter = baseSpec();
    emptyDelimiter["csvOptions"]["fieldDelimiter"] = "";
    expectThrowsContaining(emptyDelimiter, "fieldDelimiter");

    json longEscape = baseSpec();
    longEscape["csvOptions"]["escapeCharacter"] = "ab";
    expectThrowsContaining(longEscape, "escapeCharacter");
}

TEST(CsvSinkSpecTest, AcceptsSingleByteAsciiAndOptionalOptions)
{
    json spec = baseSpec();
    spec["csvOptions"] = {
        {"fieldDelimiter", "\t"},
        {"quoteCharacter", "'"},
        {"escapeCharacter", "\\"},
        {"nullLiteral", "\\N"},
    };
    CsvSinkSpec parsed = CsvSinkSpec::fromJson(spec);
    EXPECT_EQ(parsed.options.fieldDelimiter, '\t');
    EXPECT_EQ(parsed.options.quoteCharacter, '\'');
    EXPECT_TRUE(parsed.options.hasEscapeCharacter);
    EXPECT_EQ(parsed.options.escapeCharacter, '\\');
    EXPECT_TRUE(parsed.options.hasNullLiteral);
    EXPECT_EQ(parsed.options.nullLiteral, "\\N");
}

TEST(CsvSinkSpecTest, TreatsNullEscapeCharacterAsAbsent)
{
    json spec = baseSpec();
    spec["csvOptions"]["escapeCharacter"] = nullptr;
    spec["csvOptions"]["nullLiteral"] = nullptr;
    CsvSinkSpec parsed = CsvSinkSpec::fromJson(spec);
    EXPECT_FALSE(parsed.options.hasEscapeCharacter);
    EXPECT_FALSE(parsed.options.hasNullLiteral);
}

TEST(CsvSinkSpecTest, RejectsDisableQuoteCharacter)
{
    // The adapter gate never routes this to native (E1); if it ever arrives, the
    // native CsvFormatOptions::validate() must refuse rather than emit unquoted
    // fields that differ from stock Flink.
    json spec = baseSpec();
    spec["csvOptions"]["disableQuoteCharacter"] = true;
    expectThrowsContaining(spec, "disable-quote-character");
}

TEST(CsvSinkSpecTest, RejectsBadMetadata)
{
    // csvSpecVersion is ignored for any value; only the structural metadata below
    // is validated.
    json badConnector = baseSpec();
    badConnector["connector"] = "kafka";
    expectThrowsContaining(badConnector, "connector");

    json badFormat = baseSpec();
    badFormat["format"] = "json";
    expectThrowsContaining(badFormat, "format");

    json notAnObject = json::array({1, 2});
    expectThrowsContaining(notAnObject, "JSON object");
}

TEST(CsvSinkSpecTest, RejectsBadNonPartitionIndexes)
{
    json outOfRange = baseSpec();
    outOfRange["nonPartitionIndexes"] = json::array({0, 7});
    expectThrowsContaining(outOfRange, "out of range");

    json duplicate = baseSpec();
    duplicate["nonPartitionIndexes"] = json::array({0, 0});
    expectThrowsContaining(duplicate, "duplicate nonPartitionIndex");

    json negative = baseSpec();
    negative["nonPartitionIndexes"] = json::array({-1});
    expectThrowsContaining(negative, "out of range");
}
