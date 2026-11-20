#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <optional>

#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "table/runtime/operators/source/csv/CsvInputFormat.h"
#include "table/runtime/operators/source/csv/CsvRow.h"
#include "table/runtime/operators/source/csv/CsvSchema.h"
#include "table/runtime/operators/source/csv/CsvConverter.h"
#include "table/types/logical/LogicalType.h"
#include "type/data_type.h"

omniruntime::type::DataTypeId OMNI_LONG_TYPE = omniruntime::type::DataTypeId::OMNI_LONG;
omniruntime::type::DataTypeId OMNI_INT_TYPE = omniruntime::type::DataTypeId::OMNI_INT;
omniruntime::type::DataTypeId OMNI_VARCHAR_TYPE = omniruntime::type::DataTypeId::OMNI_VARCHAR;
omniruntime::type::DataTypeId OMNI_TIMESTAMP_TYPE = omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE;

TEST(CsvConversionTest, CsvRowBigIntConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_LONG_TYPE, OMNI_LONG_TYPE, OMNI_LONG_TYPE});
    omnistream::csv::CsvRow row = omnistream::csv::CsvRow("1,2,3", schema);
    BinaryRowData* rowData = omnistream::csv::CsvConverter::convert(row);
    EXPECT_EQ(*rowData->getLong(0), 1);
    EXPECT_EQ(*rowData->getLong(1), 2);
    EXPECT_EQ(*rowData->getLong(2), 3);
}

TEST(CsvConversionTest, CsvBigIntRequiresCompleteInteger)
{
    struct TestCase {
        const char* input;
        std::optional<int64_t> expected;
    };
    const std::vector<TestCase> cases = {
        {"1.17E+18", std::nullopt},
        {"123abc", std::nullopt},
        {"9223372036854775808", std::nullopt},
        {"-9223372036854775809", std::nullopt},
        {"null", std::nullopt},
        {"", std::nullopt},
        {"\t +42 \r", 42},
        {"9223372036854775807", std::numeric_limits<int64_t>::max()},
        {"-9223372036854775808", std::numeric_limits<int64_t>::min()},
    };

    omnistream::csv::CsvSchema schema({OMNI_LONG_TYPE});
    schema.setNullValue("null");
    std::vector<omnistream::csv::CsvRow> rows;
    for (const auto& testCase : cases) {
        rows.emplace_back(testCase.input, schema);
    }

    const std::unique_ptr<omnistream::VectorBatch> batch(omnistream::csv::CsvConverter::convert(rows));
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(cases[i].input);
        const std::unique_ptr<BinaryRowData> row(omnistream::csv::CsvConverter::convert(rows[i]));
        const auto expected = cases[i].expected;
        ASSERT_EQ(row->isNullAt(0), !expected.has_value());
        ASSERT_EQ(batch->Get(0)->IsNull(i), !expected.has_value());
        if (expected.has_value()) {
            EXPECT_EQ(*row->getLong(0), *expected);
            EXPECT_EQ(batch->GetValueAt<int64_t>(0, i), *expected);
        }
    }
}

TEST(CsvConversionTest, CsvRowVarCharConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE});
    omnistream::csv::CsvRow row =
        omnistream::csv::CsvRow("hi,how are you today,hi,i am fine thank you and you", schema);
    BinaryRowData* rowData = omnistream::csv::CsvConverter::convert(row);
    EXPECT_EQ(std::string(rowData->getStringView(0)), "hi"); // varchar less than or equal 7 bytes
    EXPECT_EQ(
        std::string(rowData->getStringView(1)),
        "how are you today");                                // varchar greater than 7 bytes in middle of the string
    EXPECT_EQ(std::string(rowData->getStringView(2)), "hi"); // varchar less than or equal 7 bytes
    EXPECT_EQ(
        std::string(rowData->getStringView(3)),
        "i am fine thank you and you"); // varchar greater than 7 bytes in the end of the string
}

TEST(CsvConversionTest, CsvQuotesConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE});
    std::string s = R"(!\&KELQ]n?,L5O9c\pN7P,"lVfIve^L*,",9w#sXQ-d>E)";
    omnistream::csv::CsvRow row = omnistream::csv::CsvRow(s, schema);
    BinaryRowData* rowData = omnistream::csv::CsvConverter::convert(row);
    EXPECT_EQ(std::string(rowData->getStringView(0)), R"(!\&KELQ]n?)"); // varchar less than or equal 7 bytes
    EXPECT_EQ(
        std::string(rowData->getStringView(1)),
        R"(L5O9c\pN7P)"); // varchar greater than 7 bytes in middle of the string
    EXPECT_EQ(std::string(rowData->getStringView(2)), R"(lVfIve^L*,)"); // varchar less than or equal 7 bytes
}

TEST(CsvConversionTest, CsvRowTimestampConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_TIMESTAMP_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_TIMESTAMP_TYPE});
    omnistream::csv::CsvRow row =
        omnistream::csv::CsvRow("2025-02-07 00:00:00.000,2025-02-07 16:33:20.111,2025-02-07 22:00:00.001", schema);
    BinaryRowData* rowData = omnistream::csv::CsvConverter::convert(row);
    EXPECT_EQ(rowData->getTimestamp(0).getMillisecond(), 1738886400000);
    EXPECT_EQ(rowData->getTimestamp(1).getMillisecond(), 1738946000111);
    EXPECT_EQ(rowData->getTimestamp(2).getMillisecond(), 1738965600001);
}

TEST(CsvConversionTest, CsvRowMixedTypeConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_LONG_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_VARCHAR_TYPE});
    omnistream::csv::CsvRow row = omnistream::csv::CsvRow("how are you today,1,2025-02-07 00:00:00.000,hi", schema);
    BinaryRowData* rowData = omnistream::csv::CsvConverter::convert(row);
    EXPECT_EQ(std::string(rowData->getStringView(0)), "how are you today");
    EXPECT_EQ(*rowData->getLong(1), 1);
    EXPECT_EQ(rowData->getTimestamp(2).getMillisecond(), 1738886400000);
    EXPECT_EQ(std::string(rowData->getStringView(3)), "hi");
}

TEST(CsvConversionTest, CsvRowIntNullConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_INT_TYPE, OMNI_VARCHAR_TYPE});
    schema.setNullValue("null");
    omnistream::csv::CsvRow row = omnistream::csv::CsvRow("null,hi", schema);
    BinaryRowData* rowData = omnistream::csv::CsvConverter::convert(row);
    EXPECT_TRUE(rowData->isNullAt(0));
    EXPECT_EQ(std::string(rowData->getStringView(1)), "hi");
}

TEST(CsvConversionTest, CsvRowConfiguredNullLiteralIsCaseSensitive)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE});
    schema.setNullValue("null");

    BinaryRowData* upperRowData = omnistream::csv::CsvConverter::convert(omnistream::csv::CsvRow("NULL", schema));
    EXPECT_FALSE(upperRowData->isNullAt(0));
    EXPECT_EQ(std::string(upperRowData->getStringView(0)), "NULL");

    BinaryRowData* lowerRowData = omnistream::csv::CsvConverter::convert(omnistream::csv::CsvRow("null", schema));
    EXPECT_TRUE(lowerRowData->isNullAt(0));
}

TEST(CsvConversionTest, CsvRowBigIntVecBatchConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_LONG_TYPE, OMNI_LONG_TYPE, OMNI_LONG_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    for (int i = 0; i < 5; i++) {
        omnistream::csv::CsvRow row = omnistream::csv::CsvRow("1,2,3", schema);
        csvRows.push_back(row);
    }

    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    for (int i = 0; i < 5; i++) {
        std::cout << batch->GetValueAt<int64_t>(0, i) << std::endl;
        std::cout << batch->GetValueAt<int64_t>(1, i) << std::endl;
        std::cout << batch->GetValueAt<int64_t>(2, i) << std::endl;

        EXPECT_EQ(batch->GetValueAt<int64_t>(0, i), 1L);
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, i), 2L);
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, i), 3L);
    }
}

TEST(CsvConversionTest, CsvRowTimestampVecBatchConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_TIMESTAMP_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_TIMESTAMP_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    for (int i = 0; i < 5; i++) {
        omnistream::csv::CsvRow row =
            omnistream::csv::CsvRow("2025-02-07 00:00:00.000,2025-02-07 16:33:20.111,2025-02-07 22:00:00.001", schema);
        ;
        csvRows.push_back(row);
    }

    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(batch->GetValueAt<int64_t>(0, i), 1738886400000);
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, i), 1738946000111);
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, i), 1738965600001);
    }
}

TEST(CsvConversionTest, CsvRowVarCharVecBatchConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE, OMNI_VARCHAR_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    for (int i = 0; i < 5; i++) {
        omnistream::csv::CsvRow row =
            omnistream::csv::CsvRow("hi,how are you today,hi,i am fine thank you and you", schema);
        csvRows.push_back(row);
    }

    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    for (int i = 0; i < 5; i++) {
        auto val1 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(0))
                ->GetValue(i);
        std::string str1(val1);

        auto val2 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(1))
                ->GetValue(i);
        std::string str2(val2);

        auto val3 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(2))
                ->GetValue(i);
        std::string str3(val3);

        auto val4 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(3))
                ->GetValue(i);
        std::string str4(val4);

        EXPECT_EQ(str1, "hi");
        EXPECT_EQ(str2, "how are you today");
        EXPECT_EQ(str3, "hi");
        EXPECT_EQ(str4, "i am fine thank you and you");
    }
}

TEST(CsvConversionTest, CsvRowMixedTypeVecBatchConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_LONG_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_VARCHAR_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    for (int i = 0; i < 5; i++) {
        omnistream::csv::CsvRow row = omnistream::csv::CsvRow("how are you today,1,2025-02-07 00:00:00.000,hi", schema);
        csvRows.push_back(row);
    }
    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    for (int i = 0; i < 5; i++) {
        auto val1 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(0))
                ->GetValue(i);
        std::string str1(val1);

        auto val2 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(3))
                ->GetValue(i);
        std::string str2(val2);

        EXPECT_EQ(str1, "how are you today");
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, i), 1);
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, i), 1738886400000);
        EXPECT_EQ(str2, "hi");
    }
}

TEST(CsvConversionTest, CsvRowIntNullVecBatchConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_INT_TYPE, OMNI_VARCHAR_TYPE});
    schema.setNullValue("null");
    std::vector<omnistream::csv::CsvRow> csvRows;
    csvRows.emplace_back("null,hello", schema);
    csvRows.emplace_back("null,world", schema);

    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    auto* intVector = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(0));
    EXPECT_TRUE(intVector->IsNull(0));
    EXPECT_TRUE(intVector->IsNull(1));
}

TEST(CsvConversionTest, CsvRowVecBatchConfiguredNullLiteralIsCaseSensitive)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE});
    schema.setNullValue("null");
    std::vector<omnistream::csv::CsvRow> csvRows;
    csvRows.emplace_back("NULL", schema);
    csvRows.emplace_back("null", schema);

    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    auto* stringVector =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
            batch->Get(0));
    EXPECT_FALSE(stringVector->IsNull(0));
    EXPECT_EQ(std::string(stringVector->GetValue(0)), "NULL");
    EXPECT_TRUE(stringVector->IsNull(1));
}

TEST(CsvConversionTest, CsvFileVectorBatchConversion)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_LONG_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_VARCHAR_TYPE});
    omnistream::csv::CsvInputFormat<omnistream::VectorBatch> csvFormat(schema, 100);

    csvFormat.open(new omnistream::InputSplit("input/testVectorBatch.csv", 0, 100));
    std::vector<omnistream::csv::CsvRow> csvRows;
    omnistream::csv::CsvRow* row;
    while ((row = csvFormat.nextCsvRecord()) != nullptr) {
        csvRows.push_back(*row);
    }

    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows);
    std::string expectedCol1Values[3] = {"a", "b", "c"};
    int expectedCol2Values[3] = {1, 2, 3};
    long expectedCol31Values[3] = {1738886400000, 1738946000111, 1738965600001};
    std::string expectedCol4Values[3] = {"x", "y", "z"};

    for (int i = 0; i < batch->GetRowCount(); i++) {
        auto val1 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(0))
                ->GetValue(i);
        std::string str1(val1);

        auto val2 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(3))
                ->GetValue(i);
        std::string str2(val2);

        EXPECT_EQ(str1, expectedCol1Values[i]);
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, i), expectedCol2Values[i]);
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, i), expectedCol31Values[i]);
        EXPECT_EQ(str2, expectedCol4Values[i]);
    }
}

TEST(CsvConversionTest, CsvFileVectorBatchConversionWithMapping)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_LONG_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_VARCHAR_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    for (int i = 0; i < 5; i++) {
        omnistream::csv::CsvRow row = omnistream::csv::CsvRow("how are you today,1,2025-02-07 00:00:00.000,hi", schema);
        csvRows.push_back(row);
    }
    std::vector<int> mapping = {3, 0, 2, 1}; // CsvRow after mapping: "hi,how are you today,2025-02-07 00:00:00.000,1"
    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows, mapping);
    for (int i = 0; i < 5; i++) {
        auto val1 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(0))
                ->GetValue(i);
        std::string str1(val1);

        auto val2 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(1))
                ->GetValue(i);
        std::string str2(val2);

        EXPECT_EQ(str1, "hi");
        EXPECT_EQ(str2, "how are you today");
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, i), 1738886400000);
        EXPECT_EQ(batch->GetValueAt<int64_t>(3, i), 1);
    }
}

TEST(CsvConversionTest, CsvFileVectorBatchConversionWithSelectiveMapping)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_LONG_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_VARCHAR_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    for (int i = 0; i < 5; i++) {
        omnistream::csv::CsvRow row = omnistream::csv::CsvRow("how are you today,1,2025-02-07 00:00:00.000,hi", schema);
        csvRows.push_back(row);
    }
    std::vector<int> mapping = {
        3, 2, 1}; // CsvRow after selectivemapping: "hi,2025-02-07 00:00:00.000,how are you today,1"
    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows, mapping);
    for (int i = 0; i < 5; i++) {
        auto val1 =
            reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                batch->Get(0))
                ->GetValue(i);
        std::string str1(val1);

        EXPECT_EQ(str1, "hi");
        EXPECT_EQ(batch->GetValueAt<int64_t>(1, i), 1738886400000);
        EXPECT_EQ(batch->GetValueAt<int64_t>(2, i), 1);
    }
}

TEST(CsvConversionTest, CsvVecBatchMissingProjectedFieldBecomesNull)
{
    omnistream::csv::CsvSchema schema({OMNI_VARCHAR_TYPE, OMNI_TIMESTAMP_TYPE, OMNI_TIMESTAMP_TYPE});
    std::vector<omnistream::csv::CsvRow> csvRows;
    csvRows.emplace_back("prefix,2025-02-07 00:00:00.000,2025-02-07 16:33:20.111", schema);
    csvRows.emplace_back("prefix,2025-02-07 00:00:00.000,2025-02-07 16:33:20.111", schema);
    csvRows.emplace_back("prefix,NULL", schema);
    csvRows.emplace_back("prefix,2025-02-07 00:00:00.000,2025-02-07 16:33:20.111", schema);

    std::vector<int> mapping = {1, 2};
    omnistream::VectorBatch* batch = omnistream::csv::CsvConverter::convert(csvRows, mapping);
    auto* firstTimestamp = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0));
    auto* secondTimestamp = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(1));

    ASSERT_EQ(batch->GetRowCount(), 4);
    EXPECT_EQ(batch->GetValueAt<int64_t>(0, 0), 1738886400000);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 0), 1738946000111);
    EXPECT_EQ(batch->GetValueAt<int64_t>(0, 1), 1738886400000);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 1), 1738946000111);
    EXPECT_TRUE(firstTimestamp->IsNull(2));
    EXPECT_TRUE(secondTimestamp->IsNull(2));
    EXPECT_EQ(batch->GetValueAt<int64_t>(0, 3), 1738886400000);
    EXPECT_EQ(batch->GetValueAt<int64_t>(1, 3), 1738946000111);
}
