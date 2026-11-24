/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#include "../../../../table/data/binary/BinaryRowData.cpp"
#include "../../../../table/data/writer/BinaryRowWriter.h"
#include <gtest/gtest.h>

TEST(BinaryRowDataTest, BasicTest)
{
    // consider header 1 byte.
    EXPECT_EQ((new BinaryRowData(0))->getFixedLengthPartSize(), 8);
    EXPECT_EQ((new BinaryRowData(1))->getFixedLengthPartSize(), 16);
    EXPECT_EQ((new BinaryRowData(65))->getFixedLengthPartSize(), 536);
    EXPECT_EQ((new BinaryRowData(128))->getFixedLengthPartSize(), 1048);

    // Buffer setup
    uint8_t buffer[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // // Create MemorySegment
    // MemorySegment* memSeg     = new MemorySegment(buffer, sizeof(buffer));
    // MemorySegment* segments[] = {memSeg};

    // // Create BinaryRowData and point to the memory segment
    // BinaryRowData* row = new BinaryRowData(4);

    // row->pointTo(memSeg, 10, 48);
    // EXPECT_EQ(memSeg, row->getSegments()[0]);
    BinaryRowData* row = new BinaryRowData(4);
    row->pointTo(buffer, 10, 48, 40);
    EXPECT_EQ(buffer, row->getSegment());
    row->setInt(0, 5);
    EXPECT_EQ(*(row->getInt(0)), 5);
}

TEST(BinaryRowDataTest, TimestampLowPercisionTest)
{
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(1);
    TimestampData data = TimestampData(1000, 1);
    row->setTimestamp(0, data, 1);
    // EXPECT_EQ(row->getTimestamp(0).getMillisecond(), 1000);
    delete row;
}

TEST(BinaryRowDataTest, TimestampHighPercisionTest)
{
    // Does not work, need to fix errors in high percision
    /*
    BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(1);
    TimestampData data = TimestampData(1000, 1000);
    row->setTimestamp(0, data, 1000);
    EXPECT_EQ(row->getTimestampPrecise(0).getMillisecond(), 1000);
    delete row;
    */
}

TEST(BinaryRowDataTest, HashCode_DeterminismTest)
{
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(1);

    EXPECT_EQ(row->hashCode(), row->hashCode());
    delete row;
}

TEST(BinaryRowDataTest, HashCode_CollisionTest)
{
    BinaryRowData* row1 = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData* row2 = BinaryRowData::createBinaryRowDataWithMem(2);

    EXPECT_NE(row1->hashCode(), row2->hashCode());

    delete row1;
    delete row2;
}

TEST(BinaryRowDataTest, HashCode_AlignsWithEqualityTest)
{
    // Buffer setup
    uint8_t buffer1[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg1     = new MemorySegment(buffer1, sizeof(buffer1));
    // MemorySegment* segments1[] = {memSeg1};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row1 = new BinaryRowData(4);
    // row1->pointTo(segments1, 1, 0, sizeof(buffer1));
    row1->pointTo(buffer1, 0, sizeof(buffer1), 40);

    // Set up identical BinaryRowData
    // Buffer setup
    uint8_t buffer2[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg2     = new MemorySegment(buffer2, sizeof(buffer2));
    // MemorySegment* segments2[] = {memSeg2};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row2 = new BinaryRowData(4);
    // row2->pointTo(segments2, 1, 0, sizeof(buffer2));
    row2->pointTo(buffer2, 0, sizeof(buffer2), 40);

    EXPECT_TRUE(*row1 == *row2);
    std::equal_to<RowData*> equalizerPtr;
    EXPECT_EQ(equalizerPtr(row1, row2), true);

    EXPECT_EQ(row1->hashCode(), row2->hashCode());
    std::hash<RowData*> hasherPtr;
    EXPECT_EQ(hasherPtr(row1), hasherPtr(row2));
    delete row1;
    delete row2;
}

TEST(BinaryRowDataTest, OperatorEquals_SelfReferenceTest)
{
    // Buffer setup
    uint8_t buffer[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg     = new MemorySegment(buffer, sizeof(buffer));
    // MemorySegment* segments[] = {memSeg};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row1 = new BinaryRowData(4);
    // row1->pointTo(segments, 1, 0, sizeof(buffer));
    row1->pointTo(buffer, 0, sizeof(buffer), sizeof(buffer));

    BinaryRowData* row2 = row1;

    EXPECT_TRUE(*row1 == *row2);

    delete row1;
}

TEST(BinaryRowDataTest, OperatorEquals_EqualBinaryRowDataTest)
{
    // Buffer setup
    uint8_t buffer1[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg1     = new MemorySegment(buffer1, sizeof(buffer1));
    // MemorySegment* segments1[] = {memSeg1};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row1 = new BinaryRowData(4);
    // row1->pointTo(segments1, 1, 0, sizeof(buffer1));
    row1->pointTo(buffer1, 0, sizeof(buffer1), sizeof(buffer1));

    // Set up identical BinaryRowData
    // Buffer setup
    uint8_t buffer2[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg2     = new MemorySegment(buffer2, sizeof(buffer2));
    // MemorySegment* segments2[] = {memSeg2};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row2 = new BinaryRowData(4);
    // row2->pointTo(segments2, 1, 0, sizeof(buffer2));
    row2->pointTo(buffer2, 0, sizeof(buffer2), sizeof(buffer2));

    EXPECT_TRUE(*row1 == *row2);

    delete row1;
    delete row2;
}

TEST(BinaryRowDataTest, OperatorEquals_CopyBinaryRowDataTest)
{
    // Buffer setup
    uint8_t buffer1[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg1     = new MemorySegment(buffer1, sizeof(buffer1));
    // MemorySegment* segments1[] = {memSeg1};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row1 = new BinaryRowData(4);
    // row1->pointTo(segments1, 1, 0, sizeof(buffer1));
    row1->pointTo(buffer1, 0, sizeof(buffer1), sizeof(buffer1));

    RowData* row2 = row1->copy();

    EXPECT_EQ(*row1->getLong(0), *row2->getLong(0));
    EXPECT_EQ(*row1->getLong(1), *row2->getLong(1));

    EXPECT_TRUE(*row1 == *row2);

    delete row1;
    delete row2;
}

TEST(BinaryRowDataTest, OperatorEquals_UnequalBinaryRowDataTest)
{
    // Buffer setup
    uint8_t buffer1[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84, // VARCHAR "Some" with `len` of 4
        0x53, 0x6f, 0x6d, 0x65, 0x6f, 0x6e, 0x65,
        0x87 // VARCHAR "Someone" with `len` of 7
    };

    // Create MemorySegment
    // MemorySegment* memSeg1     = new MemorySegment(buffer1, sizeof(buffer1));
    // MemorySegment* segments1[] = {memSeg1};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row1 = new BinaryRowData(4);
    // row1->pointTo(segments1, 1, 0, sizeof(buffer1));
    row1->pointTo(buffer1, 0, sizeof(buffer1), sizeof(buffer1));

    // Set up identical BinaryRowData
    // Buffer setup
    uint8_t buffer2[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // null bytes
        0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00,
        0x00, // BIGINT 1234567890 -> 0x499602d2
        0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, // BIGINT 10000      -> 0x2710
        0x53, 0x6f, 0x6d, 0x65, 0x00, 0x00, 0x00,
        0x84 // VARCHAR "Some" with `len` of 4
    };

    // Create MemorySegment
    // MemorySegment* memSeg2     = new MemorySegment(buffer2, sizeof(buffer2));
    // MemorySegment* segments2[] = {memSeg2};

    // Create BinaryRowData and point to the memory segment
    BinaryRowData* row2 = new BinaryRowData(4);
    // row2->pointTo(segments2, 1, 0, sizeof(buffer2));
    row2->pointTo(buffer2, 0, sizeof(buffer2), sizeof(buffer2));

    EXPECT_FALSE(*row1 == *row2);

    delete row1;
    delete row2;
}

TEST(BinaryRowDataTest, BinaryRowWriter_SetNullAtModifiesNullSet)
{
    BinaryRowData* row = new BinaryRowData(3);
    BinaryRowWriter* writer = new BinaryRowWriter(row);

    writer->setNullAt(0);
    writer->setNullAt(1);
    writer->setNullAt(2);
    writer->complete();
    EXPECT_TRUE(row->isNullAt(0));
    EXPECT_TRUE(row->isNullAt(1));
    EXPECT_TRUE(row->isNullAt(2));

    delete row;
    delete writer;
}

TEST(BinaryRowDataTest, SetLongTest)
{
    BinaryRowData* row = new BinaryRowData(3);
    BinaryRowWriter* writer = new BinaryRowWriter(row);

    row->setLong(0, 1L);
    row->setLong(1, 2L);
    row->setLong(2, 3L);

    // Works fine as setLong modifies the null bits
    EXPECT_FALSE(row->isNullAt(0));
    EXPECT_FALSE(row->isNullAt(1));
    EXPECT_FALSE(row->isNullAt(2));

    delete row;
    delete writer;
}

TEST(BinaryRowDataTest, CreateBinaryRowDataWithMemTest)
{
    int arity = 57;
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);

    // The null bits are initialized to 0, which means all fields are not null
    for (int pos = 0; pos < arity; pos++) {
        EXPECT_FALSE(row->isNullAt(pos));
    }

    row->setInt(0, 5);
    EXPECT_FALSE(row->isNullAt(0));
    delete row;
}

TEST(BinaryRowDataTest, TimestampSetCompactTime)
{
    int precision = 3;
    int arity = 2;
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    TimestampData timestamp = TimestampData::fromEpochMillis(123L, 456);
    row->setTimestamp(0, timestamp, precision);

    // Check values are set correctly
    EXPECT_EQ(*(row->getLong(0)), 123L);
    EXPECT_EQ(*(row->getInt(1)), 0);

    // Check null bits are set correctly
    EXPECT_FALSE(row->isNullAt(0));

    // Free offHeapBuffer
    // delete row->getSegments()[0]->getAll();
    // // Free the memory segment
    // delete row->getSegments()[0];
    // // Free the array storing the memory segments
    // delete row->getSegments();
    // Finally, free the row itself
    delete row;
}

TEST(BinaryRowDataTest, TimestampSetNonCompactTime)
{
    int precision = 4;
    int arity = 2;
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    TimestampData timestamp = TimestampData::fromEpochMillis(123L, 456);
    row->setTimestamp(0, timestamp, precision);

    // Check values are set correctly
    EXPECT_EQ(*(row->getLong(0)), 123L);
    EXPECT_EQ(*(row->getInt(1)), 456);

    // Check null bits are set correctly
    EXPECT_FALSE(row->isNullAt(0));
    EXPECT_FALSE(row->isNullAt(1));

    // Free offHeapBuffer
    // delete row->getSegments()[0]->getAll();
    // // Free the memory segment
    // delete row->getSegments()[0];
    // // Free the array storing the memory segments
    // delete row->getSegments();
    // Finally, free the row itself
    delete row;
}

TEST(BinaryRowDataTest, TimestampSetOutOfBoundNonCompactTimeThrowsException)
{
    int precision = 4;
    int arity = 1;
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    TimestampData timestamp = TimestampData::fromEpochMillis(123L, 456);

    EXPECT_THROW(row->setTimestamp(0, timestamp, precision), std::logic_error);

    // // Free offHeapBuffer
    // delete row->getSegments()[0]->getAll();
    // // Free the memory segment
    // delete row->getSegments()[0];
    // // Free the array storing the memory segments
    // delete row->getSegments();
    // Finally, free the row itself
    delete row;
}

TEST(BinaryRowDataTest, TimestampGetCompactTime)
{
    int precision = 3;
    int arity = 2;
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    row->setLong(0, 123L);

    TimestampData timestamp = row->getTimestamp(0);

    // Check values are get correctly
    EXPECT_EQ(timestamp.getMillisecond(), 123L);

    // Free offHeapBuffer
    // delete row->getSegments()[0]->getAll();
    // // Free the memory segment
    // delete row->getSegments()[0];
    // // Free the array storing the memory segments
    // delete row->getSegments();
    // Finally, free the row itself
    delete row;
}

TEST(BinaryRowDataTest, DISABLED_TimestampGetNonCompactTime)
{
    int precision = 4;
    int arity = 2;
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    row->setLong(0, 123L);
    row->setInt(1, 456);

    TimestampData timestamp = row->getTimestampPrecise(0);

    // Check values are get correctly
    EXPECT_EQ(timestamp.getMillisecond(), 123L);
    EXPECT_EQ(timestamp.getNanoOfMillisecond(), 456);

    // Free offHeapBuffer
    // delete row->getSegments()[0]->getAll();
    // // Free the memory segment
    // delete row->getSegments()[0];
    // // Free the array storing the memory segments
    // delete row->getSegments();
    // Finally, free the row itself
    delete row;
}

TEST(BinaryRowDataTest, SetGetStringView)
{
    int arity = 4;
    std::string_view sv0 = "hello";
    std::string_view sv1 = "hellohello_worldworld_abcdefg";
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    row->setLong(0, 42);
    row->setStringView(1, sv0);
    row->setInt(2, 42);
    row->setStringView(3, sv1);

    EXPECT_EQ(*row->getLong(0), 42);
    EXPECT_EQ(row->getStringView(1), sv0);
    EXPECT_EQ(*row->getInt(2), 42);
    EXPECT_EQ(row->getStringView(3), sv1);
}

TEST(BinaryRowDataTest, BinaryRowWriterReusesVariableLengthStorageWithoutStalePadding)
{
    BinaryRowData row(1);
    BinaryRowWriter writer(&row);

    writer.writeString(0, "abcdefghijklmnop");
    writer.complete();

    writer.reset();
    constexpr std::string_view value = "123456789";
    writer.writeString(0, value);
    writer.complete();

    EXPECT_EQ(row.getStringView(0), value);
    const int variableOffset = row.getFixedLengthPartSize();
    for (int i = static_cast<int>(value.size()); i < 16; ++i) {
        EXPECT_EQ(row.getSegment()[variableOffset + i], 0);
    }

    const int expectedHash = row.hashCodeFast();
    writer.reset();
    writer.writeString(0, value);
    writer.complete();
    EXPECT_EQ(row.hashCodeFast(), expectedHash);
}

TEST(BinaryRowDataTest, RawValueLengthsUseVariableLengthEncoding)
{
    for (const size_t length : {size_t(0), size_t(1), size_t(7), size_t(8)}) {
        BinaryRowData row(1);
        BinaryRowWriter writer(&row);
        std::vector<uint8_t> bytes(length, 0x5A);
        writer.writeRawValue(0, bytes.data(), bytes.size());
        writer.complete();

        ASSERT_FALSE(row.isNullAt(0));
        auto* raw = static_cast<BinaryRawValueData*>(row.getRawValue(0));
        ASSERT_NE(raw, nullptr);
        EXPECT_EQ(raw->size(), length);
        EXPECT_EQ(raw->toBytes(), bytes);
    }
}

TEST(BinaryRowDataTest, RawValueNullAndNonZeroSectionOffset)
{
    BinaryRowData nullRow(1);
    BinaryRowWriter nullWriter(&nullRow);
    nullWriter.setNullAt(0);
    nullWriter.complete();
    EXPECT_EQ(nullRow.getRawValue(0), nullptr);

    std::vector<uint8_t> buffer(29, 0);
    BinaryRowData row(1);
    row.pointTo(buffer.data(), 5, 24, static_cast<int>(buffer.size()));
    row.setNotNullAt(0);
    row.setOffsetAndSize(5 + row.getNullBitsSizeInBytes(), 16, 3);
    const uint8_t bytes[] = {0x10, 0x20, 0x30};
    MemorySegmentUtils::put(row.getSegment(), row.getBufferCapacity(), 5 + 16, bytes, 0, 3);
    auto* raw = static_cast<BinaryRawValueData*>(row.getRawValue(0));
    ASSERT_NE(raw, nullptr);
    EXPECT_EQ(raw->toBytes(), std::vector<uint8_t>({0x10, 0x20, 0x30}));
}

TEST(BinaryRowDataTest, RawValueInvalidOffsetAndSizeThrows)
{
    BinaryRowData row(1, 24);
    row.setNotNullAt(0);
    row.setOffsetAndSize(row.getNullBitsSizeInBytes(), 23, 2);
    EXPECT_THROW(row.getRawValue(0), std::runtime_error);
}
