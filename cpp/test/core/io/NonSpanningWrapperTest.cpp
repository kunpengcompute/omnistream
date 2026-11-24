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

#include <gtest/gtest.h>
#include "runtime/io/network/api/serialization/NonSpanningWrapper.h"
#include "runtime/plugable/SerializationDelegate.h"
#include "core/utils/utils.h"
#include <cstdint>
#include <cstring>

using namespace std;

class MockIOReadableWritable : public IOReadableWritable {
public:
    void write(DataOutputSerializer& out) override
    {
        // Mock implementation
    }

    void read(DataInputView& in) override
    {
        // Mock implementation
    }
};

TEST(NonSpanningWrapperTest, Constructor)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    EXPECT_EQ(wrapper.remaining(), 0);
    EXPECT_FALSE(wrapper.hasRemaining());
}

TEST(NonSpanningWrapperTest, InitializeFromMemoryBuffer)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10] = {0};
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    EXPECT_EQ(wrapper.remaining(), 10);
    EXPECT_TRUE(wrapper.hasRemaining());
    EXPECT_EQ(wrapper.remaining(), 10);
}

TEST(NonSpanningWrapperTest, ReadByte_OutOfBounds)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[0];
    wrapper.initializeFromMemoryBuffer(buffer, 0);
    EXPECT_THROW(wrapper.readByte(), std::logic_error);
}

TEST(NonSpanningWrapperTest, ReadByte_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[1] = {0xA5};
    wrapper.initializeFromMemoryBuffer(buffer, 1);
    EXPECT_EQ(wrapper.readByte(), 0xA5);
}

TEST(NonSpanningWrapperTest, ReadInt_OutOfBounds)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[3];
    wrapper.initializeFromMemoryBuffer(buffer, 3);
    EXPECT_THROW(wrapper.readInt(), std::logic_error);
}

TEST(NonSpanningWrapperTest, ReadInt_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[4] = {0x12, 0x34, 0x56, 0x78};
    int expected = 0x12345678;
    wrapper.initializeFromMemoryBuffer(buffer, 4);
    EXPECT_EQ(wrapper.readInt(), expected);
}

TEST(NonSpanningWrapperTest, HasCompleteLength)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10];
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    EXPECT_TRUE(wrapper.hasCompleteLength());
}

TEST(NonSpanningWrapperTest, CanReadRecord)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10];
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    EXPECT_TRUE(wrapper.canReadRecord(5));
    EXPECT_TRUE(wrapper.canReadRecord(10));
    EXPECT_FALSE(wrapper.canReadRecord(11));
}

TEST(NonSpanningWrapperTest, TransferTo_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10] = {0};
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    ByteBuffer dst(10);
    EXPECT_NO_THROW(wrapper.transferTo(dst));
    EXPECT_EQ(wrapper.remaining(), 0);
}

TEST(NonSpanningWrapperTest, TransferTo_BufferOverflow)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10] = {0};
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    ByteBuffer dst(5);
    EXPECT_THROW(wrapper.transferTo(dst), std::logic_error);
}

TEST(NonSpanningWrapperTest, Clear)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10] = {0};
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    wrapper.clear();
    EXPECT_EQ(wrapper.remaining(), 0);
    EXPECT_FALSE(wrapper.hasRemaining());
}

TEST(NonSpanningWrapperTest, CopyContentTo_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    uint8_t dst[10] = {0};
    EXPECT_EQ(wrapper.copyContentTo(dst), 10);
    EXPECT_EQ(memcmp(dst, buffer, 10), 0);
}

TEST(NonSpanningWrapperTest, ReadUnsignedByte_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[1] = {0xFF};
    wrapper.initializeFromMemoryBuffer(buffer, 1);
    EXPECT_EQ(wrapper.readUnsignedByte(), 0xFF);
}

TEST(NonSpanningWrapperTest, ReadLong_OutOfBounds)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[7];
    wrapper.initializeFromMemoryBuffer(buffer, 7);
    EXPECT_THROW(wrapper.readLong(), std::logic_error);
}

TEST(NonSpanningWrapperTest, ReadLong_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    int64_t val = 123l;
    uint8_t* data = reinterpret_cast<uint8_t*>(&val);
    wrapper.initializeFromMemoryBuffer(data, 8);
    EXPECT_EQ(wrapper.readLong(), __builtin_bswap64(val));
}

TEST(NonSpanningWrapperTest, ReadFully_Valid)
{
    omnistream::datastream::NonSpanningWrapper wrapper;
    uint8_t buffer[10] = {0x01, 0x02, 0x03, 0x04, 0x05};
    wrapper.initializeFromMemoryBuffer(buffer, 10);
    uint8_t b[5];
    wrapper.readFully(b, 5, 0, 5);
    EXPECT_EQ(memcmp(b, buffer, 5), 0);
}
