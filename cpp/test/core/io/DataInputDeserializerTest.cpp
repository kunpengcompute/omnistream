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
#include "core/memory/DataInputDeserializer.h"
#include <cstdint>
#include <cstring>

using namespace std;

TEST(DataInputDeserializerTest, SetBuffer_InvalidBounds_StartNegative)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    EXPECT_THROW(deserializer.setBuffer(buffer, 10, -1, 10), std::logic_error);
}

TEST(DataInputDeserializerTest, SetBuffer_InvalidBounds_LenNegative)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    EXPECT_THROW(deserializer.setBuffer(buffer, 10, 0, -1), std::logic_error);
}

TEST(DataInputDeserializerTest, SetBuffer_InvalidBounds_OutOfRange)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    EXPECT_THROW(deserializer.setBuffer(buffer, 10, 5, 6), std::logic_error);
}

TEST(DataInputDeserializerTest, SetBuffer_ValidBounds)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    EXPECT_NO_THROW(deserializer.setBuffer(buffer, 10, 0, 5));
}

TEST(DataInputDeserializerTest, ReadByte_Valid)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[1] = {0xA5};
    deserializer.setBuffer(buffer, 1, 0, 1);
    EXPECT_EQ(deserializer.readByte(), 0xA5);
}

TEST(DataInputDeserializerTest, ReadLong_OutOfBounds)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[7];
    deserializer.setBuffer(buffer, 7, 0, 7);
    EXPECT_THROW(deserializer.readLong(), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadLong_Valid)
{
    DataInputDeserializer deserializer;
    int64_t val = 123l;
    uint8_t* buffer = reinterpret_cast<uint8_t*>(&val);
    deserializer.setBuffer(buffer, 8, 0, 8);
    EXPECT_EQ(deserializer.readLong(), __builtin_bswap64(val));
}

TEST(DataInputDeserializerTest, ReadInt_OutOfBounds)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[3];
    deserializer.setBuffer(buffer, 3, 0, 3);
    EXPECT_THROW(deserializer.readInt(), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadInt_Valid)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[4] = {0x12, 0x34, 0x56, 0x78};
    deserializer.setBuffer(buffer, 4, 0, 4);
    int expected = 0x12345678;
    EXPECT_EQ(deserializer.readInt(), expected);
}

TEST(DataInputDeserializerTest, ReadFully_OutOfBounds_StartNegative)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    deserializer.setBuffer(buffer, 10, 0, 5);
    uint8_t b[6];
    EXPECT_THROW(deserializer.readFully(b, 5, -1, 6), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadFully_OutOfBounds_LenNegative)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    deserializer.setBuffer(buffer, 10, 0, 5);
    uint8_t b[5];
    EXPECT_THROW(deserializer.readFully(b, 5, 0, -5), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadFully_OutOfBounds_ArrayIndexOutOfBounds)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    deserializer.setBuffer(buffer, 10, 0, 5);
    uint8_t b[5];
    EXPECT_THROW(deserializer.readFully(b, 5, 6, 1), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadFully_OutOfBounds_EOFException)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10];
    deserializer.setBuffer(buffer, 10, 0, 5);
    uint8_t b[5];
    EXPECT_THROW(deserializer.readFully(b, 5, 0, 6), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadFully_Valid)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[10] = {0x01, 0x02, 0x03, 0x04, 0x05};
    deserializer.setBuffer(buffer, 10, 0, 5);
    uint8_t b[5];
    deserializer.readFully(b, 5, 0, 5);
    EXPECT_EQ(memcmp(b, buffer, 5), 0);
}

TEST(DataInputDeserializerTest, ReadUnsignedByte_OutOfBounds)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[0];
    deserializer.setBuffer(buffer, 0, 0, 0);
    EXPECT_THROW(deserializer.readUnsignedByte(), std::logic_error);
}

TEST(DataInputDeserializerTest, ReadUnsignedByte_Valid)
{
    DataInputDeserializer deserializer;
    uint8_t buffer[1] = {0xFF};
    deserializer.setBuffer(buffer, 1, 0, 1);
    EXPECT_EQ(deserializer.readUnsignedByte(), 0xFF);
}
