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
#include "core/memory/MemorySegmentUtils.h"
#include <vector>
#include <iostream>
#include <cstring>

// 自定义测试类
class MemorySegmentUtilsTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        // 初始化缓冲区
        buffer1 = new uint8_t[1024];
        buffer2 = new uint8_t[1024];
        std::memset(buffer1, 0, 1024);
        std::memset(buffer2, 0, 1024);
    }

    void TearDown() override
    {
        // 释放缓冲区
        delete[] buffer1;
        delete[] buffer2;
    }

    uint8_t* buffer1;
    uint8_t* buffer2;
};

// 测试 getAll 函数
TEST_F(MemorySegmentUtilsTest, GetAllTest)
{
    EXPECT_EQ(MemorySegmentUtils::getAll(buffer1), buffer1);
}

// 测试 get 函数
TEST_F(MemorySegmentUtilsTest, GetTest)
{
    for (int i = 0; i < 10; ++i) {
        buffer1[i] = static_cast<uint8_t>(i);
    }
    EXPECT_EQ(MemorySegmentUtils::get(buffer1, 10, 0), 0);
    EXPECT_EQ(MemorySegmentUtils::get(buffer1, 10, 9), 9);
}

// 测试 get 函数 - 索引越界
TEST_F(MemorySegmentUtilsTest, GetIndexOutOfBoundsTest)
{
    EXPECT_THROW({ MemorySegmentUtils::get(buffer1, 10, 10); }, std::logic_error);

    EXPECT_THROW({ MemorySegmentUtils::get(buffer1, 10, -1); }, std::logic_error);
}

// 测试 put 函数
TEST_F(MemorySegmentUtilsTest, PutTest)
{
    MemorySegmentUtils::put(buffer1, 10, 0, 255);
    MemorySegmentUtils::put(buffer1, 10, 9, 128);
    EXPECT_EQ(buffer1[0], 255);
    EXPECT_EQ(buffer1[9], 128);
}

// 测试 put 函数 - 索引越界
TEST_F(MemorySegmentUtilsTest, PutIndexOutOfBoundsTest)
{
    EXPECT_THROW({ MemorySegmentUtils::put(buffer1, 10, 10, 255); }, std::logic_error);

    EXPECT_THROW({ MemorySegmentUtils::put(buffer1, 10, -1, 255); }, std::logic_error);
}

// 测试 put 函数 - 复制数据
TEST_F(MemorySegmentUtilsTest, PutCopyDataTest)
{
    uint8_t src[] = {1, 2, 3, 4, 5};
    MemorySegmentUtils::put(buffer1, 10, 0, src, 0, 5);
    EXPECT_EQ(buffer1[0], 1);
    EXPECT_EQ(buffer1[1], 2);
    EXPECT_EQ(buffer1[2], 3);
    EXPECT_EQ(buffer1[3], 4);
    EXPECT_EQ(buffer1[4], 5);
}

// 测试 put 函数 - 复制数据 - 索引越界
TEST_F(MemorySegmentUtilsTest, PutCopyDataIndexOutOfBoundsTest)
{
    uint8_t src[] = {1, 2, 3, 4, 5};
    EXPECT_THROW({ MemorySegmentUtils::put(buffer1, 10, 6, src, 0, 5); }, std::out_of_range);

    EXPECT_THROW({ MemorySegmentUtils::put(buffer1, 10, -1, src, 0, 5); }, std::out_of_range);
}

// 测试 getSize 函数
TEST_F(MemorySegmentUtilsTest, GetSizeTest)
{
    EXPECT_EQ(MemorySegmentUtils::getSize(42), 42);
}

// 测试 get 函数 - 复制数据
TEST_F(MemorySegmentUtilsTest, GetCopyDataTest)
{
    uint8_t dst[10] = {0};
    buffer1[0] = 1;
    buffer1[1] = 2;
    buffer1[2] = 3;
    buffer1[3] = 4;
    buffer1[4] = 5;
    MemorySegmentUtils::get(buffer1, 10, 0, dst, 0, 5);
    EXPECT_EQ(dst[0], 1);
    EXPECT_EQ(dst[1], 2);
    EXPECT_EQ(dst[2], 3);
    EXPECT_EQ(dst[3], 4);
    EXPECT_EQ(dst[4], 5);
}

// 测试 get 函数 - 复制数据 - 索引越界
TEST_F(MemorySegmentUtilsTest, GetCopyDataIndexOutOfBoundsTest)
{
    uint8_t dst[10] = {0};
    EXPECT_THROW({ MemorySegmentUtils::get(buffer1, 10, 6, dst, 0, 5); }, std::out_of_range);

    EXPECT_THROW({ MemorySegmentUtils::get(buffer1, 10, -1, dst, 0, 5); }, std::out_of_range);
}

// 测试 size 函数
TEST_F(MemorySegmentUtilsTest, SizeTest)
{
    EXPECT_EQ(MemorySegmentUtils::size(42), 42);
}

// 测试 getLong 函数
TEST_F(MemorySegmentUtilsTest, GetLongTest)
{
    long value = 123456789;
    std::memcpy(buffer1, &value, sizeof(long));
    EXPECT_EQ(*(MemorySegmentUtils::getLong(buffer1, 1024, 0)), value);
}

// 测试 getLong 函数 - 索引越界
TEST_F(MemorySegmentUtilsTest, GetLongIndexOutOfBoundsTest)
{
    EXPECT_THROW({ MemorySegmentUtils::getLong(buffer1, 1024, 1024 - sizeof(long) + 1); }, std::logic_error);

    EXPECT_THROW({ MemorySegmentUtils::getLong(buffer1, 1024, -1); }, std::logic_error);
}

// 测试 putLong 函数
TEST_F(MemorySegmentUtilsTest, PutLongTest)
{
    long value = 123456789;
    MemorySegmentUtils::putLong(buffer1, 1024, 0, value);
    EXPECT_EQ(*(reinterpret_cast<long*>(buffer1)), value);
}

// 测试 putLong 函数 - 索引越界
TEST_F(MemorySegmentUtilsTest, PutLongIndexOutOfBoundsTest)
{
    long value = 123456789;
    EXPECT_THROW({ MemorySegmentUtils::putLong(buffer1, 1024, 1024 - sizeof(long) + 1, value); }, std::logic_error);

    EXPECT_THROW({ MemorySegmentUtils::putLong(buffer1, 1024, -1, value); }, std::logic_error);
}

// 测试 getInt 函数
TEST_F(MemorySegmentUtilsTest, GetIntTest)
{
    int value = 12345;
    std::memcpy(buffer1, &value, sizeof(int));
    EXPECT_EQ(*(MemorySegmentUtils::getInt(buffer1, 1024, 0)), value);
}

// 测试 getInt 函数 - 索引越界
TEST_F(MemorySegmentUtilsTest, GetIntIndexOutOfBoundsTest)
{
    EXPECT_THROW({ MemorySegmentUtils::getInt(buffer1, 1024, 1024 - sizeof(int) + 1); }, std::logic_error);

    EXPECT_THROW({ MemorySegmentUtils::getInt(buffer1, 1024, -1); }, std::logic_error);
}

// 测试 putInt 函数
TEST_F(MemorySegmentUtilsTest, PutIntTest)
{
    int value = 12345;
    MemorySegmentUtils::putInt(buffer1, 1024, 0, value);
    EXPECT_EQ(*(reinterpret_cast<int*>(buffer1)), value);
}

// 测试 putInt 函数 - 索引越界
TEST_F(MemorySegmentUtilsTest, PutIntIndexOutOfBoundsTest)
{
    int value = 12345;
    EXPECT_THROW({ MemorySegmentUtils::putInt(buffer1, 1024, 1024 - sizeof(int) + 1, value); }, std::logic_error);

    EXPECT_THROW({ MemorySegmentUtils::putInt(buffer1, 1024, -1, value); }, std::logic_error);
}

// 测试 equalTo 函数
TEST_F(MemorySegmentUtilsTest, EqualToTest)
{
    for (int i = 0; i < 10; ++i) {
        buffer1[i] = static_cast<uint8_t>(i);
        buffer2[i] = static_cast<uint8_t>(i);
    }
    EXPECT_TRUE(MemorySegmentUtils::equalTo(buffer1, buffer2, 0, 0, 10));
    EXPECT_FALSE(MemorySegmentUtils::equalTo(buffer1, buffer2, 0, 1, 10));
}

// 测试 byteIndex 函数
TEST_F(MemorySegmentUtilsTest, ByteIndexTest)
{
    EXPECT_EQ(MemorySegmentUtils::byteIndex(0), 0);
    EXPECT_EQ(MemorySegmentUtils::byteIndex(3), 0);
    EXPECT_EQ(MemorySegmentUtils::byteIndex(4), 0);
    EXPECT_EQ(MemorySegmentUtils::byteIndex(7), 0);
    EXPECT_EQ(MemorySegmentUtils::byteIndex(8), 1);
}

// 测试 bitUnSet 函数
TEST_F(MemorySegmentUtilsTest, BitUnSetTest)
{
    buffer1[0] = 0xFF;
    MemorySegmentUtils::bitUnSet(buffer1, 1024, 0, 0);
    EXPECT_EQ(buffer1[0], 0xFE);
    MemorySegmentUtils::bitUnSet(buffer1, 1024, 0, 7);
    EXPECT_EQ(buffer1[0], 0x7E);
}

// 测试 bitSet 函数
TEST_F(MemorySegmentUtilsTest, BitSetTest)
{
    buffer1[0] = 0x00;
    MemorySegmentUtils::bitSet(buffer1, 1024, 0, 0);
    EXPECT_EQ(buffer1[0], 0x01);
}

// 测试 copy 函数
TEST_F(MemorySegmentUtilsTest, CopyTest)
{
    uint8_t src[] = {1, 2, 3, 4, 5};
    MemorySegmentUtils::copy(src, 0, buffer1, 0, 5);
    EXPECT_EQ(buffer1[0], 1);
    EXPECT_EQ(buffer1[1], 2);
    EXPECT_EQ(buffer1[2], 3);
    EXPECT_EQ(buffer1[3], 4);
    EXPECT_EQ(buffer1[4], 5);
}
