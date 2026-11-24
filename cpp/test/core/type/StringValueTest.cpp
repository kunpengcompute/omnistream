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
#include "core/type/StringValue.h"
#include "memory/DataOutputSerializer.h"
#include "runtime/io/network/api/serialization/NonSpanningWrapper.h"
#include <string>
#include <cstring>
#include <codecvt>
#include <locale>

static const int HIGH_BIT = 0x1 << 7;
static const int HIGH_BIT14 = 0x1 << 14;
static const int HIGH_BIT21 = 0x1 << 21;
static const int HIGH_BIT28 = 0x1 << 28;

// 测试用例定义
TEST(StringValueTest, WriteAndReadTest)
{
    StringValue stringValue;
    std::u32string value(U"Hello, World!");
    stringValue.setValue(value);

    // 写入数据到 DataOutputSerializer
    DataOutputSerializer out{};
    uint8_t* data = reinterpret_cast<uint8_t*>(malloc(100));
    out.setBackendBuffer(data, 100);
    stringValue.write(out);

    // 读取数据从 DataInputView
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(out.getData(), 100);
    StringValue stringValue2;
    stringValue2.read(in);

    EXPECT_EQ(stringValue.getValue(), stringValue2.getValue());
}

TEST(StringValueTest, WriteAndReadLargeStringTest)
{
    std::string largeString(1000, 'A');
    StringValue stringValue;
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> convert;
    std::u32string u32str = convert.from_bytes(largeString);
    stringValue.setValue(u32str);

    // 写入数据到 DataOutputSerializer
    DataOutputSerializer out{};
    uint8_t* data = reinterpret_cast<uint8_t*>(malloc(100));
    OutputBufferStatus outputBufferStatus_;
    out.setBackendBuffer(&outputBufferStatus_);
    stringValue.write(out);

    // 读取数据从 DataInputView
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(out.getData(), 100);
    StringValue stringValue2;
    stringValue2.read(in);

    EXPECT_EQ(stringValue.getValue(), stringValue2.getValue());
}

TEST(StringValueTest, WriteStringTest)
{
    std::string expectedValue = "Hello, World!";
    String buffer(expectedValue);
    DataOutputSerializer out{};
    OutputBufferStatus outputBufferStatus_;
    out.setBackendBuffer(&outputBufferStatus_);

    StringValue::writeString(&buffer, out);
    uint8_t* data = out.getData();

    // 验证写入的数据是否正确
    size_t len = buffer.getValue().size();
    size_t expectedLen = len + 1; // Length is offset by one

    // 验证长度
    EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen));

    // 验证内容
    for (size_t i = 0; i < len; ++i) {
        EXPECT_EQ(data[i + 1], static_cast<uint8_t>(buffer.getValue()[i]));
    }
}

TEST(StringValueTest, WriteStringLargeTest)
{
    std::string largeString(1000, 'A');
    String buffer(largeString);
    DataOutputSerializer out{};
    uint8_t* data = reinterpret_cast<uint8_t*>(malloc(100));
    OutputBufferStatus outputBufferStatus_;
    out.setBackendBuffer(&outputBufferStatus_);

    StringValue::writeString(&buffer, out);
    data = out.getData();

    // 验证写入的数据是否正确
    size_t len = buffer.getValue().size();
    size_t expectedLen = len + 1; // Length is offset by one

    // 验证长度
    if (expectedLen < HIGH_BIT) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen));
    } else if (expectedLen < HIGH_BIT14) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>(expectedLen >> 7));
    } else if (expectedLen < HIGH_BIT21) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>((expectedLen >> 7) | HIGH_BIT));
        EXPECT_EQ(data[2], static_cast<uint8_t>(expectedLen >> 14));
    } else if (expectedLen < HIGH_BIT28) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>((expectedLen >> 7) | HIGH_BIT));
        EXPECT_EQ(data[2], static_cast<uint8_t>((expectedLen >> 14) | HIGH_BIT));
        EXPECT_EQ(data[3], static_cast<uint8_t>(expectedLen >> 21));
    } else {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>((expectedLen >> 7) | HIGH_BIT));
        EXPECT_EQ(data[2], static_cast<uint8_t>((expectedLen >> 14) | HIGH_BIT));
        EXPECT_EQ(data[3], static_cast<uint8_t>((expectedLen >> 21) | HIGH_BIT));
        EXPECT_EQ(data[4], static_cast<uint8_t>(expectedLen >> 28));
    }
}

TEST(StringValueTest, WriteAndReadStringTest)
{
    std::string expectedValue = "o4测试用字符..............................************************************";
    String* s = new String(expectedValue);

    DataOutputSerializer out{};
    uint8_t* data = reinterpret_cast<uint8_t*>(malloc(100));
    out.setBackendBuffer(data, 100);

    StringValue::writeString(s, out);

    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(out.getData(), 100);

    String* result = new String();
    StringValue::readString(result, in);

    EXPECT_EQ(result->getValue(), s->getValue());
    delete s;
    delete result;
}

TEST(StringValueTest, ReadStringTest)
{
    std::string expectedValue = "Hello, World!";
    // 计算长度并填充到前缀中
    size_t len = expectedValue.size();
    size_t prefixLength =
        len < HIGH_BIT ? 1 : (len < HIGH_BIT14 ? 2 : (len < HIGH_BIT21 ? 3 : (len < HIGH_BIT28 ? 4 : 5)));

    std::vector<uint8_t> data(prefixLength);
    size_t idx = 0;
    if (len < HIGH_BIT) {
        data[idx++] = static_cast<uint8_t>(len + 1);
    } else if (len < HIGH_BIT14) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 7);
    } else if (len < HIGH_BIT21) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 14);
    } else if (len < HIGH_BIT28) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 21);
    } else {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 21) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 28);
    }

    // 添加字符串内容
    data.insert(data.end(), expectedValue.begin(), expectedValue.end());

    auto buffer = new String();
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), 100);

    StringValue::readString(buffer, in);

    EXPECT_EQ(buffer->getValue(), expectedValue);
    delete buffer;
}

TEST(StringValueTest, ReadStringLargeTest)
{
    std::string expectedValue(1000, 'A');
    // 计算长度并填充到前缀中
    size_t len = expectedValue.size();

    int32_t maxLimit = 5 + 3 * len;

    std::vector<uint8_t> data(1000 + maxLimit);

    size_t idx = 0;
    if (len < HIGH_BIT) {
        data[idx++] = static_cast<uint8_t>(len + 1);
    } else if (len < HIGH_BIT14) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 7);
    } else if (len < HIGH_BIT21) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 14);
    } else if (len < HIGH_BIT28) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 21);
    } else {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 21) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 28);
    }

    uint32_t lenToWrite = len + 1;
    for (int i = 0; i < expectedValue.size(); i++) {
        uint32_t c = static_cast<uint32_t>(static_cast<unsigned char>(expectedValue[i]));

        // manual loop unroll, as it performs much better on jdk8
        if (c < HIGH_BIT) {
            data[idx++] = (c & 0xff);
        } else if (c < HIGH_BIT14) {
            data[idx++] = ((c | HIGH_BIT) & 0xff);
            data[idx++] = ((c >> 7) & 0xff);
        } else {
            data[idx++] = ((lenToWrite | HIGH_BIT) & 0xff);
            data[idx++] = ((lenToWrite >> 7 | HIGH_BIT) & 0xff);
            data[idx++] = ((lenToWrite >> 14) & 0xff);
        }
    }

    // 添加字符串内容
    // data.insert(data.end(), expectedValue.begin(), expectedValue.end());

    auto buffer = new String("", idx + 1);
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), idx + 1);

    StringValue::readString(buffer, in);

    EXPECT_EQ(buffer->getValue(), expectedValue);
    delete buffer;
}

TEST(StringValueTest, ReadStringNullTest)
{
    // 创建一个表示空字符串的数据vector（长度为0）
    std::vector<uint8_t> data = {0};

    String buffer("", 1);
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), data.size());

    StringValue::readString(&buffer, in);

    EXPECT_EQ(buffer.getSize(), 1);
}

TEST(StringValueTest, ReadStringReallocateMemoryTest)
{
    std::string expectedValue(1000, 'A');
    // 计算长度并填充到前缀中
    size_t len = expectedValue.size();
    int32_t maxLimit = 5 + 3 * len;

    std::vector<uint8_t> data(1000 + maxLimit);
    size_t idx = 0;
    if (len < HIGH_BIT) {
        data[idx++] = static_cast<uint8_t>(len + 1);
    } else if (len < HIGH_BIT14) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 7);
    } else if (len < HIGH_BIT21) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 14);
    } else if (len < HIGH_BIT28) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 21);
    } else {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 21) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 28);
    }

    uint32_t lenToWrite = len + 1;
    for (int i = 0; i < expectedValue.size(); i++) {
        uint32_t c = static_cast<uint32_t>(static_cast<unsigned char>(expectedValue[i]));

        // manual loop unroll, as it performs much better on jdk8
        if (c < HIGH_BIT) {
            data[idx++] = (c & 0xff);
        } else if (c < HIGH_BIT14) {
            data[idx++] = ((c | HIGH_BIT) & 0xff);
            data[idx++] = ((c >> 7) & 0xff);
        } else {
            data[idx++] = ((lenToWrite | HIGH_BIT) & 0xff);
            data[idx++] = ((lenToWrite >> 7 | HIGH_BIT) & 0xff);
            data[idx++] = ((lenToWrite >> 14) & 0xff);
        }
    }

    auto buffer = new String("", 1); // 初始容量很小，确保需要重新分配内存
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), idx + 1);

    StringValue::readString(buffer, in);

    EXPECT_EQ(buffer->getValue(), expectedValue);
    EXPECT_EQ(buffer->getSize(), len);
    delete buffer;
}
