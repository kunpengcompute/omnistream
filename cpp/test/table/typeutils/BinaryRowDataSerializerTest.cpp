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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>

#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

TEST(BinaryRowDataSerializerTest, DeserializesRowsLargerThanInitialReuseBuffer)
{
    constexpr int rowSize = 4096;
    auto row = std::make_unique<BinaryRowData>(1);
    auto* rowBytes = new uint8_t[rowSize];
    for (int i = 0; i < rowSize; ++i) {
        rowBytes[i] = static_cast<uint8_t>(i % 251);
    }
    row->own(rowBytes, 0, rowSize, rowSize);

    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    BinaryRowDataSerializer serializer(1);
    serializer.serialize(row.get(), output);

    DataInputDeserializer input(output.getData(), output.getPosition(), 0);
    auto* restored = static_cast<BinaryRowData*>(serializer.deserialize(input));

    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(restored->getSizeInBytes(), rowSize);
    EXPECT_GE(restored->getBufferCapacity(), rowSize);
    EXPECT_TRUE(std::equal(rowBytes, rowBytes + rowSize, restored->getSegment()));
}

namespace {
// 将 int 按大端序写入 4 字节缓冲区，与 DataOutputSerializer::writeInt 的编码一致。
void writeBigEndianInt(uint8_t* buffer, int value)
{
    const auto encoded = static_cast<uint32_t>(value);
    buffer[0] = static_cast<uint8_t>((encoded >> 24) & 0xFF);
    buffer[1] = static_cast<uint8_t>((encoded >> 16) & 0xFF);
    buffer[2] = static_cast<uint8_t>((encoded >> 8) & 0xFF);
    buffer[3] = static_cast<uint8_t>(encoded & 0xFF);
}

class InputWithoutRemaining : public DataInputDeserializer {
public:
    using DataInputDeserializer::DataInputDeserializer;

    int remaining() const override
    {
        return DataInputView::remaining();
    }
};
} // namespace

TEST(BinaryRowDataSerializerTest, RejectsRowLengthExceedingStaticLimit)
{
    // INT_MAX（0x7FFFFFFF）超过 64MB 静态上限，必须被拒绝而不是触发 ~2GB 内存申请。
    uint8_t buffer[4];
    writeBigEndianInt(buffer, std::numeric_limits<int>::max());

    InputWithoutRemaining input(buffer, sizeof(buffer), 0);
    BinaryRowDataSerializer serializer(1);
    EXPECT_THROW(serializer.deserialize(input), std::runtime_error);
}

TEST(BinaryRowDataSerializerTest, RejectsRowLengthExceedingRemainingBytes)
{
    // 长度前缀声明 1000 字节，但输入流只剩 0 字节（4 字节长度前缀已被读取），
    // 必须被动态限制拒绝。
    uint8_t buffer[4];
    writeBigEndianInt(buffer, 1000);

    DataInputDeserializer input(buffer, sizeof(buffer), 0);
    BinaryRowDataSerializer serializer(1);
    EXPECT_THROW(serializer.deserialize(input), std::runtime_error);
}
