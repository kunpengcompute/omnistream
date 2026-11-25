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

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

#include "core/memory/DataOutputSerializer.h"

TEST(DataOutputSerializerTest, WritesBigEndianPrimitivesAtUnalignedOffsets)
{
    std::array<uint8_t, 23> buffer{};
    OutputBufferStatus outputStatus;
    outputStatus.outputBuffer_ = reinterpret_cast<uintptr_t>(buffer.data());
    outputStatus.capacity_ = static_cast<int32_t>(buffer.size());
    outputStatus.ownership = 0;
    DataOutputSerializer output;
    output.setBackendBuffer(&outputStatus);

    output.writeByte(0xAA);
    output.writeShort(0x0102);
    output.writeInt(0x03040506);
    output.writeLong(0x0708090A0B0C0D0E);
    output.writeRecordTimestamp(0x0F10111213141516);

    const std::array<uint8_t, 23> expected = {0xAA, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
                                              0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16};
    EXPECT_EQ(buffer, expected);
}

TEST(DataOutputSerializerTest, WritesFrameLengthAtUnalignedOffset)
{
    std::array<uint8_t, 5> buffer{};
    OutputBufferStatus outputStatus;
    outputStatus.outputBuffer_ = reinterpret_cast<uintptr_t>(buffer.data());
    outputStatus.capacity_ = static_cast<int32_t>(buffer.size());
    outputStatus.ownership = 0;
    DataOutputSerializer output;
    output.setBackendBuffer(&outputStatus);

    output.writeByte(0xFF);
    output.writeIntUnsafe(0x01020304, 1);

    const std::array<uint8_t, 5> expected = {0xFF, 0x01, 0x02, 0x03, 0x04};
    EXPECT_EQ(buffer, expected);
}
