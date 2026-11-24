/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include "table/utils/BinaryRowDataSerializationUtils.h"

using omnistream::BinaryRowDataSerializationUtils;

TEST(BinaryRowDataNetworkSerializationTest, RoundTripsNullFieldsAndRowKind)
{
    auto row = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(2));
    row->setLong(0, 42L);
    row->setNullAt(1);
    row->setRowKind(RowKind::UPDATE_AFTER);
    std::vector<uint8_t> bytes(BinaryRowDataSerializationUtils::calculateBinaryRowSerializableSize(row.get()));
    auto* writer = bytes.data();
    BinaryRowDataSerializationUtils::serializeBinaryRowData(row.get(), bytes.size(), writer);
    auto* reader = bytes.data() + sizeof(int8_t);
    auto restored = std::unique_ptr<BinaryRowData>(
        BinaryRowDataSerializationUtils::deserializeBinaryRowData(reader, bytes.size() - sizeof(int8_t)));
    EXPECT_EQ(*restored->getLong(0), 42L);
    EXPECT_TRUE(restored->isNullAt(1));
    EXPECT_EQ(restored->getRowKind(), RowKind::UPDATE_AFTER);
    EXPECT_EQ(reader, writer);
}

TEST(BinaryRowDataNetworkSerializationTest, RejectsTruncatedHeaderWithoutAdvancingCursor)
{
    uint8_t bytes[8] = {};
    for (size_t remaining = 0; remaining < sizeof(bytes); ++remaining) {
        auto* cursor = bytes;
        EXPECT_THROW(
            BinaryRowDataSerializationUtils::deserializeBinaryRowData(cursor, remaining), std::invalid_argument);
        EXPECT_EQ(cursor, bytes);
    }
}

TEST(BinaryRowDataNetworkSerializationTest, RejectsInvalidLengthAndArityBeforeAllocation)
{
    std::array<std::array<int32_t, 2>, 5> headers = {
        {{-1, 8},
         {1, -1},
         {1, 8},
         {1, std::numeric_limits<int32_t>::max()},
         {std::numeric_limits<int32_t>::max(), 16}}};
    uint8_t bytes[24] = {};
    for (const auto& header : headers) {
        std::memcpy(bytes, header.data(), sizeof(int32_t) * 2);
        auto* cursor = bytes;
        EXPECT_THROW(
            BinaryRowDataSerializationUtils::deserializeBinaryRowData(cursor, sizeof(bytes)), std::invalid_argument);
        EXPECT_EQ(cursor, bytes);
    }
    int32_t truncated[2] = {1, 24};
    std::memcpy(bytes, truncated, sizeof(truncated));
    auto* cursor = bytes;
    EXPECT_THROW(
        BinaryRowDataSerializationUtils::deserializeBinaryRowData(cursor, sizeof(bytes)), std::invalid_argument);
    EXPECT_EQ(cursor, bytes);
}

TEST(BinaryRowDataNetworkSerializationTest, RejectsUndersizedOutputBufferWithoutAdvancingCursor)
{
    auto row = std::unique_ptr<BinaryRowData>(BinaryRowData::createBinaryRowDataWithMem(1));
    uint8_t bytes[8] = {};
    auto* cursor = bytes;
    EXPECT_THROW(
        BinaryRowDataSerializationUtils::serializeBinaryRowData(row.get(), sizeof(bytes), cursor),
        std::invalid_argument);
    EXPECT_EQ(cursor, bytes);
}
