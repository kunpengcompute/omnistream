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

#include <array>
#include <cstdint>

#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "table/data/util/ComboIdUtil.h"

using omnistream::ComboId;
using omnistream::ComboIdUtil;

TEST(ComboIdUtilTest, RoundTripsUnsignedBoundaryValues)
{
    constexpr std::array<ComboId, 5> values = {
        0,
        1,
        0x7FFFFFFFFFFFFFFFULL,
        0x8000000000000000ULL,
        omnistream::INVALID_COMBO_ID,
    };

    for (ComboId expected : values) {
        DataOutputSerializer output;
        OutputBufferStatus outputStatus;
        output.setBackendBuffer(&outputStatus);
        ComboIdUtil::writeComboId(output, expected);

        ASSERT_EQ(output.getPosition(), static_cast<int>(sizeof(ComboId)));
        DataInputDeserializer input(output.getData(), output.getPosition(), 0);
        EXPECT_EQ(ComboIdUtil::readComboId(input), expected);
        EXPECT_EQ(input.Available(), 0);
    }
}

TEST(ComboIdUtilTest, WritesStableBigEndianLayout)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus;
    output.setBackendBuffer(&outputStatus);
    ComboIdUtil::writeComboId(output, 0x0123456789ABCDEFULL);

    constexpr std::array<uint8_t, sizeof(ComboId)> expected = {0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF};
    ASSERT_EQ(output.getPosition(), static_cast<int>(expected.size()));
    for (int index = 0; index < output.getPosition(); ++index) {
        EXPECT_EQ(output.getData()[index], expected[static_cast<size_t>(index)]);
    }
}
