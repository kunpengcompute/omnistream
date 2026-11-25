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

#include <cstdint>
#include <vector>

#include "../../../../core/utils/SVEUtil.h"

TEST(SVEUtilTest, CopyU8ArrayByPositions)
{
    constexpr int32_t sourceSize = 521;
    constexpr int32_t resultSize = 257;
    std::vector<uint8_t> source(sourceSize);
    for (int32_t i = 0; i < sourceSize; ++i) {
        source[i] = static_cast<uint8_t>((i * 37 + 11) % 256);
    }

    std::vector<int32_t> positions(resultSize);
    for (int32_t i = 0; i < resultSize; ++i) {
        positions[i] = (i * 83 + 19) % sourceSize;
    }
    positions[1] = positions[0];
    positions[resultSize - 1] = sourceSize - 1;

    std::vector<uint8_t> target(resultSize, 0);
    omnistream::SVEUtil::copyU8ArrayByIndices(source.data(), target.data(), positions);

    for (int32_t i = 0; i < resultSize; ++i) {
        EXPECT_EQ(target[i], source[positions[i]]) << "Unexpected value at result position " << i;
    }
}

TEST(SVEUtilTest, CopyI64ArrayByIndices)
{
    constexpr int32_t sourceSize = 521;
    constexpr int32_t resultSize = 257;
    std::vector<int64_t> source(sourceSize);
    for (int32_t i = 0; i < sourceSize; ++i) {
        source[i] = 10000000000L + static_cast<int64_t>(i) * 97L;
    }

    std::vector<int32_t> indices(resultSize);
    for (int32_t i = 0; i < resultSize; ++i) {
        indices[i] = (i * 89 + 23) % sourceSize;
    }
    indices[1] = indices[0];
    indices[resultSize - 1] = sourceSize - 1;

    std::vector<int64_t> target(resultSize, 0);
    omnistream::SVEUtil::copyI64ArrayByIndices(source.data(), target.data(), indices);

    for (int32_t i = 0; i < resultSize; ++i) {
        EXPECT_EQ(target[i], source[indices[i]]) << "Unexpected value at result position " << i;
    }
}
