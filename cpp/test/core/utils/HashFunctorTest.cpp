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
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/utils/HashFunctor.h"

using omnistream::utils::Fnv1a64Hash;

static_assert(std::is_same_v<decltype(Fnv1a64Hash{}(uint64_t{})), std::size_t>);
static_assert(noexcept(Fnv1a64Hash{}(uint64_t{})));
static_assert(Fnv1a64Hash{}(0ULL) == static_cast<std::size_t>(0xa8c7f832281a39c5ULL));
static_assert(std::is_same_v<decltype(Fnv1a64Hash{}(static_cast<const int8_t*>(nullptr), std::size_t{})), std::size_t>);
static_assert(noexcept(Fnv1a64Hash{}(static_cast<const int8_t*>(nullptr), std::size_t{})));
static_assert(std::is_same_v<decltype(Fnv1a64Hash{}(std::declval<const std::vector<int8_t>&>())), std::size_t>);
static_assert(noexcept(Fnv1a64Hash{}(std::declval<const std::vector<int8_t>&>())));

// 标准 FNV-1a 64-bit 应按低字节到高字节处理完整 uint64_t，并使用标准 offset basis。
TEST(HashFunctorTest, Fnv1a64MatchesFixedUint64Vectors)
{
    constexpr Fnv1a64Hash hasher;

    EXPECT_EQ(hasher(0x0000000000000000ULL), static_cast<std::size_t>(0xa8c7f832281a39c5ULL));
    EXPECT_EQ(hasher(0x0000000000000001ULL), static_cast<std::size_t>(0x89cd31291d2aefa4ULL));
    EXPECT_EQ(hasher(0x0123456789abcdefULL), static_cast<std::size_t>(0x37eb3f3347761c55ULL));
    EXPECT_EQ(hasher(0x0001000000010000ULL), static_cast<std::size_t>(0xcc34f9ff3a2b56b5ULL));
    EXPECT_EQ(hasher(0x123489abcdef0000ULL), static_cast<std::size_t>(0xa608fa0d9ebce743ULL));
}

// pointer 和 vector 重载应按输入存储顺序处理任意长度字节，并正确处理 int8_t 的高位。
TEST(HashFunctorTest, Fnv1a64MatchesFixedByteSequenceVectors)
{
    constexpr Fnv1a64Hash hasher;
    const std::vector<int8_t> empty;
    const std::vector<int8_t> oneZero{0x00};
    const std::vector<int8_t> signedBytes{
        static_cast<int8_t>(0x80), static_cast<int8_t>(0xff), static_cast<int8_t>(0x01), static_cast<int8_t>(0x7f)};
    const std::vector<int8_t> nineBytes{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

    EXPECT_EQ(hasher(nullptr, 0), static_cast<std::size_t>(0xcbf29ce484222325ULL));
    EXPECT_EQ(hasher(empty), static_cast<std::size_t>(0xcbf29ce484222325ULL));
    EXPECT_EQ(hasher(oneZero.data(), oneZero.size()), static_cast<std::size_t>(0xaf63bd4c8601b7dfULL));
    EXPECT_EQ(hasher(oneZero), static_cast<std::size_t>(0xaf63bd4c8601b7dfULL));
    EXPECT_EQ(hasher(signedBytes.data(), signedBytes.size()), static_cast<std::size_t>(0x7d86f4abf796a6c4ULL));
    EXPECT_EQ(hasher(signedBytes), static_cast<std::size_t>(0x7d86f4abf796a6c4ULL));
    EXPECT_EQ(hasher(nineBytes.data(), nineBytes.size()), static_cast<std::size_t>(0xb11d013568a3b7cfULL));
    EXPECT_EQ(hasher(nineBytes), static_cast<std::size_t>(0xb11d013568a3b7cfULL));
}

// uint64_t 重载应与同一数值的 8-byte little-endian 序列产生完全相同的结果。
TEST(HashFunctorTest, Uint64MatchesEquivalentLittleEndianByteSequence)
{
    constexpr Fnv1a64Hash hasher;
    constexpr uint64_t value = 0x0123456789abcdefULL;
    constexpr std::array<int8_t, sizeof(value)> littleEndianBytes{
        static_cast<int8_t>(0xef),
        static_cast<int8_t>(0xcd),
        static_cast<int8_t>(0xab),
        static_cast<int8_t>(0x89),
        static_cast<int8_t>(0x67),
        static_cast<int8_t>(0x45),
        static_cast<int8_t>(0x23),
        static_cast<int8_t>(0x01),
    };

    EXPECT_EQ(hasher(value), static_cast<std::size_t>(0x37eb3f3347761c55ULL));
    EXPECT_EQ(
        hasher(littleEndianBytes.data(), littleEndianBytes.size()), static_cast<std::size_t>(0x37eb3f3347761c55ULL));
}

// 对低 16 位恒为 0 的代表性 VectorBatchId，FNV-1a 的低 32 位结果应两两不同。
TEST(HashFunctorTest, StructuredVectorBatchIdsDoNotRetainIdenticalLowBits)
{
    constexpr std::array<uint64_t, 4> batchIds{
        0x0001000000010000ULL,
        0x0001000000020000ULL,
        0x0002000000010000ULL,
        0x123489abcdef0000ULL,
    };
    constexpr uint64_t low32Mask = 0xffffffffULL;
    constexpr Fnv1a64Hash fnv1a;

    for (std::size_t left = 0; left < batchIds.size(); ++left) {
        for (std::size_t right = left + 1; right < batchIds.size(); ++right) {
            EXPECT_NE(fnv1a(batchIds[left]) & low32Mask, fnv1a(batchIds[right]) & low32Mask);
        }
    }
}
