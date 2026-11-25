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

#include "gtest/gtest.h"
#include "table/data/binary/MurmurHashUtils.h"
#include <cstdint>
#include <cstring>

// Test default hash functions
TEST(MurmurHashUtilsTest, DefaultSeedHash)
{
    const char* testString = "test";
    int hash1 = MurmurHashUtils::hashBytes(reinterpret_cast<const uint8_t*>(testString), 0, 4);
    int hash2 = MurmurHashUtils::hashUnsafeBytes(testString, 0, 4);
    EXPECT_EQ(hash1, hash2);
}

// Test hashBytesByWords and hashUnsafeBytesByWords
TEST(MurmurHashUtilsTest, HashByWords)
{
    const char* testString = "abcdefgh"; // 8 bytes, exactly 2 words
    int hash1 = MurmurHashUtils::hashBytesByWords(reinterpret_cast<const uint8_t*>(testString), 0, 8);
    int hash2 = MurmurHashUtils::hashUnsafeBytesByWords(testString, 0, 8);
    EXPECT_EQ(hash1, hash2);
}

// Test with different offsets
TEST(MurmurHashUtilsTest, HashWithOffsets)
{
    const char* testString = "abcdefghij"; // 10 bytes, 2 full words + 2 bytes
    int hash1 = MurmurHashUtils::hashBytes(reinterpret_cast<const uint8_t*>(testString), 2, 8);
    int hash2 = MurmurHashUtils::hashUnsafeBytes(testString, 2, 8);
    EXPECT_EQ(hash1, hash2);
}

// Ensure correct handling of non-4 byte aligned lengths
TEST(MurmurHashUtilsTest, UnevenLengths)
{
    const char* testString = "abcdef"; // 6 bytes, not aligned to 4-byte boundary
    int hash1 = MurmurHashUtils::hashBytes(reinterpret_cast<const uint8_t*>(testString), 0, 6);
    int hash2 = MurmurHashUtils::hashUnsafeBytes(testString, 0, 6);
    EXPECT_EQ(hash1, hash2);
}
