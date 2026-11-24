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
#include "runtime/state/UUID.h"
#include <unordered_set>

TEST(UUIDTest, RandomUUID_UniqueAndFormat)
{
    std::unordered_set<std::string> seen;
    // Generate 100 UUIDs to ensure uniqueness and correct format
    for (int i = 0; i < 100; ++i) {
        UUID uuid = UUID::randomUUID();
        std::string str = uuid.ToString();
        ASSERT_EQ(str.size(), 36);
        ASSERT_EQ(str[8], '-');
        ASSERT_EQ(str[13], '-');
        ASSERT_EQ(str[18], '-');
        ASSERT_EQ(str[23], '-');
        ASSERT_EQ(seen.count(str), 0);
        seen.insert(str);
    }
}

TEST(UUIDTest, ToStringAndFromStringConsistency)
{
    for (int i = 0; i < 10; ++i) {
        UUID uuid = UUID::randomUUID();
        std::string str = uuid.ToString();
        UUID parsed = UUID::FromString(str);
        ASSERT_EQ(uuid, parsed);
    }
}

TEST(UUIDTest, OperatorEqualsNotEquals)
{
    UUID a = UUID::randomUUID();
    UUID b = UUID::FromString(a.ToString());
    UUID c = UUID::randomUUID();
    ASSERT_EQ(a, b);
    ASSERT_NE(a, c);
    ASSERT_TRUE(a == b);
    ASSERT_TRUE(a != c);
}

TEST(UUIDTest, OperatorLess)
{
    UUID a(1, 2);
    UUID b(1, 3);
    UUID c(2, 0);
    ASSERT_TRUE(a < b);
    ASSERT_TRUE(b < c);
    ASSERT_TRUE(a < c);
}

TEST(UUIDTest, HashWorksInUnorderedSet)
{
    UUID a = UUID::randomUUID();
    UUID b = UUID::FromString(a.ToString());
    UUID c = UUID::randomUUID();

    std::unordered_set<UUID> uset;
    uset.insert(a);
    uset.insert(c);
    ASSERT_EQ(uset.count(b), 1);
    ASSERT_EQ(uset.count(c), 1);
}

TEST(UUIDTest, InvalidFromStringThrows)
{
    ASSERT_THROW(UUID::FromString("not-a-uuid"), std::invalid_argument);
    ASSERT_THROW(UUID::FromString("000000000000000000000000000000000000"), std::invalid_argument);      // wrong length
    ASSERT_THROW(UUID::FromString("00000000-0000-0000-0000-000000000000-0000"), std::invalid_argument); // too long
}
