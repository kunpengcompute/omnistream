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
#include <string>
#include "table/runtime/operators/window/internal/LRUMap.h"

TEST(LRUMapTest, DefaultConstructor)
{
    LRUMap<int, std::string> lru;
    EXPECT_FALSE(lru.get(1).has_value());
}

TEST(LRUMapTest, ConstructorWithCapacity)
{
    LRUMap<int, int> lru(5);
    for (int i = 0; i < 10; ++i) {
        lru.put(i, i * 10);
    }
    EXPECT_FALSE(lru.get(0).has_value());
    EXPECT_FALSE(lru.get(1).has_value());
    EXPECT_FALSE(lru.get(2).has_value());
    EXPECT_FALSE(lru.get(3).has_value());
    EXPECT_FALSE(lru.get(4).has_value());
    EXPECT_TRUE(lru.get(5).has_value());
    EXPECT_EQ(lru.get(5).value(), 50);
    EXPECT_TRUE(lru.get(9).has_value());
    EXPECT_EQ(lru.get(9).value(), 90);
}

TEST(LRUMapTest, PutAndGet)
{
    LRUMap<int, std::string> lru;
    lru.put(1, "one");
    lru.put(2, "two");
    lru.put(3, "three");

    auto val1 = lru.get(1);
    EXPECT_TRUE(val1.has_value());
    EXPECT_EQ(val1.value(), "one");

    auto val2 = lru.get(2);
    EXPECT_TRUE(val2.has_value());
    EXPECT_EQ(val2.value(), "two");

    auto val3 = lru.get(3);
    EXPECT_TRUE(val3.has_value());
    EXPECT_EQ(val3.value(), "three");
}

TEST(LRUMapTest, GetNonExistentKey)
{
    LRUMap<int, int> lru;
    lru.put(1, 100);

    auto val = lru.get(999);
    EXPECT_FALSE(val.has_value());
}

TEST(LRUMapTest, UpdateExistingKey)
{
    LRUMap<int, std::string> lru;
    lru.put(1, "one");
    lru.put(1, "ONE");

    auto val = lru.get(1);
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), "ONE");
}

TEST(LRUMapTest, LRUEviction)
{
    LRUMap<int, int> lru(3);

    lru.put(1, 10);
    lru.put(2, 20);
    lru.put(3, 30);
    lru.put(4, 40);

    EXPECT_FALSE(lru.get(1).has_value());
    EXPECT_TRUE(lru.get(2).has_value());
    EXPECT_TRUE(lru.get(3).has_value());
    EXPECT_TRUE(lru.get(4).has_value());
}

TEST(LRUMapTest, LRUOrderOnGet)
{
    LRUMap<int, int> lru(3);

    lru.put(1, 10);
    lru.put(2, 20);
    lru.put(3, 30);

    lru.get(1);

    lru.put(4, 40);

    EXPECT_TRUE(lru.get(1).has_value());
    EXPECT_FALSE(lru.get(2).has_value());
    EXPECT_TRUE(lru.get(3).has_value());
    EXPECT_TRUE(lru.get(4).has_value());
}

TEST(LRUMapTest, LRUOrderOnPut)
{
    LRUMap<int, int> lru(3);

    lru.put(1, 10);
    lru.put(2, 20);
    lru.put(3, 30);

    lru.put(1, 100);

    lru.put(4, 40);

    EXPECT_TRUE(lru.get(1).has_value());
    EXPECT_EQ(lru.get(1).value(), 100);
    EXPECT_FALSE(lru.get(2).has_value());
    EXPECT_TRUE(lru.get(3).has_value());
    EXPECT_TRUE(lru.get(4).has_value());
}

TEST(LRUMapTest, StringKey)
{
    LRUMap<std::string, int> lru(2);

    lru.put("one", 1);
    lru.put("two", 2);
    lru.put("three", 3);

    EXPECT_FALSE(lru.get("one").has_value());
    EXPECT_TRUE(lru.get("two").has_value());
    EXPECT_EQ(lru.get("two").value(), 2);
    EXPECT_TRUE(lru.get("three").has_value());
    EXPECT_EQ(lru.get("three").value(), 3);
}

TEST(LRUMapTest, MultipleGetUpdatesLRU)
{
    LRUMap<int, int> lru(3);

    lru.put(1, 10);
    lru.put(2, 20);
    lru.put(3, 30);

    lru.get(1);
    lru.get(2);

    lru.put(4, 40);

    EXPECT_TRUE(lru.get(1).has_value());
    EXPECT_TRUE(lru.get(2).has_value());
    EXPECT_FALSE(lru.get(3).has_value());
    EXPECT_TRUE(lru.get(4).has_value());
}

TEST(LRUMapTest, OverwriteDoesNotChangeSize)
{
    LRUMap<int, int> lru(2);

    lru.put(1, 10);
    lru.put(2, 20);
    lru.put(1, 100);
    lru.put(1, 1000);

    EXPECT_TRUE(lru.get(1).has_value());
    EXPECT_EQ(lru.get(1).value(), 1000);
    EXPECT_TRUE(lru.get(2).has_value());
    EXPECT_EQ(lru.get(2).value(), 20);
}

TEST(LRUMapTest, CapacityOne)
{
    LRUMap<int, int> lru(1);

    lru.put(1, 10);
    EXPECT_TRUE(lru.get(1).has_value());
    EXPECT_EQ(lru.get(1).value(), 10);

    lru.put(2, 20);
    EXPECT_FALSE(lru.get(1).has_value());
    EXPECT_TRUE(lru.get(2).has_value());
    EXPECT_EQ(lru.get(2).value(), 20);
}
