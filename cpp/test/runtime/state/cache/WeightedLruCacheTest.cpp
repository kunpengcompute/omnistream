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

#include <memory>
#include <string>

#include "runtime/state/cache/WeightedLruCache.h"

// 缺失 key 查询应返回 nullptr，且不改变缓存大小和权重。
TEST(WeightedLruCacheTest, GetMissingReturnsNull)
{
    WeightedLruCache<int, std::string> cache(10);

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.currentWeight(), 0);
    EXPECT_EQ(cache.maxWeight(), 10);
}

// get 命中后应提升为最近使用，后续淘汰保留该 entry。
TEST(WeightedLruCacheTest, PutAndGetPromotesEntry)
{
    WeightedLruCache<int, std::string> cache(2);

    ASSERT_NE(cache.put(1, std::string("one"), 1), nullptr);
    ASSERT_NE(cache.put(2, std::string("two"), 1), nullptr);

    auto* one = cache.get(1);
    ASSERT_NE(one, nullptr);
    EXPECT_EQ(*one, "one");

    ASSERT_NE(cache.put(3, std::string("three"), 1), nullptr);

    EXPECT_NE(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_NE(cache.get(3), nullptr);
    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.currentWeight(), 2);
}

// 超过最大权重时，应从最久未使用 entry 开始连续淘汰到不超限。
TEST(WeightedLruCacheTest, EvictsLeastRecentlyUsedUntilWithinWeight)
{
    WeightedLruCache<int, std::string> cache(5);

    ASSERT_NE(cache.put(1, std::string("one"), 2), nullptr);
    ASSERT_NE(cache.put(2, std::string("two"), 2), nullptr);
    ASSERT_NE(cache.put(3, std::string("three"), 1), nullptr);

    auto* four = cache.put(4, std::string("four"), 3);
    ASSERT_NE(four, nullptr);
    EXPECT_EQ(*four, "four");

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_NE(cache.get(3), nullptr);
    EXPECT_NE(cache.get(4), nullptr);
    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.currentWeight(), 4);
}

// 淘汰前的 get 命中应刷新 LRU 顺序，避免该 entry 被优先淘汰。
TEST(WeightedLruCacheTest, GetPromotesEntryBeforeEviction)
{
    WeightedLruCache<int, std::string> cache(3);

    ASSERT_NE(cache.put(1, std::string("one"), 1), nullptr);
    ASSERT_NE(cache.put(2, std::string("two"), 1), nullptr);
    ASSERT_NE(cache.put(3, std::string("three"), 1), nullptr);

    ASSERT_NE(cache.get(1), nullptr);
    ASSERT_NE(cache.put(4, std::string("four"), 1), nullptr);

    EXPECT_NE(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_NE(cache.get(3), nullptr);
    EXPECT_NE(cache.get(4), nullptr);
    EXPECT_EQ(cache.size(), 3);
    EXPECT_EQ(cache.currentWeight(), 3);
}

// 覆写已有 key 应更新 value 和 weight，并按新权重触发必要淘汰。
TEST(WeightedLruCacheTest, OverwriteExistingEntryUpdatesWeightAndValue)
{
    WeightedLruCache<int, std::string> cache(5);

    ASSERT_NE(cache.put(1, std::string("one"), 2), nullptr);
    ASSERT_NE(cache.put(2, std::string("two"), 2), nullptr);
    ASSERT_NE(cache.put(3, std::string("three"), 1), nullptr);

    auto* updated = cache.put(2, std::string("two-updated"), 4);
    ASSERT_NE(updated, nullptr);
    EXPECT_EQ(*updated, "two-updated");

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_NE(cache.get(3), nullptr);

    auto* two = cache.get(2);
    ASSERT_NE(two, nullptr);
    EXPECT_EQ(*two, "two-updated");
    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.currentWeight(), 5);

    auto* smaller = cache.put(2, std::string("two-small"), 1);
    ASSERT_NE(smaller, nullptr);
    EXPECT_EQ(*smaller, "two-small");
    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.currentWeight(), 2);
}

// 单个 entry 权重大于 maxWeight 时应拒绝缓存；若为已有 key，应删除旧值避免读到过期对象。
TEST(WeightedLruCacheTest, RejectsEntryHeavierThanMaxWeight)
{
    WeightedLruCache<int, std::string> cache(3);

    ASSERT_NE(cache.put(1, std::string("one"), 1), nullptr);

    EXPECT_EQ(cache.put(2, std::string("too-heavy"), 4), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    auto* one = cache.get(1);
    ASSERT_NE(one, nullptr);
    EXPECT_EQ(*one, "one");
    EXPECT_EQ(cache.size(), 1);
    EXPECT_EQ(cache.currentWeight(), 1);

    EXPECT_EQ(cache.put(1, std::string("too-heavy-update"), 4), nullptr);
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.currentWeight(), 0);
}

// maxWeight 为 0 时应禁用缓存，所有 put 都返回 nullptr 且不插入。
TEST(WeightedLruCacheTest, ZeroMaxWeightDisablesCaching)
{
    WeightedLruCache<int, std::string> cache(0);

    EXPECT_EQ(cache.put(1, std::string("one"), 0), nullptr);
    EXPECT_EQ(cache.put(2, std::string("two"), 1), nullptr);
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.currentWeight(), 0);
    EXPECT_EQ(cache.maxWeight(), 0);
}

// value 类型为 move-only 时，put/get 仍应返回缓存内对象指针。
TEST(WeightedLruCacheTest, SupportsMoveOnlyValue)
{
    WeightedLruCache<int, std::unique_ptr<std::string>> cache(2);

    auto* inserted = cache.put(1, std::make_unique<std::string>("one"), 1);
    ASSERT_NE(inserted, nullptr);
    ASSERT_NE(inserted->get(), nullptr);
    EXPECT_EQ(**inserted, "one");

    auto* cached = cache.get(1);
    ASSERT_NE(cached, nullptr);
    ASSERT_NE(cached->get(), nullptr);
    EXPECT_EQ(**cached, "one");

    ASSERT_NE(cache.put(2, std::make_unique<std::string>("two"), 1), nullptr);
    ASSERT_NE(cache.put(3, std::make_unique<std::string>("three"), 1), nullptr);

    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_NE(cache.get(2), nullptr);
    EXPECT_NE(cache.get(3), nullptr);
}

// move-only value 被超重拒绝时，不应提前 consume 调用方持有的对象。
TEST(WeightedLruCacheTest, RejectsOverweightMoveOnlyValueWithoutConsumingInput)
{
    WeightedLruCache<int, std::unique_ptr<std::string>> cache(2);

    ASSERT_NE(cache.put(1, std::make_unique<std::string>("cached"), 1), nullptr);

    auto tooHeavy = std::make_unique<std::string>("too-heavy");
    EXPECT_EQ(cache.put(2, std::move(tooHeavy), 3), nullptr);
    ASSERT_NE(tooHeavy, nullptr);
    EXPECT_EQ(*tooHeavy, "too-heavy");
    auto* cached = cache.get(1);
    ASSERT_NE(cached, nullptr);
    ASSERT_NE(cached->get(), nullptr);
    EXPECT_EQ(**cached, "cached");
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(cache.size(), 1);
    EXPECT_EQ(cache.currentWeight(), 1);

    auto replacement = std::make_unique<std::string>("replacement");
    EXPECT_EQ(cache.put(1, std::move(replacement), 3), nullptr);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(*replacement, "replacement");
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.currentWeight(), 0);
}

// erase 和 clear 应同步更新 size/currentWeight，并保留 maxWeight。
TEST(WeightedLruCacheTest, EraseAndClearUpdateSizeAndWeight)
{
    WeightedLruCache<int, std::string> cache(10);

    ASSERT_NE(cache.put(1, std::string("one"), 2), nullptr);
    ASSERT_NE(cache.put(2, std::string("two"), 3), nullptr);
    ASSERT_NE(cache.put(3, std::string("three"), 4), nullptr);

    EXPECT_TRUE(cache.erase(2));
    EXPECT_FALSE(cache.erase(2));
    EXPECT_EQ(cache.get(2), nullptr);
    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.currentWeight(), 6);

    cache.clear();
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.get(3), nullptr);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(cache.currentWeight(), 0);
    EXPECT_EQ(cache.maxWeight(), 10);
}
