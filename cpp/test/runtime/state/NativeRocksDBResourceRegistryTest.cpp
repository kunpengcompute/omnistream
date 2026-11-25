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
#include <limits>
#include "runtime/state/NativeRocksDBResourceRegistry.h"

TEST(NativeRocksDBResourceRegistryTest, SharesWithinHandleAndIsolatesPools)
{
    auto& registry = NativeRocksDBResourceRegistry::instance();
    const auto first = registry.create(12 * 1024 * 1024, 0.5, 0.1, false);
    const auto second = registry.create(12 * 1024 * 1024, 0.5, 0.1, false);
    auto a = registry.acquire(first);
    auto b = registry.acquire(first);
    auto c = registry.acquire(second);
    EXPECT_EQ(a, b);
    EXPECT_EQ(a->getCache(), b->getCache());
    EXPECT_EQ(a->getWriteBufferManager(), b->getWriteBufferManager());
    EXPECT_NE(a->getCache(), c->getCache());
    EXPECT_EQ(a->getCache()->GetCapacity(), 10 * 1024 * 1024);
    EXPECT_EQ(a->getWriteBufferManagerCapacity(), 4 * 1024 * 1024);
    EXPECT_THROW(registry.release(first), std::logic_error);
    a.reset();
    b.reset();
    c.reset();
    registry.release(first);
    registry.release(second);
    EXPECT_THROW(registry.acquire(first), std::invalid_argument);
    EXPECT_NO_THROW(registry.release(first));
}

TEST(NativeRocksDBResourceRegistryTest, RejectsInvalidBudgetsAndRatios)
{
    auto& registry = NativeRocksDBResourceRegistry::instance();
    EXPECT_THROW(registry.create(0, 0.5, 0.1, false), std::invalid_argument);
    EXPECT_THROW(registry.create(1024, 1, 0.1, false), std::invalid_argument);
    EXPECT_THROW(registry.create(1024, 0.5, 0.6, false), std::invalid_argument);
    EXPECT_THROW(registry.create(1024, std::numeric_limits<double>::quiet_NaN(), 0.1, false), std::invalid_argument);
    EXPECT_THROW(registry.acquire(-1), std::invalid_argument);
}
