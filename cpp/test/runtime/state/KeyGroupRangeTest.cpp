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
#include "runtime/state/KeyGroupRange.h"

TEST(KeyGroupRangeTest, Getters)
{
    KeyGroupRange range = KeyGroupRange(0, 10);
    EXPECT_EQ(range.getStartKeyGroup(), 0);
    EXPECT_EQ(range.getEndKeyGroup(), 10);
    EXPECT_EQ(range.getNumberOfKeyGroups(), 11);
    EXPECT_EQ(range.getKeyGroupId(1), 1);
    EXPECT_EQ(range.getKeyGroupId(10), 10);
}

TEST(KeyGroupRangeTest, Equals)
{
    KeyGroupRange range1 = KeyGroupRange(0, 10);
    KeyGroupRange range2 = KeyGroupRange(0, 10);
    KeyGroupRange range3 = KeyGroupRange(0, 2);
    EXPECT_EQ(range1 == range2, true);
    EXPECT_EQ(range1 == range3, false);
}

TEST(KeyGroupRangeTest, Contains)
{
    KeyGroupRange range = KeyGroupRange(0, 10);
    EXPECT_EQ(range.contains(5), true);
    EXPECT_EQ(range.contains(11), false);
}

TEST(KeyGroupRangeTest, EmptyKeyGroupRange)
{
    KeyGroupRange* empty = KeyGroupRange::EMPTY_KEY_GROUP_RANGE();
    EXPECT_EQ(empty->getStartKeyGroup(), 0);
    EXPECT_EQ(empty->getEndKeyGroup(), -1);
}
