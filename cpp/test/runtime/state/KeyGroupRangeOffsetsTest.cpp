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
#include "runtime/state/KeyGroupRangeOffsets.h"

std::vector<int64_t> copyOfRange(const std::vector<int64_t>& vec, int start, int end)
{
    return std::vector<int64_t>(vec.begin() + start, vec.begin() + end);
}

TEST(KeyGroupRangeOffsetsTest, KeyGroupIntersection)
{
    // offsets [0,1,2,3,4,5,6,7,8]
    std::vector<int64_t> offsets(9);
    for (int i = 0; i < 9; ++i) offsets[i] = i;
    int startKeyGroup = 2;

    // KeyGroupRangeOffsets with KeyGroupRange [2, 10]
    KeyGroupRangeOffsets keyGroupRangeOffsets(
        *std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(startKeyGroup, 10)), offsets);
    // KeyGroupRangeOffsets with KeyGroupRange [3, 7]
    KeyGroupRangeOffsets intersection =
        keyGroupRangeOffsets.getIntersection(*std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(3, 7)));

    // [2, 10] and [3, 7]: keyGroupRange intersection is [3, 7], offsets intersection is [1, 5]
    KeyGroupRangeOffsets expected(
        *std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(3, 7)),
        copyOfRange(offsets, 3 - startKeyGroup, 8 - startKeyGroup));
    std::cout << "Expected: " << expected.ToString() << std::endl;
    std::cout << "Actual:   " << intersection.ToString() << std::endl;

    EXPECT_TRUE(intersection == expected);

    // intersection with itself
    intersection = keyGroupRangeOffsets.getIntersection(keyGroupRangeOffsets.getKeyGroupRange());
    EXPECT_TRUE(intersection == keyGroupRangeOffsets);

    // [2, 10] and [11, 13]: keyGroupRange intersection is [0, -1]
    intersection = keyGroupRangeOffsets.getIntersection(*std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(11, 13)));
    EXPECT_TRUE(intersection.getKeyGroupRange() == *KeyGroupRange::EMPTY_KEY_GROUP_RANGE());
    EXPECT_EQ(intersection.getKeyGroupRange().getNumberOfKeyGroups(), 0);
    EXPECT_EQ(intersection.ToString().find("offsets="), std::string::npos);

    // [2, 10] and [5, 13]: keyGroupRange intersection is [5, 10], offsets intersection is [3, 8]
    intersection = keyGroupRangeOffsets.getIntersection(*std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(5, 13)));
    expected = KeyGroupRangeOffsets(
        *std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(5, 10)),
        copyOfRange(offsets, 5 - startKeyGroup, 11 - startKeyGroup));
    EXPECT_TRUE(intersection == expected);

    // [2, 10] and [0, 2]: keyGroupRange intersection is [2], offsets intersection is [0]
    intersection = keyGroupRangeOffsets.getIntersection(*std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(0, 2)));
    expected = KeyGroupRangeOffsets(
        *std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(2, 2)),
        copyOfRange(offsets, 2 - startKeyGroup, 3 - startKeyGroup));
    EXPECT_TRUE(intersection == expected);
}

void testKeyGroupRangeOffsetsBasicsInternal(int startKeyGroup, int endKeyGroup)
{
    int len = endKeyGroup - startKeyGroup + 1;
    std::vector<int64_t> offsets(len > 0 ? len : 0);
    for (int i = 0; i < offsets.size(); ++i) offsets[i] = i;

    KeyGroupRangeOffsets keyGroupRange(startKeyGroup, endKeyGroup, offsets);
    KeyGroupRangeOffsets sameButDifferentConstr(
        *std::unique_ptr<KeyGroupRange>(KeyGroupRange::of(startKeyGroup, endKeyGroup)), offsets);
    EXPECT_TRUE(sameButDifferentConstr == keyGroupRange);

    int numberOfKeyGroup = keyGroupRange.getKeyGroupRange().getNumberOfKeyGroups();
    EXPECT_EQ(numberOfKeyGroup, std::max(0, endKeyGroup - startKeyGroup + 1));

    if (numberOfKeyGroup > 0) {
        EXPECT_EQ(keyGroupRange.getKeyGroupRange().getStartKeyGroup(), startKeyGroup);
        EXPECT_EQ(keyGroupRange.getKeyGroupRange().getEndKeyGroup(), endKeyGroup);

        int c = startKeyGroup;
        for (int idx = 0; idx < numberOfKeyGroup; ++idx, ++c) {
            int kg = keyGroupRange.getKeyGroupRange().getStartKeyGroup() + idx;
            int64_t val = keyGroupRange.getKeyGroupOffset(kg);
            EXPECT_EQ(kg, c);
            EXPECT_TRUE(keyGroupRange.getKeyGroupRange().contains(kg));
            EXPECT_EQ(val, int64_t(c - startKeyGroup));
        }

        for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
            EXPECT_EQ(keyGroupRange.getKeyGroupOffset(i), i - startKeyGroup);
        }

        int newOffset = 42;
        for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
            keyGroupRange.setKeyGroupOffset(i, newOffset);
            ++newOffset;
        }

        for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
            EXPECT_EQ(keyGroupRange.getKeyGroupOffset(i), 42 + i - startKeyGroup);
        }

        EXPECT_FALSE(keyGroupRange.getKeyGroupRange().contains(startKeyGroup - 1));
        EXPECT_FALSE(keyGroupRange.getKeyGroupRange().contains(endKeyGroup + 1));
    } else {
        EXPECT_TRUE(keyGroupRange.getKeyGroupRange() == *KeyGroupRange::EMPTY_KEY_GROUP_RANGE());
    }
}

TEST(KeyGroupRangeOffsetsTest, KeyGroupRangeOffsetsBasics)
{
    testKeyGroupRangeOffsetsBasicsInternal(0, 0);
    testKeyGroupRangeOffsetsBasicsInternal(0, 1);
    testKeyGroupRangeOffsetsBasicsInternal(1, 2);
    testKeyGroupRangeOffsetsBasicsInternal(42, 42);
    testKeyGroupRangeOffsetsBasicsInternal(3, 7);
    testKeyGroupRangeOffsetsBasicsInternal(0, std::numeric_limits<short>::max());
    testKeyGroupRangeOffsetsBasicsInternal(std::numeric_limits<short>::max() - 1, std::numeric_limits<short>::max());

    EXPECT_THROW(testKeyGroupRangeOffsetsBasicsInternal(-3, 2), std::invalid_argument);

    KeyGroupRangeOffsets testNoGivenOffsets(3, 7);
    for (int i = 3; i <= 7; ++i) {
        testNoGivenOffsets.setKeyGroupOffset(i, i + 1);
    }
    for (int i = 3; i <= 7; ++i) {
        EXPECT_EQ(testNoGivenOffsets.getKeyGroupOffset(i), i + 1);
    }
}
