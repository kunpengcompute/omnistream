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
#include "runtime/state/heap/HeapPriorityQueueSet.h"
#include "state/heap/HeapPriorityQueueElement.h"

namespace {
class TestIntType : public HeapPriorityQueueElement {
private:
    int val;

public:
    TestIntType(int val) : val(val) {};
    int getKey()
    {
        return val;
    }
    size_t hashCode()
    {
        return std::hash<int>()(val);
    };

    struct SharedPtrHash {
        size_t operator()(const std::shared_ptr<TestIntType>& obj) const
        {
            if (obj) {
                return obj->hashCode();
            } else {
                return 0;
            }
        }
    };

    struct SharedPtrEqual {
        bool operator()(const std::shared_ptr<TestIntType>& lhs, const std::shared_ptr<TestIntType>& rhs) const
        {
            return lhs->getKey() == rhs->getKey();
        }
    };

    struct SharedPtrComparator {
        bool operator()(const std::shared_ptr<TestIntType>& lhs, const std::shared_ptr<TestIntType>& rhs) const
        {
            return lhs->getKey() < rhs->getKey();
        }
    };
};
} // namespace

TEST(HeapPriorityQueueSetTest, Initialization)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(1));
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, Inserts)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(3));
    pq.add(std::make_shared<TestIntType>(2));
    pq.add(std::make_shared<TestIntType>(4));

    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.poll()->getKey(), 4);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, HeadChange)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    EXPECT_EQ(pq.add(std::make_shared<TestIntType>(5)), true);
    EXPECT_EQ(pq.add(std::make_shared<TestIntType>(3)), true);
    EXPECT_EQ(pq.add(std::make_shared<TestIntType>(3)), false);
    EXPECT_EQ(pq.add(std::make_shared<TestIntType>(4)), false);
    EXPECT_EQ(pq.add(std::make_shared<TestIntType>(1)), true);
}

TEST(HeapPriorityQueueSetTest, ClearTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    EXPECT_EQ(pq.add(std::make_shared<TestIntType>(5)), true);
    EXPECT_EQ(pq.isEmpty(), false);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, DuplicateTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(1));

    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, RemoveTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));
    pq.add(std::make_shared<TestIntType>(3));
    pq.remove(std::make_shared<TestIntType>(2));
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, PeekTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    EXPECT_EQ(pq.peek(), nullptr);

    pq.add(std::make_shared<TestIntType>(5));
    pq.add(std::make_shared<TestIntType>(3));
    pq.add(std::make_shared<TestIntType>(1));

    EXPECT_EQ(pq.peek()->getKey(), 1);
    EXPECT_EQ(pq.peek()->getKey(), 1);
    EXPECT_EQ(pq.size(), 3);
}

TEST(HeapPriorityQueueSetTest, SizeTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    EXPECT_EQ(pq.size(), 0);

    pq.add(std::make_shared<TestIntType>(1));
    EXPECT_EQ(pq.size(), 1);

    pq.add(std::make_shared<TestIntType>(2));
    EXPECT_EQ(pq.size(), 2);

    pq.add(std::make_shared<TestIntType>(1));
    EXPECT_EQ(pq.size(), 2);

    pq.poll();
    EXPECT_EQ(pq.size(), 1);

    pq.remove(std::make_shared<TestIntType>(2));
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, IteratorTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));
    pq.add(std::make_shared<TestIntType>(3));

    auto iter = pq.iterator();
    int count = 0;
    while (iter->hasNext()) {
        iter->next();
        count++;
    }
    EXPECT_EQ(count, 3);
}

TEST(HeapPriorityQueueSetTest, AddAllTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    std::vector<std::shared_ptr<TestIntType>> elements = {
        std::make_shared<TestIntType>(5),
        std::make_shared<TestIntType>(2),
        std::make_shared<TestIntType>(8),
        std::make_shared<TestIntType>(1)};

    pq.addAll(elements);
    EXPECT_EQ(pq.size(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll()->getKey(), 8);
}

TEST(HeapPriorityQueueSetTest, ToArrayTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(3));
    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));

    auto arr = pq.toArray();
    EXPECT_EQ(arr.size(), 3);
}

TEST(HeapPriorityQueueSetTest, RemoveNonExistentTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));

    EXPECT_EQ(pq.remove(std::make_shared<TestIntType>(999)), false);
    EXPECT_EQ(pq.size(), 2);
}

TEST(HeapPriorityQueueSetTest, RemoveHeadTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    auto elem1 = std::make_shared<TestIntType>(1);
    auto elem2 = std::make_shared<TestIntType>(2);
    auto elem3 = std::make_shared<TestIntType>(3);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);

    pq.remove(elem1);
    EXPECT_EQ(pq.size(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, EmptyQueueOperationsTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);
    EXPECT_EQ(pq.isEmpty(), true);
    EXPECT_EQ(pq.size(), 0);
    EXPECT_EQ(pq.peek(), nullptr);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.remove(std::make_shared<TestIntType>(1)), false);
}

TEST(HeapPriorityQueueSetTest, LargeScaleTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    for (int i = 500; i >= 1; --i) {
        pq.add(std::make_shared<TestIntType>(i));
    }
    EXPECT_EQ(pq.size(), 500);

    for (int i = 1; i <= 500; ++i) {
        EXPECT_EQ(pq.poll()->getKey(), i);
    }
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupBasic)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));
    pq.add(std::make_shared<TestIntType>(3));
    pq.add(std::make_shared<TestIntType>(4));
    pq.add(std::make_shared<TestIntType>(5));

    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, pq.size());
    EXPECT_EQ(totalSubsetSize, 5);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupEmpty)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        EXPECT_EQ(subset->size(), 0u);
    }
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupAfterRemove)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));
    pq.add(std::make_shared<TestIntType>(3));

    pq.remove(std::make_shared<TestIntType>(2));

    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, 2);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupAfterPoll)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(2));
    pq.add(std::make_shared<TestIntType>(3));

    pq.poll();

    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, 2);
}

TEST(HeapPriorityQueueSetTest, GetSubsetForKeyGroupConsistency)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    for (int i = 1; i <= 10; ++i) {
        pq.add(std::make_shared<TestIntType>(i));
    }

    std::unordered_set<int> allKeysInSubsets;
    int totalSubsetSize = 0;
    for (int i = 0; i < keyGroupRange->getNumberOfKeyGroups(); ++i) {
        auto subset = pq.getSubsetForKeyGroup(i);
        ASSERT_NE(subset, nullptr);
        for (const auto& elem : *subset) {
            EXPECT_EQ(allKeysInSubsets.count(elem->getKey()), 0u)
                << "Key " << elem->getKey() << " found in multiple subsets";
            allKeysInSubsets.insert(elem->getKey());
        }
        totalSubsetSize += static_cast<int>(subset->size());
    }
    EXPECT_EQ(totalSubsetSize, 10);
    EXPECT_EQ(allKeysInSubsets.size(), 10u);
}

TEST(HeapPriorityQueueSetTest, MixedOperationsTest)
{
    auto keyGroupRange = std::make_unique<KeyGroupRange>(0, 5);
    auto pq = HeapPriorityQueueSet<int, std::shared_ptr<TestIntType>, TestIntType::SharedPtrComparator>(
        keyGroupRange.get(), keyGroupRange->getNumberOfKeyGroups(), 6);

    pq.add(std::make_shared<TestIntType>(5));
    pq.add(std::make_shared<TestIntType>(3));
    EXPECT_EQ(pq.poll()->getKey(), 3);

    pq.add(std::make_shared<TestIntType>(1));
    pq.add(std::make_shared<TestIntType>(4));
    EXPECT_EQ(pq.peek()->getKey(), 1);

    pq.remove(std::make_shared<TestIntType>(4));
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.isEmpty(), true);
}
