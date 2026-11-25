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
#include "runtime/state/heap/HeapPriorityQueue.h"
#include "state/heap/HeapPriorityQueueElement.h"

namespace {
class IntType : public HeapPriorityQueueElement {
private:
    int val;

public:
    IntType(int val) : val(val)
    {
    }
    int getKey() const
    {
        return val;
    }
    void setKey(int v)
    {
        val = v;
    }

    struct SharedPtrComparator {
        bool operator()(const std::shared_ptr<IntType>& lhs, const std::shared_ptr<IntType>& rhs) const
        {
            return lhs->getKey() < rhs->getKey();
        }
    };
};
} // namespace

TEST(HeapPriorityQueueTest, Initialization)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    EXPECT_EQ(pq.size(), 0);
    EXPECT_EQ(pq.isEmpty(), true);
    EXPECT_EQ(pq.peek(), nullptr);
    EXPECT_EQ(pq.poll(), nullptr);
}

TEST(HeapPriorityQueueTest, Inserts)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(4));
    pq.add(std::make_shared<IntType>(2));

    EXPECT_EQ(pq.size(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.poll()->getKey(), 4);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, HeadChange)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(5)), true);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(3)), true);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(4)), false);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(1)), true);
    EXPECT_EQ(pq.add(std::make_shared<IntType>(2)), false);
}

TEST(HeapPriorityQueueTest, Peek)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    pq.add(std::make_shared<IntType>(5));
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(1));

    EXPECT_EQ(pq.peek()->getKey(), 1);
    EXPECT_EQ(pq.peek()->getKey(), 1);
    EXPECT_EQ(pq.size(), 3);
}

TEST(HeapPriorityQueueTest, ClearTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    pq.add(std::make_shared<IntType>(5));
    pq.add(std::make_shared<IntType>(3));
    EXPECT_EQ(pq.isEmpty(), false);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll(), nullptr);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, RemoveTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem1 = std::make_shared<IntType>(1);
    auto elem2 = std::make_shared<IntType>(2);
    auto elem3 = std::make_shared<IntType>(3);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);

    pq.remove(elem2);
    EXPECT_EQ(pq.size(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 3);
}

TEST(HeapPriorityQueueTest, AddAll)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(2);
    std::vector<std::shared_ptr<IntType>> elements = {
        std::make_shared<IntType>(5),
        std::make_shared<IntType>(2),
        std::make_shared<IntType>(8),
        std::make_shared<IntType>(1)};

    pq.addAll(elements);
    EXPECT_EQ(pq.size(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll()->getKey(), 8);
}

TEST(HeapPriorityQueueTest, Iterator)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));
    pq.add(std::make_shared<IntType>(3));

    auto iter = pq.iterator();
    int count = 0;
    while (iter->hasNext()) {
        iter->next();
        count++;
    }
    EXPECT_EQ(count, 3);
}

TEST(HeapPriorityQueueTest, ToArray)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    pq.add(std::make_shared<IntType>(3));
    pq.add(std::make_shared<IntType>(1));
    pq.add(std::make_shared<IntType>(2));

    auto arr = pq.toArray();
    EXPECT_EQ(arr.size(), 3);
}

TEST(HeapPriorityQueueTest, AdjustModifiedElement)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem1 = std::make_shared<IntType>(1);
    auto elem2 = std::make_shared<IntType>(2);
    auto elem3 = std::make_shared<IntType>(3);
    auto elem4 = std::make_shared<IntType>(4);
    auto elem5 = std::make_shared<IntType>(5);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);
    pq.add(elem4);
    pq.add(elem5);

    elem3->setKey(6);
    pq.adjustModifiedElement(elem3);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll()->getKey(), 6);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, RemoveHeadTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem1 = std::make_shared<IntType>(1);
    auto elem2 = std::make_shared<IntType>(2);
    auto elem3 = std::make_shared<IntType>(3);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);

    pq.remove(elem1);
    EXPECT_EQ(pq.size(), 2);
    EXPECT_EQ(pq.peek()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 3);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, RemoveLastElementTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem1 = std::make_shared<IntType>(1);
    auto elem2 = std::make_shared<IntType>(2);
    auto elem3 = std::make_shared<IntType>(3);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);

    pq.remove(elem3);
    EXPECT_EQ(pq.size(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, SingleElementTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem = std::make_shared<IntType>(42);

    pq.add(elem);
    EXPECT_EQ(pq.size(), 1);
    EXPECT_EQ(pq.isEmpty(), false);
    EXPECT_EQ(pq.peek()->getKey(), 42);

    pq.remove(elem);
    EXPECT_EQ(pq.size(), 0);
    EXPECT_EQ(pq.isEmpty(), true);
    EXPECT_EQ(pq.peek(), nullptr);
}

TEST(HeapPriorityQueueTest, LargeScaleTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(100);

    for (int i = 500; i >= 1; --i) {
        pq.add(std::make_shared<IntType>(i));
    }
    EXPECT_EQ(pq.size(), 500);

    for (int i = 1; i <= 500; ++i) {
        EXPECT_EQ(pq.poll()->getKey(), i);
    }
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, MixedOperationsTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);

    pq.add(std::make_shared<IntType>(5));
    pq.add(std::make_shared<IntType>(3));
    EXPECT_EQ(pq.poll()->getKey(), 3);

    pq.add(std::make_shared<IntType>(1));

    auto elem4 = std::make_shared<IntType>(4);
    pq.add(elem4);
    EXPECT_EQ(pq.peek()->getKey(), 1);

    pq.remove(elem4);
    EXPECT_EQ(pq.size(), 2);

    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, SamePriorityTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem1 = std::make_shared<IntType>(5);
    auto elem2 = std::make_shared<IntType>(5);
    auto elem3 = std::make_shared<IntType>(5);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);

    EXPECT_EQ(pq.size(), 3);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, AddAllEmpty)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    std::vector<std::shared_ptr<IntType>> emptyElements;

    pq.addAll(emptyElements);
    EXPECT_EQ(pq.size(), 0);
    EXPECT_EQ(pq.isEmpty(), true);
}

TEST(HeapPriorityQueueTest, RemoveMiddleElementTest)
{
    auto pq = HeapPriorityQueue<std::shared_ptr<IntType>, IntType::SharedPtrComparator>(10);
    auto elem1 = std::make_shared<IntType>(1);
    auto elem2 = std::make_shared<IntType>(5);
    auto elem3 = std::make_shared<IntType>(3);
    auto elem4 = std::make_shared<IntType>(2);
    auto elem5 = std::make_shared<IntType>(4);

    pq.add(elem1);
    pq.add(elem2);
    pq.add(elem3);
    pq.add(elem4);
    pq.add(elem5);

    pq.remove(elem3);
    EXPECT_EQ(pq.size(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 1);
    EXPECT_EQ(pq.poll()->getKey(), 2);
    EXPECT_EQ(pq.poll()->getKey(), 4);
    EXPECT_EQ(pq.poll()->getKey(), 5);
    EXPECT_EQ(pq.isEmpty(), true);
}
