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

#pragma once

#include "HeapPriorityQueue.h"
#include "state/KeyGroupedInternalPriorityQueue.h"
#include "state/KeyGroupRange.h"
#include "state/rocksdb/RocksDBCachingPriorityQueueSet.h"
#include "runtime/state/KeyGroupRangeAssignment.h"
#include "core/utils/type_traits_ext.h"

template <typename K, typename T, typename Comparator>
class KeyGroupPartitionedPriorityQueue : public KeyGroupedInternalPriorityQueue<T> {
    static_assert(is_shared_ptr_v<T>, "T should be shared ptr.");
    using DedupSet = typename KeyGroupedInternalPriorityQueue<T>::DedupSet;

    class KeyGroupConcatenationIterator : public omnistream::utils::Iterator<T> {
    public:
        KeyGroupConcatenationIterator(KeyGroupPartitionedPriorityQueue* self)
            : self_(self),
              current(self_->keyGroupedHeaps_[0]->iterator())
        {
        }

        bool hasNext() override
        {
            bool currentHasNext = current->hasNext();
            while (!currentHasNext && index_ < self_->keyGroupedHeaps_.size() - 1) {
                current = self_->keyGroupedHeaps_[++index_]->iterator();
                currentHasNext = current->hasNext();
            }
            return currentHasNext;
        }

        T next() override
        {
            return current->next();
        }

    private:
        KeyGroupPartitionedPriorityQueue* self_;
        int32_t index_ = 0;
        std::unique_ptr<omnistream::utils::Iterator<T>> current = nullptr;
    };

public:
    using RocksDBCachingPQ = RocksDBCachingPriorityQueueSet<K, T, Comparator>;
    using PartitionQueueSetFactory =
        std::function<std::shared_ptr<RocksDBCachingPQ>(int32_t keyGroupId, int32_t totalKeyGroups)>;

    KeyGroupPartitionedPriorityQueue(
        PartitionQueueSetFactory priorityQueueSetFactory, KeyGroupRange* keyGroupRange, int32_t totalKeyGroups)
        : heapOfKeyGroupedHeaps_(keyGroupRange->getNumberOfKeyGroups()),
          keyGroupRange_(keyGroupRange),
          totalKeyGroups_(totalKeyGroups)
    {
        if (keyGroupRange_->getNumberOfKeyGroups() <= 0) {
            THROW_LOGIC_EXCEPTION("KeyGroupRange must have at least one key group.");
        }
        firstKeyGroup_ = keyGroupRange_->getStartKeyGroup();
        keyGroupedHeaps_.reserve(keyGroupRange_->getNumberOfKeyGroups());

        for (auto i = 0; i < keyGroupRange_->getNumberOfKeyGroups(); i++) {
            auto priorityQueue = priorityQueueSetFactory(firstKeyGroup_ + i, totalKeyGroups_);
            keyGroupedHeaps_.emplace_back(priorityQueue);
            heapOfKeyGroupedHeaps_.add(priorityQueue);
        }
    }

    T poll() override
    {
        auto headPQ = heapOfKeyGroupedHeaps_.peek();
        auto head = headPQ->poll();
        heapOfKeyGroupedHeaps_.adjustModifiedElement(headPQ);
        return head;
    }

    T peek() override
    {
        const auto& headPQ = heapOfKeyGroupedHeaps_.peek();
        return headPQ->peek();
    }

    bool add(const T& element) override
    {
        auto pq = getKeyGroupSubHeapForElement(element);
        if (pq->add(element)) {
            heapOfKeyGroupedHeaps_.adjustModifiedElement(pq);
            const auto& newHead = peek();
            return !comparator_(element, newHead) && !comparator_(newHead, element);
        }
        return false;
    }

    void addAll(const std::vector<T>& elements) override
    {
        if (elements.empty()) {
            return;
        }
        for (const auto& element : elements) {
            add(element);
        }
    }

    bool remove(const T& element) override
    {
        auto pq = getKeyGroupSubHeapForElement(element);
        auto oldHead = peek();
        if (pq->remove(element)) {
            heapOfKeyGroupedHeaps_.adjustModifiedElement(pq);
            return !comparator_(element, oldHead) && !comparator_(oldHead, element);
        }
        return false;
    }

    bool isEmpty() override
    {
        return peek() == nullptr;
    }

    int32_t size() override
    {
        auto sizeSum = 0;
        for (const auto& pq : keyGroupedHeaps_) {
            sizeSum += pq->size();
        }
        return sizeSum;
    }

    std::unique_ptr<omnistream::utils::Iterator<T>> iterator() override
    {
        return std::make_unique<KeyGroupConcatenationIterator>(this);
    }

    std::shared_ptr<DedupSet> getSubsetForKeyGroup(int32_t keyGroupId) override
    {
        std::shared_ptr<DedupSet> result = std::make_shared<DedupSet>();
        auto& partitionQueue = keyGroupedHeaps_[globalKeyGroupToLocalIndex(keyGroupId)];
        // Pre-allocate memory to prevent expensive rehashing
        result->reserve(partitionQueue->size());
        auto iter = partitionQueue->iterator();
        try {
            while (iter->hasNext()) {
                result->insert(iter->next());
            }
        } catch (const std::exception& e) {
            THROW_RUNTIME_ERROR("Exception while iterating key group: " + std::string(e.what()));
        }

        return result;
    }

private:
    struct PQComparator {
        bool operator()(std::shared_ptr<RocksDBCachingPQ> a, const std::shared_ptr<RocksDBCachingPQ>& b) const
        {
            auto left = a->peek();
            auto right = b->peek();
            if (left == nullptr) {
                return false;
            }
            return right == nullptr ? true : comparator_(left, right);
        }
        Comparator comparator_;
    };

    // A heap of heap sets. Each sub-heap represents the partition for a key-group.
    HeapPriorityQueue<std::shared_ptr<RocksDBCachingPQ>, PQComparator> heapOfKeyGroupedHeaps_;

    // All elements from keyGroupHeap, indexed by their key-group id, relative to firstKeyGroup.
    std::vector<std::shared_ptr<RocksDBCachingPQ>> keyGroupedHeaps_;

    KeyGroupRange* keyGroupRange_;
    int32_t firstKeyGroup_;
    int32_t totalKeyGroups_;
    Comparator comparator_;

    std::shared_ptr<RocksDBCachingPQ> getKeyGroupSubHeapForElement(T element)
    {
        return keyGroupedHeaps_[computeKeyGroupIndex(element)];
    }

    int32_t computeKeyGroupIndex(const T& element)
    {
        auto key = element->getKey();
        int32_t keyGroupId = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, totalKeyGroups_);
        return globalKeyGroupToLocalIndex(keyGroupId);
    }

    int32_t globalKeyGroupToLocalIndex(int32_t keyGroup) const
    {
        if (!keyGroupRange_->contains(keyGroup)) {
            THROW_LOGIC_EXCEPTION(
                "Key group " + std::to_string(keyGroup) + " is not included in" + "[" +
                std::to_string(keyGroupRange_->getStartKeyGroup()) + ", " +
                std::to_string(keyGroupRange_->getEndKeyGroup()) + "]");
        }

        return keyGroup - keyGroupRange_->getStartKeyGroup();
    }
};
