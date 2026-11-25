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

#include <climits>
#include <vector>
#include <unordered_set>

#include "HeapPriorityQueue.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupRangeAssignment.h"
#include "state/KeyGroupedInternalPriorityQueue.h"
#include "state/restore/KeyGroupEntryIterator.h"
#include "core/utils/type_traits_ext.h"

/**
 *
 * @tparam T
 * @tparam Comparator Comparator{}(first, second) return true means the first has a higher priority than the second
 * (first will be put before the second)
 */
template <typename K, typename T, typename Comparator>
class HeapPriorityQueueSet : public HeapPriorityQueue<T, Comparator>, public KeyGroupedInternalPriorityQueue<T> {
public:
    static_assert(is_shared_ptr_v<T>, "T should be shared ptr.");
    using DedupSet = typename KeyGroupedInternalPriorityQueue<T>::DedupSet;

    HeapPriorityQueueSet(KeyGroupRange* keyGroupRange, int32_t minCapacity, int32_t totalNumberOfKeyGroups)
        : HeapPriorityQueue<T, Comparator>(minCapacity),
          totalNumberOfKeyGroups(totalNumberOfKeyGroups),
          keyGroupRange(keyGroupRange)
    {
        int32_t keyGroupsInLocalRange = keyGroupRange->getNumberOfKeyGroups();
        if (keyGroupsInLocalRange <= 0) {
            THROW_LOGIC_EXCEPTION("keyGroupsInLocalRange should be larger than 0.");
        }

        deduplicationSetsByKeyGroup.reserve(keyGroupsInLocalRange);
        int32_t deduplicationSetSize = 1 + minCapacity / keyGroupsInLocalRange;
        for (int32_t i = 0; i < keyGroupsInLocalRange; ++i) {
            deduplicationSetsByKeyGroup.push_back(std::make_shared<DedupSet>(deduplicationSetSize));
        }
    }

    T poll() override
    {
        auto toRemove = HeapPriorityQueue<T, Comparator>::poll();
        if (toRemove == nullptr) {
            return nullptr;
        }

        const auto& set = getDedupSetForElement(toRemove);
        if (set->erase(toRemove) == 0) {
            THROW_LOGIC_EXCEPTION("Element exists in priority queue but not in dedup set (data inconsistency)");
        }
        return toRemove;
    }

    bool add(const T& element) override
    {
        const auto& set = getDedupSetForElement(element);
        bool changed = false;
        // emplace() does the check and the insertion in a single step
        auto [iter, inserted] = set->emplace(element);
        if (inserted) {
            changed = HeapPriorityQueue<T, Comparator>::add(element);
        }
        return changed;
    }

    bool remove(const T& toRemove) override
    {
        const auto& set = getDedupSetForElement(toRemove);
        auto iter = set->find(toRemove);
        bool inSet = iter != set->end();
        if (inSet) {
            auto elementInPQ = *iter;
            set->erase(iter);
            return HeapPriorityQueue<T, Comparator>::remove(elementInPQ);
        }
        return false;
    }

    std::shared_ptr<DedupSet> getSubsetForKeyGroup(int32_t keyGroupId) override
    {
        return getDedupSetForKeyGroup(keyGroupId);
    }

private:
    int totalNumberOfKeyGroups;
    KeyGroupRange* keyGroupRange;
    std::vector<std::shared_ptr<DedupSet>> deduplicationSetsByKeyGroup;

    int globalKeyGroupToLocalIndex(int keyGroup) const
    {
        if (!keyGroupRange->contains(keyGroup)) {
            THROW_LOGIC_EXCEPTION(
                "Key group " + std::to_string(keyGroup) + " is not included in" + "[" +
                std::to_string(keyGroupRange->getStartKeyGroup()) + ", " +
                std::to_string(keyGroupRange->getEndKeyGroup()) + "]");
        }
        return keyGroup - keyGroupRange->getStartKeyGroup();
    }

    std::shared_ptr<DedupSet> getDedupSetForElement(const T& element)
    {
        const auto& key = element->getKey();
        int keyGroup = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, totalNumberOfKeyGroups);
        return getDedupSetForKeyGroup(keyGroup);
    }

    std::shared_ptr<DedupSet> getDedupSetForKeyGroup(int keyGroupId)
    {
        return deduplicationSetsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
    }
};
