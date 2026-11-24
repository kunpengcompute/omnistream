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

#ifndef OMNISTREAM_HEAPFULLSNAPSHOTRESOURCES_H
#define OMNISTREAM_HEAPFULLSNAPSHOTRESOURCES_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"
#include "runtime/state/rocksdb/iterator/RocksStatesPerKeyGroupMergeIterator.h"
#include "core/fs/CloseableRegistry.h"
#include "core/typeutils/TypeSerializer.h"
#include "common.h"

/**
 * FullSnapshotResources implementation for Heap state backend.
 *
 * Holds sync-phase prepared SingleStateIterator instances whose serialized
 * entries are already materialized from the live Heap state tables.
 * The createKVStateIterator() method only stitches these frozen iterators into
 * a unified, key-group-ordered async iteration over all states.
 *
 * This class is analogous to RocksDBFullSnapshotResources but operates on
 * in-memory Heap state tables instead of RocksDB column families.
 */
class HeapFullSnapshotResources : public FullSnapshotResources {
public:
    HeapFullSnapshotResources(
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
        std::vector<std::unique_ptr<SingleStateIterator>> stateIterators,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int keyGroupPrefixBytes)
        : stateMetaInfoSnapshots_(std::move(stateMetaInfoSnapshots)),
          stateIterators_(std::move(stateIterators)),
          keyGroupRange_(keyGroupRange),
          keySerializer_(keySerializer),
          keyGroupPrefixBytes_(keyGroupPrefixBytes)
    {
    }

    ~HeapFullSnapshotResources() override = default;

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return stateMetaInfoSnapshots_;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return keyGroupRange_;
    }

    TypeSerializer* getKeySerializer() override
    {
        return keySerializer_;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        auto closeableRegistry = std::make_unique<CloseableRegistry>();

        // RocksStatesPerKeyGroupMergeIterator accepts two types of iterators.
        // For Heap, we pass the sync-phase frozen HeapSingleStateIterators as
        // heapPriorityQueueIterators (both use the SingleStateIterator interface).
        std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>> emptyRocksIterators;
        std::vector<std::unique_ptr<SingleStateIterator>> heapIterators;

        for (auto& iter : stateIterators_) {
            if (iter && iter->isValid()) {
                heapIterators.push_back(std::move(iter));
            }
        }
        stateIterators_.clear();

        return std::make_shared<RocksStatesPerKeyGroupMergeIterator>(
            std::move(closeableRegistry), emptyRocksIterators, heapIterators, keyGroupPrefixBytes_);
    }

    void cleanup() override
    {
        stateIterators_.clear();
    }

private:
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots_;
    std::vector<std::unique_ptr<SingleStateIterator>> stateIterators_;
    KeyGroupRange* keyGroupRange_;
    TypeSerializer* keySerializer_;
    int keyGroupPrefixBytes_;
};

#endif // OMNISTREAM_HEAPFULLSNAPSHOTRESOURCES_H
