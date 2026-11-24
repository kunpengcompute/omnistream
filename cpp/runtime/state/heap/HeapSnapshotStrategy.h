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

#ifndef OMNISTREAM_HEAPSNAPSHOTSTRATEGY_H
#define OMNISTREAM_HEAPSNAPSHOTSTRATEGY_H

#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/FullSnapshotAsyncWriter.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/state/heap/HeapSnapshotResourceFactory.h"
#include "common.h"

/**
 * Checkpoint snapshot strategy for Heap state backend.
 *
 * Uses the same FullSnapshotAsyncWriter as the RocksDB full/savepoint strategies,
 * but gets its data from HeapFullSnapshotResources instead of RocksDB snapshots.
 *
 * The strategy is long-lived and owns the sync/async split of the heap snapshot
 * pipeline: syncPrepareResources() freezes a point-in-time resource view through
 * HeapSnapshotResourceFactory, and asyncSnapshot() wraps it in a FullSnapshotAsyncWriter.

 * Why SavepointType (canonical) is used here even for regular checkpoints:
 * FullSnapshotAsyncWriter always emits the canonical savepoint binary format
 * (END_OF_KEY_GROUP_MASK / FIRST_BIT_IN_BYTE_MASK framing). Flink's
 * HeapKeyedStateBackendBuilder routes restore via HeapSavepointRestoreOperation
 * (-> FullSnapshotRestoreOperation, which understands that format) only when
 * the handle implements SavepointKeyedStateHandle. Tagging the produced handle
 * with a SavepointType makes FullSnapshotAsyncWriter wrap the result in a
 * KeyGroupsSavepointStateHandle, so Flink-native restore from an OmniStream
 * heap checkpoint dispatches to the right reader instead of HeapRestoreOperation
 * (which expects the heap-specific per-key-group [int keyGroupId][short stateId]...
 * framing and would otherwise throw "Unexpected key-group in restore").
 */
template <typename K>
class HeapSnapshotStrategy : public SnapshotStrategy<KeyedStateHandle, FullSnapshotResources> {
public:
    explicit HeapSnapshotStrategy(const std::shared_ptr<HeapSnapshotResourceFactory<K>>& snapshotResourceFactory)
        : snapshotResourceFactory_(snapshotResourceFactory)
    {
    }

    std::shared_ptr<FullSnapshotResources> syncPrepareResources(long checkpointId) override
    {
        auto snapshotResources = snapshotResourceFactory_->createSnapshotResources(checkpointId);
        return snapshotResources;
    }

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<FullSnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override
    {
        if (snapshotResources->getMetaInfoSnapshots().empty()) {
            INFO_RELEASE("HeapSnapshotStrategy: no states to snapshot, returning empty");
            struct EmptySnapshotResultSupplier : public SnapshotResultSupplier<KeyedStateHandle> {
                std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(
                    std::shared_ptr<omnistream::OmniTaskBridge> bridge) override
                {
                    return SnapshotResult<KeyedStateHandle>::Empty();
                }
            };
            return std::make_shared<EmptySnapshotResultSupplier>();
        }

        return std::make_shared<FullSnapshotAsyncWriter>(
            SavepointType::savepoint(SavepointFormatType::CANONICAL),
            checkpointOptions,
            checkpointId,
            snapshotResources,
            keySerializer);
    }

private:
    std::shared_ptr<HeapSnapshotResourceFactory<K>> snapshotResourceFactory_;
};

#endif // OMNISTREAM_HEAPSNAPSHOTSTRATEGY_H
