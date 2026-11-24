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

#ifndef OMNISTREAM_ROCKSDBHEAPTIMERSFULLRESTOREOPERATION_H
#define OMNISTREAM_ROCKSDBHEAPTIMERSFULLRESTOREOPERATION_H

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "RocksDBRestoreOperation.h"
#include "FullSnapshotRestoreOperation.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace fs = std::filesystem;

/**
 * RocksDB full-snapshot restore operation for HEAP priority queues.
 *
 * This mirrors Flink's RocksDBHeapTimersFullRestoreOperation: KEY_VALUE state is
 * restored into RocksDB column families, while PRIORITY_QUEUE entries from a
 * savepoint are restored into heap priority queues. Because the concrete timer
 * namespace type is only known when the timer service is created in this C++
 * implementation, priority queue entries are first stored in a pending wrapper
 * and drained into the typed heap queue from HeapPriorityQueuesManager.
 */
template <typename K>
class RocksDBHeapTimersFullRestoreOperation : public RocksDBRestoreOperation {
public:
    RocksDBHeapTimersFullRestoreOperation(
        KeyGroupRange* keyGroupRange,
        int numberOfKeyGroups,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
            registeredPQStates,
        fs::path& instanceRocksDBPath,
        std::shared_ptr<rocksdb::DBOptions> dbOptions,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
        const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
        long writeBatchSize,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge);

    ~RocksDBHeapTimersFullRestoreOperation() override = default;

    std::shared_ptr<RocksDBRestoreResult> restore() override;

private:
    KeyGroupRange* keyGroupRange_;
    int numberOfKeyGroups_;
    int keyGroupPrefixBytes_;
    long writeBatchSize_;

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
        registeredPQStates_;
    std::unique_ptr<FullSnapshotRestoreOperation<K>> savepointRestoreOperation_;
    std::unique_ptr<RocksDbHandle> rocksDbHandle_;

    void applyRestoreResult(std::unique_ptr<SavepointRestoreResult> savepointRestoreResult);

    void restoreKVStateData(
        std::shared_ptr<KeyGroupIterator> keyGroups,
        std::unordered_map<int, rocksdb::ColumnFamilyHandle*> columnFamilyHandles,
        std::unordered_map<int, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>> restoredPQStates);

    std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> getOrCreatePendingPriorityQueueState(
        const StateMetaInfoSnapshot& restoredMetaInfo);
};

template <typename K>
RocksDBHeapTimersFullRestoreOperation<K>::RocksDBHeapTimersFullRestoreOperation(
    KeyGroupRange* keyGroupRange,
    int numberOfKeyGroups,
    std::shared_ptr<TypeSerializer> keySerializer,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
        registeredPQStates,
    fs::path& instanceRocksDBPath,
    std::shared_ptr<rocksdb::DBOptions> dbOptions,
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
    const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
    long writeBatchSize,
    std::shared_ptr<OmniTaskBridge> omniTaskBridge)
    : keyGroupRange_(keyGroupRange),
      numberOfKeyGroups_(numberOfKeyGroups),
      keyGroupPrefixBytes_(CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups)),
      writeBatchSize_(writeBatchSize),
      registeredPQStates_(std::move(registeredPQStates))
{
    rocksDbHandle_ =
        std::make_unique<RocksDbHandle>(kvStateInformation, instanceRocksDBPath, dbOptions, columnFamilyOptionsFactory);

    savepointRestoreOperation_ = std::make_unique<FullSnapshotRestoreOperation<K>>(
        keyGroupRange, restoreStateHandles, keySerializer, omniTaskBridge);

    if (registeredPQStates_ == nullptr) {
        registeredPQStates_ = std::make_shared<
            std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>();
    }
}

template <typename K>
std::shared_ptr<RocksDBRestoreResult> RocksDBHeapTimersFullRestoreOperation<K>::restore()
{
    auto start = std::chrono::high_resolution_clock::now();
    rocksDbHandle_->openDB();

    auto restoreIterator = savepointRestoreOperation_->restore();
    while (restoreIterator->hasNext()) {
        applyRestoreResult(restoreIterator->next());
    }

    UUID emptyUid{};
    std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> emptyMap{};
    auto end = std::chrono::high_resolution_clock::now();
    INFO_RELEASE(
        "RocksDBHeapTimersFullRestoreOperation: restore took "
        << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
        << " ms, pendingPQStates=" << registeredPQStates_->size());

    return std::make_shared<RocksDBRestoreResult>(
        rocksDbHandle_->getDb(), rocksDbHandle_->getDefaultColumnFamilyHandle(), -1L, emptyUid, emptyMap);
}

template <typename K>
void RocksDBHeapTimersFullRestoreOperation<K>::applyRestoreResult(
    std::unique_ptr<SavepointRestoreResult> savepointRestoreResult)
{
    const std::vector<StateMetaInfoSnapshot>& restoredMetaInfos = savepointRestoreResult->getStateMetaInfoSnapshots();

    std::unordered_map<int, rocksdb::ColumnFamilyHandle*> columnFamilyHandles;
    std::unordered_map<int, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>> restoredPQStates;

    for (size_t i = 0; i < restoredMetaInfos.size(); i++) {
        const StateMetaInfoSnapshot& restoredMetaInfo = restoredMetaInfos[i];
        if (restoredMetaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
            auto restoredPQ = getOrCreatePendingPriorityQueueState(restoredMetaInfo);
            restoredPQStates.emplace(static_cast<int>(i), restoredPQ);
            INFO_RELEASE(
                "RocksDBHeapTimersFullRestoreOperation: discovered PRIORITY_QUEUE state '" << restoredMetaInfo.getName()
                                                                                           << "' at kvStateId=" << i);
            continue;
        }

        auto registeredStateCFHandle = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, restoredMetaInfo);
        columnFamilyHandles.emplace(static_cast<int>(i), registeredStateCFHandle->columnFamilyHandle_);
    }

    restoreKVStateData(savepointRestoreResult->getKeyGroupIterator(), columnFamilyHandles, restoredPQStates);
}

template <typename K>
std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>
RocksDBHeapTimersFullRestoreOperation<K>::getOrCreatePendingPriorityQueueState(
    const StateMetaInfoSnapshot& restoredMetaInfo)
{
    const std::string& stateName = restoredMetaInfo.getName();
    auto existing = registeredPQStates_->find(stateName);
    if (existing != registeredPQStates_->end()) {
        return existing->second;
    }

    auto metaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(restoredMetaInfo);
    auto pendingWrapper = std::make_shared<RestoredHeapPriorityQueueSnapshotRestoreWrapper>(metaInfo);
    registeredPQStates_->emplace(stateName, pendingWrapper);
    return pendingWrapper;
}

template <typename K>
void RocksDBHeapTimersFullRestoreOperation<K>::restoreKVStateData(
    std::shared_ptr<KeyGroupIterator> keyGroups,
    std::unordered_map<int, rocksdb::ColumnFamilyHandle*> columnFamilyHandles,
    std::unordered_map<int, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>> restoredPQStates)
{
    std::unique_ptr<RocksDBWriteBatchWrapper> rocksDbWriteBatchWrapper =
        std::make_unique<RocksDBWriteBatchWrapper>(rocksDbHandle_->getDb(), writeBatchSize_);

    rocksdb::ColumnFamilyHandle* handle = nullptr;
    std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> restoredPQ;
    int oldKvStateId = -1;
    int restoredKvEntries = 0;
    int restoredPQEntries = 0;

    while (keyGroups->hasNext()) {
        std::unique_ptr<KeyGroup> keyGroup = keyGroups->next();
        std::shared_ptr<KeyGroupEntryIterator> groupEntries = keyGroup->getKeyGroupEntries();
        while (groupEntries->hasNext()) {
            KeyGroupEntry groupEntry = groupEntries->next();
            int kvStateId = groupEntry.getKvStateId();

            if (kvStateId != oldKvStateId) {
                oldKvStateId = kvStateId;
                auto pqIt = restoredPQStates.find(kvStateId);
                restoredPQ = pqIt == restoredPQStates.end() ? nullptr : pqIt->second;

                auto cfIt = columnFamilyHandles.find(kvStateId);
                handle = cfIt == columnFamilyHandles.end() ? nullptr : cfIt->second;
            }

            if (restoredPQ != nullptr) {
                restoredPQ->restoreSerializedElement(groupEntry.getKey(), keyGroupPrefixBytes_);
                restoredPQEntries++;
                continue;
            }

            if (handle != nullptr) {
                rocksdb::Slice key(
                    reinterpret_cast<const char*>(groupEntry.getKey().data()), groupEntry.getKey().size());
                rocksdb::Slice value(
                    reinterpret_cast<const char*>(groupEntry.getValue().data()), groupEntry.getValue().size());
                rocksDbWriteBatchWrapper->Put(handle, key, value);
                restoredKvEntries++;
                continue;
            }

            INFO_RELEASE(
                "Error: restoreKVStateData Unknown savepoint state id during RocksDB heap timers restore: "
                << kvStateId);
            THROW_LOGIC_EXCEPTION("Unknown savepoint state id during RocksDB heap timers restore: " << kvStateId);
        }
    }

    rocksDbWriteBatchWrapper->Flush();
    INFO_RELEASE(
        "RocksDBHeapTimersFullRestoreOperation: restored KV entries=" << restoredKvEntries
                                                                      << ", pending PQ entries=" << restoredPQEntries);
}

#endif // OMNISTREAM_ROCKSDBHEAPTIMERSFULLRESTOREOPERATION_H
