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

#ifndef OMNISTREAM_ROCKSDBFULLRESTOREOPERATION_H
#define OMNISTREAM_ROCKSDBFULLRESTOREOPERATION_H

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <chrono>

#include "RocksDBRestoreOperation.h"
#include "FullSnapshotRestoreOperation.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace fs = std::filesystem;

/**
 * RocksDB full snapshot restore operation
 *
 * This class implements full snapshot restoration for RocksDB state backend.
 * It uses FullSnapshotRestoreOperation to handle the unified binary format
 * and then applies the restored data to RocksDB.
 */
template <typename K>
class RocksDBFullRestoreOperation : public RocksDBRestoreOperation {
public:
    /**
     * Constructor
     *
     * @param keyGroupRange range of key groups for this restore operation
     * @param keySerializer key serializer (not used but kept for API compatibility)
     * @param kvStateInformation map to store restored state information
     * @param instanceRocksDBPath path where RocksDB instance should be created
     * @param dbOptions RocksDB database options
     * @param columnFamilyOptionsFactory factory for column family options
     * @param restoreStateHandles collection of state handles to restore from
     * @param writeBatchSize write batch size for RocksDB operations
     * @param omniTaskBridge omniTaskBridge
     */
    RocksDBFullRestoreOperation(
        KeyGroupRange* keyGroupRange,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        fs::path& instanceRocksDBPath,
        std::shared_ptr<rocksdb::DBOptions> dbOptions,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
        const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
        long writeBatchSize,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge);

    /**
     * Destructor
     */
    ~RocksDBFullRestoreOperation() override = default;

    /**
     * Perform the restore operation
     *
     * @return RocksDBRestoreResult containing the restored database and metadata
     */
    std::shared_ptr<RocksDBRestoreResult> restore() override;

private:
    // RocksDB configuration
    const fs::path instanceRocksDBPath_;
    std::shared_ptr<rocksdb::DBOptions> dbOptions_;
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory_;

    // Restore state
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles_;
    long writeBatchSize_;

    // Full snapshot restore operation
    std::unique_ptr<FullSnapshotRestoreOperation<K>> savepointRestoreOperation_;

    // RocksDB Handle
    std::unique_ptr<RocksDbHandle> rocksDbHandle_;

    void applyRestoreResult(std::unique_ptr<SavepointRestoreResult> savepointRestoreResult);

    void restoreKVStateData(
        std::shared_ptr<KeyGroupIterator> keyGroups,
        std::unordered_map<int, rocksdb::ColumnFamilyHandle*> columnFamilyHandles);
};

// Template implementation
template <typename K>
RocksDBFullRestoreOperation<K>::RocksDBFullRestoreOperation(
    KeyGroupRange* keyGroupRange,
    std::shared_ptr<TypeSerializer> keySerializer,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    fs::path& instanceRocksDBPath,
    std::shared_ptr<rocksdb::DBOptions> dbOptions,
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
    const std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles,
    long writeBatchSize,
    std::shared_ptr<OmniTaskBridge> omniTaskBridge)
    : writeBatchSize_(writeBatchSize)
{
    // Create RocksDBHandle
    rocksDbHandle_ =
        std::make_unique<RocksDbHandle>(kvStateInformation, instanceRocksDBPath, dbOptions, columnFamilyOptionsFactory);

    // Create FullSnapshotRestoreOperation
    savepointRestoreOperation_ = std::make_unique<FullSnapshotRestoreOperation<K>>(
        keyGroupRange, restoreStateHandles, keySerializer, omniTaskBridge);
}

template <typename K>
std::shared_ptr<RocksDBRestoreResult> RocksDBFullRestoreOperation<K>::restore()
{
    try {
        auto start = std::chrono::high_resolution_clock::now();
        // Open or create RocksDB instance
        rocksDbHandle_->openDB();

        // Get the restore iterator from FullSnapshotRestoreOperation
        auto restoreIterator = savepointRestoreOperation_->restore();

        // Process all restore results
        while (restoreIterator->hasNext()) {
            auto restoreResult = restoreIterator->next();
            applyRestoreResult(std::move(restoreResult));
        }

        UUID empty_uid{};
        std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> empty_map{};
        auto end = std::chrono::high_resolution_clock::now();
        INFO_RELEASE(
            "Restore native task took " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                                        << " ms.");
        return std::make_shared<RocksDBRestoreResult>(
            rocksDbHandle_->getDb(), rocksDbHandle_->getDefaultColumnFamilyHandle(), -1L, empty_uid, empty_map);
    } catch (const std::exception& e) {
        // Clean up on error
        throw;
    }
}

template <typename K>
void RocksDBFullRestoreOperation<K>::applyRestoreResult(std::unique_ptr<SavepointRestoreResult> savepointRestoreResult)
{
    std::vector<StateMetaInfoSnapshot> restoredMetaInfos = savepointRestoreResult->getStateMetaInfoSnapshots();
    std::unordered_map<int, rocksdb::ColumnFamilyHandle*> columnFamilyHandles;
    for (size_t i = 0; i < restoredMetaInfos.size(); i++) {
        StateMetaInfoSnapshot restoredMetaInfo = restoredMetaInfos[i];
        auto registeredStateCFHandle = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, restoredMetaInfo);
        columnFamilyHandles.insert({i, registeredStateCFHandle->columnFamilyHandle_});
    }

    auto keyGroups = savepointRestoreResult->getKeyGroupIterator();
    restoreKVStateData(keyGroups, columnFamilyHandles);
}

template <typename K>
void RocksDBFullRestoreOperation<K>::restoreKVStateData(
    std::shared_ptr<KeyGroupIterator> keyGroups,
    std::unordered_map<int, rocksdb::ColumnFamilyHandle*> columnFamilyHandles)
{
    std::unique_ptr<RocksDBWriteBatchWrapper> rocksDbWriteBatchWrapper =
        std::make_unique<RocksDBWriteBatchWrapper>(rocksDbHandle_->getDb(), writeBatchSize_);
    rocksdb::ColumnFamilyHandle* handle;
    while (keyGroups->hasNext()) {
        std::unique_ptr<KeyGroup> keyGroup = keyGroups->next();
        std::shared_ptr<KeyGroupEntryIterator> groupEntries = keyGroup->getKeyGroupEntries();
        int oldKvStateId = -1;
        while (groupEntries->hasNext()) {
            KeyGroupEntry groupEntry = groupEntries->next();
            int kvStateId = groupEntry.getKvStateId();
            if (kvStateId != oldKvStateId) {
                oldKvStateId = kvStateId;
                handle = columnFamilyHandles[kvStateId];
            }
            rocksdb::Slice key(reinterpret_cast<const char*>(groupEntry.getKey().data()), groupEntry.getKey().size());
            rocksdb::Slice value(
                reinterpret_cast<const char*>(groupEntry.getValue().data()), groupEntry.getValue().size());
            rocksDbWriteBatchWrapper->Put(handle, key, value);
        }
    }
    rocksDbWriteBatchWrapper->Flush();
}

#endif // OMNISTREAM_ROCKSDBFULLRESTOREOPERATION_H
