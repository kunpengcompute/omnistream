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

#include "RocksNativeFullSnapshotStrategy.h"
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

RocksNativeFullSnapshotStrategy::RocksNativeFullSnapshotStrategy(
    rocksdb::DB* db,
    std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
    std::shared_ptr<TypeSerializer> keySerializer,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    KeyGroupRange keyGroupRange,
    int keyGroupPrefixBytes,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
    const fs::path& instanceBasePath,
    UUID backendUID,
    std::shared_ptr<RocksDBStateUploader> rocksDBStateUploader)
    : RocksDBSnapshotStrategyBase(
          "Asynchronous full RocksDB snapshot",
          db,
          rocksDBResourceGuard,
          keySerializer,
          kvStateInformation,
          keyGroupRange,
          keyGroupPrefixBytes,
          localRecoveryConfig,
          instanceBasePath,
          backendUID),
      stateUploader(rocksDBStateUploader)
{
}

std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> RocksNativeFullSnapshotStrategy::asyncSnapshot(
    const std::shared_ptr<SnapshotResources>& snapshotResources,
    long checkpointId,
    long timestamp,
    std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
    CheckpointOptions* checkpointOptions,
    std::string keySerializer)
{
    LOG("RocksNativeFullSnapshotStrategy::asyncSnapshot");
    auto rocksdbSnapshotResources = static_cast<NativeRocksDBSnapshotResources*>(snapshotResources.get());

    if (rocksdbSnapshotResources->stateMetaInfoSnapshots.empty()) {
        return std::make_shared<SnapshotResultSupplierEmpty>();
    }

    auto snapshotDirectory = rocksdbSnapshotResources->snapshotDirectory;
    auto stateMetaInfoSnapshots = rocksdbSnapshotResources->stateMetaInfoSnapshots;

    return std::static_pointer_cast<SnapshotResultSupplier<KeyedStateHandle>>(
        std::make_shared<RocksDBNativeFullSnapshotOperation>(
            checkpointId,
            checkpointStreamFactory,
            std::move(snapshotDirectory),
            std::move(stateMetaInfoSnapshots),
            backendUID_,
            keyGroupRange_,
            std::static_pointer_cast<RocksNativeFullSnapshotStrategy>(retainForAsyncSnapshot()),
            checkpointOptions,
            keySerializer_));
}

void RocksNativeFullSnapshotStrategy::notifyCheckpointComplete(int64_t completedCheckpointId)
{
}

void RocksNativeFullSnapshotStrategy::notifyCheckpointAborted(int64_t abortedCheckpointId)
{
}

void RocksNativeFullSnapshotStrategy::close()
{
}

std::shared_ptr<PreviousSnapshot> RocksNativeFullSnapshotStrategy::snapshotMetaData(
    int64_t checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots)
{
    for (const auto& kv : *kvStateInformation_) {
        stateMetaInfoSnapshots.push_back(kv.second->metaInfo_->snapshot());
    }

    return PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT;
}

// RocksDBNativeFullSnapshotOperation implementation
RocksNativeFullSnapshotStrategy::RocksDBNativeFullSnapshotOperation::RocksDBNativeFullSnapshotOperation(
    int64_t checkpointId,
    std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
    std::shared_ptr<SnapshotDirectory> localBackupDirectory,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
    UUID backendUID,
    KeyGroupRange keyGroupRange,
    std::shared_ptr<RocksNativeFullSnapshotStrategy> outerStrategy,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<TypeSerializer> keySerializer)
    : RocksDBSnapshotOperation(
          checkpointId,
          checkpointStreamFactory,
          std::move(localBackupDirectory),
          std::move(stateMetaInfoSnapshots),
          std::move(keySerializer)),
      backendUID_(backendUID),
      keyGroupRange_(keyGroupRange),
      outerStrategy_(std::move(outerStrategy)),
      checkpointOptions_(checkpointOptions)
{
}

std::shared_ptr<SnapshotResult<KeyedStateHandle>>
RocksNativeFullSnapshotStrategy::RocksDBNativeFullSnapshotOperation::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    bool completed = false;
    std::shared_ptr<SnapshotResult<StreamStateHandle>> metaStateHandle;
    std::vector<HandleAndLocalPath> privateFiles;
    std::shared_ptr<CloseableRegistry> tmpResourcesRegistry = std::make_shared<CloseableRegistry>();
    try {
        metaStateHandle = outerStrategy_->materializeMetaData(
            stateMetaInfoSnapshots, checkpointId, checkpointOptions_, bridge, keySerializer->toJson());

        int64_t checkpointedSize = metaStateHandle->GetStateSize();
        checkpointedSize += uploadSnapshotFiles(privateFiles, bridge);

        auto jmIncrementalKeyedStateHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
            backendUID_,
            keyGroupRange_,
            checkpointId,
            std::vector<HandleAndLocalPath>(),
            privateFiles,
            metaStateHandle->GetJobManagerOwnedSnapshot(),
            checkpointedSize);

        auto localSnapshot = getLocalSnapshot(
            outerStrategy_, metaStateHandle->GetTaskLocalSnapshot(), std::vector<HandleAndLocalPath>());

        std::shared_ptr<SnapshotResult<KeyedStateHandle>> result;
        if (localSnapshot) {
            result =
                (jmIncrementalKeyedStateHandle != nullptr)
                    ? std::make_shared<SnapshotResult<KeyedStateHandle>>(jmIncrementalKeyedStateHandle, localSnapshot)
                    : std::make_shared<SnapshotResult<KeyedStateHandle>>(nullptr, nullptr);
        } else {
            result = std::make_shared<SnapshotResult<KeyedStateHandle>>(jmIncrementalKeyedStateHandle, nullptr);
        }

        completed = true;
        return result;
    } catch (...) {
        if (!completed) {
            outerStrategy_->cleanupIncompleteSnapshot(localBackupDirectory);
        }
        throw;
    }
}

int64_t RocksNativeFullSnapshotStrategy::RocksDBNativeFullSnapshotOperation::uploadSnapshotFiles(
    std::vector<HandleAndLocalPath>& privateFiles, std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    auto files = localBackupDirectory->listDirectory();
    int64_t uploadedSize = 0;

    if (!files.empty()) {
        std::vector<fs::path> filePaths;
        filePaths.reserve(files.size());
        for (const auto& file : files) {
            filePaths.emplace_back(file);
        }

        auto uploadedFiles = outerStrategy_->stateUploader->callUploadFilesToCheckpointFs(bridge, filePaths);

        uploadedSize += std::accumulate(
            uploadedFiles.begin(), uploadedFiles.end(), 0LL, [](int64_t sum, const HandleAndLocalPath& handle) {
                return sum + handle.GetStateSize();
            });

        privateFiles.reserve(privateFiles.size() + uploadedFiles.size());
        std::move(uploadedFiles.begin(), uploadedFiles.end(), std::back_inserter(privateFiles));
    }
    return uploadedSize;
}
