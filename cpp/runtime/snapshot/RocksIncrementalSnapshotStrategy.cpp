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

#include "RocksIncrementalSnapshotStrategy.h"

constexpr const char* SST_FILE_SUFFIX = ".sst";

RocksIncrementalSnapshotStrategy::RocksIncrementalSnapshotStrategy(
    rocksdb::DB* db,
    std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
    std::shared_ptr<TypeSerializer> keySerializer,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    KeyGroupRange keyGroupRange,
    int keyGroupPrefixBytes,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
    const std::string& instanceBasePath,
    UUID backendUID,
    const std::map<long, std::vector<HandleAndLocalPath>>& uploadedStateHandles,
    std::shared_ptr<RocksDBStateUploader> rocksDBStateUploader,
    long lastCompletedCheckpointId)
    : RocksDBSnapshotStrategyBase(
          "Asynchronous incremental RocksDB snapshot",
          db,
          rocksDBResourceGuard,
          keySerializer,
          kvStateInformation,
          keyGroupRange,
          keyGroupPrefixBytes,
          localRecoveryConfig,
          instanceBasePath,
          backendUID),
      uploadedSstFiles_(uploadedStateHandles),
      lastCompletedCheckpointId_(lastCompletedCheckpointId),
      stateUploader_(rocksDBStateUploader)
{
}

std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> RocksIncrementalSnapshotStrategy::asyncSnapshot(
    const std::shared_ptr<SnapshotResources>& snapshotResources,
    long checkpointId,
    long timestamp,
    std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
    CheckpointOptions* checkpointOptions,
    std::string keySerializer)
{
    auto rocksdbSnapshotResources = static_cast<NativeRocksDBSnapshotResources*>(snapshotResources.get());

    if (rocksdbSnapshotResources->stateMetaInfoSnapshots.empty()) {
        return std::make_shared<SnapshotResultSupplierEmpty>();
    }

    auto sharingStrategy = checkpointOptions->GetCheckpointType()->GetSharingFilesStrategy();
    std::shared_ptr<PreviousSnapshot> previousSnapshot;

    switch (sharingStrategy) {
        case SnapshotType::SharingFilesStrategy::FORWARD_BACKWARD:
            previousSnapshot = rocksdbSnapshotResources->previousSnapshot;
            break;
        case SnapshotType::SharingFilesStrategy::FORWARD:
        case SnapshotType::SharingFilesStrategy::NO_SHARING:
            previousSnapshot = PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT;
            break;
        default: throw std::invalid_argument("Unsupported sharing files strategy");
    }
    auto snapshotDirectory = rocksdbSnapshotResources->snapshotDirectory;
    auto stateMetaInfoSnapshots = rocksdbSnapshotResources->stateMetaInfoSnapshots;

    return std::make_shared<RocksDBIncrementalSnapshotOperation>(
        std::static_pointer_cast<RocksIncrementalSnapshotStrategy>(retainForAsyncSnapshot()),
        checkpointId,
        checkpointStreamFactory,
        std::move(snapshotDirectory),
        std::move(previousSnapshot),
        sharingStrategy,
        std::move(stateMetaInfoSnapshots),
        checkpointOptions,
        keySerializer_);
}

void RocksIncrementalSnapshotStrategy::notifyCheckpointComplete(long completedCheckpointId)
{
    std::lock_guard<std::mutex> lock(uploadedSstFilesMutex_);
    if (completedCheckpointId > lastCompletedCheckpointId_ &&
        uploadedSstFiles_.find(completedCheckpointId) != uploadedSstFiles_.end()) {
        auto it = uploadedSstFiles_.begin();
        while (it != uploadedSstFiles_.end()) {
            if (it->first < completedCheckpointId) {
                it = uploadedSstFiles_.erase(it);
            } else {
                ++it;
            }
        }
        lastCompletedCheckpointId_ = completedCheckpointId;
    }
}

void RocksIncrementalSnapshotStrategy::notifyCheckpointAborted(long abortedCheckpointId)
{
    std::lock_guard<std::mutex> lock(uploadedSstFilesMutex_);
    uploadedSstFiles_.erase(abortedCheckpointId);
}

void RocksIncrementalSnapshotStrategy::close()
{
}

std::shared_ptr<PreviousSnapshot> RocksIncrementalSnapshotStrategy::snapshotMetaData(
    long checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots)
{
    long lastCompletedCheckpoint;
    std::vector<HandleAndLocalPath> confirmedSstFiles;

    {
        std::lock_guard<std::mutex> lock(uploadedSstFilesMutex_);
        lastCompletedCheckpoint = lastCompletedCheckpointId_;
        auto it = uploadedSstFiles_.find(lastCompletedCheckpoint);
        if (it != uploadedSstFiles_.end()) {
            confirmedSstFiles = it->second;
        }
    }

    for (auto kv : *kvStateInformation_) {
        stateMetaInfoSnapshots.push_back(kv.second->metaInfo_->snapshot());
    }

    return std::make_shared<PreviousSnapshot>(confirmedSstFiles);
}

// ================ RocksDBIncrementalSnapshotOperation ================
RocksIncrementalSnapshotStrategy::RocksDBIncrementalSnapshotOperation::RocksDBIncrementalSnapshotOperation(
    std::shared_ptr<RocksIncrementalSnapshotStrategy> parent,
    long checkpointId,
    std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
    std::shared_ptr<SnapshotDirectory> localBackupDirectory,
    std::shared_ptr<PreviousSnapshot> previousSnapshot,
    SnapshotType::SharingFilesStrategy sharingFilesStrategy,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<TypeSerializer> keySerializer)
    : RocksDBSnapshotOperation(
          checkpointId,
          checkpointStreamFactory,
          std::move(localBackupDirectory),
          std::move(stateMetaInfoSnapshots),
          std::move(keySerializer)),
      parent_(std::move(parent)),
      previousSnapshot_(std::move(previousSnapshot)),
      sharingFilesStrategy_(sharingFilesStrategy),
      checkpointOptions_(checkpointOptions)
{
}

std::shared_ptr<SnapshotResult<KeyedStateHandle>>
RocksIncrementalSnapshotStrategy::RocksDBIncrementalSnapshotOperation::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    bool completed = false;
    std::shared_ptr<SnapshotResult<StreamStateHandle>> metaStateHandle;
    std::vector<HandleAndLocalPath> sstFiles;
    std::vector<HandleAndLocalPath> miscFiles;

    try {
        // 1. 序列化元数据，内部反向调用jni
        metaStateHandle = parent_->materializeMetaData(
            stateMetaInfoSnapshots, checkpointId, checkpointOptions_, bridge, keySerializer->toJson());

        // 2. 上传文件
        long uploadedSize = uploadSnapshotFiles(sstFiles, miscFiles, bridge);

        // 3. 构建状态句柄
        auto jmHandle = std::make_shared<IncrementalRemoteKeyedStateHandle>(
            parent_->backendUID_,
            parent_->keyGroupRange_,
            checkpointId,
            sstFiles,
            miscFiles,
            metaStateHandle->GetJobManagerOwnedSnapshot(),
            metaStateHandle->GetStateSize() + uploadedSize);

        // 4. 创建本地快照（如果启用）
        auto localSnapshot = getLocalSnapshot(parent_, metaStateHandle->GetTaskLocalSnapshot(), sstFiles);

        // 5. 返回最终结果
        std::shared_ptr<SnapshotResult<KeyedStateHandle>> result;
        if (localSnapshot) {
            result = (jmHandle != nullptr) ? std::make_shared<SnapshotResult<KeyedStateHandle>>(jmHandle, localSnapshot)
                                           : std::make_shared<SnapshotResult<KeyedStateHandle>>(nullptr, nullptr);
        } else {
            result = std::make_shared<SnapshotResult<KeyedStateHandle>>(jmHandle, nullptr);
        }

        completed = true;
        return result;
    } catch (...) {
        if (!completed) {
            parent_->cleanupIncompleteSnapshot(localBackupDirectory);
        }
        throw;
    }
}

long RocksIncrementalSnapshotStrategy::RocksDBIncrementalSnapshotOperation::uploadSnapshotFiles(
    std::vector<HandleAndLocalPath>& sstFiles,
    std::vector<HandleAndLocalPath>& miscFiles,
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    if (!localBackupDirectory->exists()) {
        return 0;
    }

    auto files = localBackupDirectory->listDirectory();
    std::vector<fs::path> sstPathsToUpload;
    std::vector<fs::path> miscPathsToUpload;
    long totalSize = 0;

    // 分类处理文件
    createUploadFilePaths(files, sstFiles, sstPathsToUpload, miscPathsToUpload);

    // 上传SST文件，不在cpp侧上传，内部反向调用jni
    if (!sstPathsToUpload.empty()) {
        auto newSstHandles = parent_->stateUploader_->callUploadFilesToCheckpointFs(bridge, sstPathsToUpload);
        totalSize += std::accumulate(newSstHandles.begin(), newSstHandles.end(), 0L, [](long sum, const auto& handle) {
            return sum + handle.GetStateSize();
        });
        sstFiles.insert(sstFiles.end(), newSstHandles.begin(), newSstHandles.end());
    }

    // 上传其他文件，不在cpp侧上传，内部反向调用jni
    if (!miscPathsToUpload.empty()) {
        auto miscHandles = parent_->stateUploader_->callUploadFilesToCheckpointFs(bridge, miscPathsToUpload);
        totalSize += std::accumulate(miscHandles.begin(), miscHandles.end(), 0L, [](long sum, const auto& handle) {
            return sum + handle.GetStateSize();
        });
        miscFiles = std::move(miscHandles);
    }
    // 更新共享状态跟踪
    if (sharingFilesStrategy_ != SnapshotType::SharingFilesStrategy::NO_SHARING) {
        std::lock_guard<std::mutex> lock(parent_->uploadedSstFilesMutex_);
        parent_->uploadedSstFiles_[checkpointId] = sstFiles;
    }

    return totalSize;
}

void RocksIncrementalSnapshotStrategy::RocksDBIncrementalSnapshotOperation::createUploadFilePaths(
    const std::vector<path>& files,
    std::vector<HandleAndLocalPath>& sstFiles,
    std::vector<fs::path>& sstPathsToUpload,
    std::vector<fs::path>& miscPathsToUpload)
{
    for (const auto& filePath : files) {
        std::string fileName = filePath.filename().string();
        if (fileName.size() > 4 && fileName.compare(fileName.size() - 4, 4, SST_FILE_SUFFIX) == 0) {
            // 检查是否已存在历史版本
            auto uploaded = previousSnapshot_->getUploaded(fileName);
            if (uploaded) {
                sstFiles.push_back(HandleAndLocalPath::of(uploaded, fileName));
            } else {
                sstPathsToUpload.push_back(filePath);
            }
        } else {
            miscPathsToUpload.push_back(filePath);
        }
    }
}
