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

#include "RocksDBSnapshotStrategyBase.h"
#include <exception>
#include <stdexcept>
#include <filesystem>
#include <utility>

namespace fs = std::filesystem;

// 静态常量定义
const PreviousSnapshot RocksDBSnapshotStrategyBase::EMPTY_PREVIOUS_SNAPSHOT =
    PreviousSnapshot(std::vector<HandleAndLocalPath>());

// 主类实现
RocksDBSnapshotStrategyBase::RocksDBSnapshotStrategyBase(
    std::string description,
    rocksdb::DB* db,
    std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
    std::shared_ptr<TypeSerializer> keySerializer,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    KeyGroupRange keyGroupRange,
    int keyGroupPrefixBytes,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
    std::string instanceBasePath,
    UUID backendUID)
    : description_(std::move(description)),
      db_(db),
      rocksDBResourceGuard_(std::move(rocksDBResourceGuard)),
      kvStateInformation_(kvStateInformation),
      keyGroupRange_(std::move(keyGroupRange)),
      keyGroupPrefixBytes_(keyGroupPrefixBytes),
      localRecoveryConfig_(std::move(localRecoveryConfig)),
      instanceBasePath_(std::move(instanceBasePath)),
      backendUID_(std::move(backendUID)),
      keySerializer_(std::move(keySerializer))
{
    // 替换UUID中的破折号
    std::string uid = this->backendUID_.ToString();
    uid.erase(std::remove(uid.begin(), uid.end(), '-'), uid.end());
    localDirectoryName_ = uid;
}

std::string RocksDBSnapshotStrategyBase::getDescription() const
{
    return description_;
}

std::shared_ptr<SnapshotResources> RocksDBSnapshotStrategyBase::syncPrepareResources(long checkpointId)
{
    auto snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots;
    stateMetaInfoSnapshots.reserve(kvStateInformation_->size());

    auto previousSnapshot = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);
    takeDBNativeCheckpoint(snapshotDirectory);

    return std::make_shared<NativeRocksDBSnapshotResources>(
        snapshotDirectory, previousSnapshot, stateMetaInfoSnapshots);
}

void RocksDBSnapshotStrategyBase::cleanupIncompleteSnapshot(std::shared_ptr<SnapshotDirectory> localBackupDirectory)
{
    if (localBackupDirectory == nullptr || !localBackupDirectory->exists()) {
        return;
    }

    const auto directory = localBackupDirectory->getDirectory().string();
    try {
        if (localBackupDirectory->isSnapshotCompleted()) {
            auto directoryStateHandle = localBackupDirectory->completeSnapshotAndGetHandle();
            if (directoryStateHandle) {
                directoryStateHandle->DiscardState();
            }
        } else {
            localBackupDirectory->cleanup();
        }
    } catch (const std::exception& e) {
        ERROR_RELEASE(
            "failed to clean incomplete RocksDB snapshot directory " << directory << ", exception=" << e.what());
    }
}

std::shared_ptr<SnapshotDirectory> RocksDBSnapshotStrategyBase::prepareLocalSnapshotDirectory(long checkpointId)
{
    if (localRecoveryConfig_->IsLocalRecoveryEnabled()) {
        auto directoryProvider = localRecoveryConfig_->GetLocalStateDirectoryProvider();
        fs::path directory = directoryProvider->SubtaskSpecificCheckpointDirectory(checkpointId);
        if (!fs::exists(directory)) {
            if (!fs::create_directories(directory)) {
                throw std::runtime_error("Failed to create directory: " + directory.string());
            }
        }

        fs::path rdbSnapshotDir = directory / localDirectoryName_;
        if (fs::exists(rdbSnapshotDir)) {
            FileUtils::deleteDirectory(rdbSnapshotDir);
        }

        try {
            return SnapshotDirectoryFactory::permanent(rdbSnapshotDir);
        } catch (const std::exception& e) {
            try {
                FileUtils::deleteDirectory(directory);
            } catch (const std::exception& ex) {
                LogError("Failed to deleteDirectory: %s", ex.what());
            }
            throw;
        }
    } else {
        fs::path snapshotDir = fs::path(instanceBasePath_) / ("chk-" + std::to_string(checkpointId));
        return SnapshotDirectoryFactory::temporary(snapshotDir);
    }
}

void RocksDBSnapshotStrategyBase::takeDBNativeCheckpoint(std::shared_ptr<SnapshotDirectory> outputDirectory)
{
    auto lease = rocksDBResourceGuard_->acquireResource();
    rocksdb::Checkpoint* checkpoint = nullptr;
    const auto chkPath = outputDirectory->getDirectory().string();

    try {
        auto status = rocksdb::Checkpoint::Create(db_, &checkpoint);
        if (!status.ok()) {
            throw std::runtime_error("Failed to create RocksDB Checkpoint object: " + status.ToString());
        }

        status = checkpoint->CreateCheckpoint(chkPath);
        if (!status.ok()) {
            throw std::runtime_error(
                "Failed to create native RocksDB checkpoint at " + chkPath + ": " + status.ToString());
        }
    } catch (const std::exception& ex) {
        ERROR_RELEASE(
            "native RocksDB checkpoint creation failed for directory " << chkPath << ", exception=" << ex.what());
        try {
            outputDirectory->cleanup();
        } catch (const std::exception& cleanupEx) {
            ERROR_RELEASE(
                "failed to clean native RocksDB checkpoint directory " << chkPath
                                                                       << ", exception=" << cleanupEx.what());
        }
        lease->close();
        delete lease;
        delete checkpoint;
        throw;
    }

    lease->close();
    delete lease;
    delete checkpoint;
}

// PreviousSnapshot 实现
PreviousSnapshot::PreviousSnapshot(std::vector<HandleAndLocalPath> confirmedSstFiles)
{
    for (auto& handle : confirmedSstFiles) {
        confirmedSstFiles_[handle.getLocalPath()] = handle.getHandle();
    }
}

std::shared_ptr<StreamStateHandle> PreviousSnapshot::getUploaded(const std::string& filename)
{
    auto it = confirmedSstFiles_.find(filename);
    if (it != confirmedSstFiles_.end()) {
        auto& handle = it->second;
        auto handleId = handle->GetStreamStateHandleID();
        auto handleIdPtr = std::make_unique<PhysicalStateHandleID>(handleId.getKeyString());
        return std::make_shared<PlaceholderStreamStateHandle>(std::move(handleIdPtr), handle->GetStateSize());
    }
    return nullptr;
}

const std::shared_ptr<PreviousSnapshot> PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT =
    std::make_shared<PreviousSnapshot>(std::vector<HandleAndLocalPath>());

// NativeRocksDBSnapshotResources 实现
NativeRocksDBSnapshotResources::NativeRocksDBSnapshotResources(
    std::shared_ptr<SnapshotDirectory> snapshotDirectory,
    std::shared_ptr<PreviousSnapshot> previousSnapshot,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots)
    : snapshotDirectory(std::move(snapshotDirectory)),
      previousSnapshot(std::move(previousSnapshot)),
      stateMetaInfoSnapshots(std::move(stateMetaInfoSnapshots))
{
}

NativeRocksDBSnapshotResources::~NativeRocksDBSnapshotResources()
{
    // The packaged task may be cancelled before its callable runs. In that
    // case destruction is the only lifecycle callback available to reclaim
    // the directory prepared by syncPrepareResources().
    cleanup();
}

void NativeRocksDBSnapshotResources::release()
{
    cleanup();
}

void NativeRocksDBSnapshotResources::cleanup()
{
    if (snapshotDirectory == nullptr) {
        return;
    }

    const auto directory = snapshotDirectory->getDirectory().string();
    try {
        if (snapshotDirectory->exists()) {
            snapshotDirectory->cleanup();
        }
    } catch (const std::exception& e) {
        ERROR_RELEASE("failed to clean RocksDB snapshot directory " << directory << ", exception=" << e.what());
    }
}

RocksDBSnapshotStrategyBase::RocksDBSnapshotOperation::RocksDBSnapshotOperation(
    long checkpointId,
    std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
    std::shared_ptr<SnapshotDirectory> localBackupDirectory,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
    std::shared_ptr<TypeSerializer> keySerializer)
    : checkpointId(checkpointId),
      checkpointStreamFactory(checkpointStreamFactory),
      stateMetaInfoSnapshots(std::move(stateMetaInfoSnapshots)),
      localBackupDirectory(std::move(localBackupDirectory)),
      keySerializer(std::move(keySerializer))
{
    tmpResourcesRegistry = std::make_shared<CloseableRegistry>();
}

std::shared_ptr<KeyedStateHandle> RocksDBSnapshotStrategyBase::RocksDBSnapshotOperation::getLocalSnapshot(
    const std::shared_ptr<RocksDBSnapshotStrategyBase>& strategy,
    std::shared_ptr<StreamStateHandle> localStreamStateHandle,
    std::vector<HandleAndLocalPath> sharedState)
{
    auto directoryStateHandle = localBackupDirectory->completeSnapshotAndGetHandle();
    if (directoryStateHandle && localStreamStateHandle) {
        auto stateHandle = std::make_shared<IncrementalLocalKeyedStateHandle>(
            strategy->backendUID_,
            checkpointId,
            directoryStateHandle.get(),
            strategy->keyGroupRange_,
            localStreamStateHandle,
            sharedState);
        return std::static_pointer_cast<KeyedStateHandle>(std::make_shared<BridgeKeyedStateHandle>(stateHandle));
    }
    return nullptr;
}

std::shared_ptr<SnapshotResult<KeyedStateHandle>> RocksDBSnapshotStrategyBase::RocksDBSnapshotOperation::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    return nullptr;
}
