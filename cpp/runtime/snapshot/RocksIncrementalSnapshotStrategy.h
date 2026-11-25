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

#ifndef OMNISTREAM_ROCKSINCREMENTALSNAPSHOTSTRATEGY_H
#define OMNISTREAM_ROCKSINCREMENTALSNAPSHOTSTRATEGY_H

#include <filesystem>
#include <algorithm>
#include <map>
#include <vector>
#include <mutex>
#include <atomic>
#include <memory>
#include "RocksDBSnapshotStrategyBase.h"
#include "runtime/state/rocksdb/RocksDBStateUploader.h"
#include "fs/CloseableRegistry.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"
#include "checkpoint/CheckpointOptions.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"

using namespace std::filesystem;
using HandleAndLocalPath = IncrementalRemoteKeyedStateHandle::HandleAndLocalPath;

class RocksIncrementalSnapshotStrategy : public RocksDBSnapshotStrategyBase {
public:
    RocksIncrementalSnapshotStrategy(
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
        long lastCompletedCheckpointId);

    std::shared_ptr<SnapshotResources> syncPrepareResources(long checkpointId)
    {
        return RocksDBSnapshotStrategyBase::syncPrepareResources(checkpointId);
    };

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override;

    void notifyCheckpointComplete(long completedCheckpointId);
    void notifyCheckpointAborted(long abortedCheckpointId);
    void close();

protected:
    std::shared_ptr<PreviousSnapshot> snapshotMetaData(
        long checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots) override;

    class RocksDBIncrementalSnapshotOperation : public RocksDBSnapshotStrategyBase::RocksDBSnapshotOperation {
    public:
        RocksDBIncrementalSnapshotOperation(
            std::shared_ptr<RocksIncrementalSnapshotStrategy> parent,
            long checkpointId,
            std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
            std::shared_ptr<SnapshotDirectory> localBackupDirectory,
            std::shared_ptr<PreviousSnapshot> previousSnapshot,
            SnapshotType::SharingFilesStrategy sharingFilesStrategy,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
            CheckpointOptions* checkpointOptions,
            std::shared_ptr<TypeSerializer> keySerializer);

        std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(
            std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;

    private:
        long uploadSnapshotFiles(
            std::vector<HandleAndLocalPath>& sstFiles,
            std::vector<HandleAndLocalPath>& miscFiles,
            std::shared_ptr<omnistream::OmniTaskBridge> bridge);

        void createUploadFilePaths(
            const std::vector<path>& files,
            std::vector<HandleAndLocalPath>& sstFiles,
            std::vector<fs::path>& sstFilePaths,
            std::vector<fs::path>& miscFilePaths);

        std::shared_ptr<RocksIncrementalSnapshotStrategy> parent_;
        std::shared_ptr<PreviousSnapshot> previousSnapshot_;
        SnapshotType::SharingFilesStrategy sharingFilesStrategy_;
        CheckpointOptions* checkpointOptions_;
    };

private:
    std::mutex uploadedSstFilesMutex_;
    std::map<long, std::vector<HandleAndLocalPath>> uploadedSstFiles_;
    std::atomic<long> lastCompletedCheckpointId_;
    std::shared_ptr<RocksDBStateUploader> stateUploader_;
};

#endif // OMNISTREAM_ROCKSINCREMENTALSNAPSHOTSTRATEGY_H
