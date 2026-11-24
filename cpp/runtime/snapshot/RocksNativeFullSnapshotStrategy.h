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

#ifndef OMNISTREAM_ROCKSNATIVEFULLSNAPSHOTSTRATEGY_H
#define OMNISTREAM_ROCKSNATIVEFULLSNAPSHOTSTRATEGY_H

#include "RocksDBSnapshotStrategyBase.h"
#include "state/rocksdb/RocksDBStateUploader.h"
#include "RocksIncrementalSnapshotStrategy.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <functional>
#include <filesystem>
#include <rocksdb/db.h>

namespace fs = std::filesystem;

class RocksNativeFullSnapshotStrategy : public RocksDBSnapshotStrategyBase {
public:
    RocksNativeFullSnapshotStrategy(
        rocksdb::DB* db,
        std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        KeyGroupRange keyGroupRange,
        int keyGroupPrefixBytes,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        const fs::path& instanceBasePath,
        UUID backendUID,
        std::shared_ptr<RocksDBStateUploader> rocksDBStateUploader);

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") override;

    void notifyCheckpointComplete(int64_t completedCheckpointId);
    void notifyCheckpointAborted(int64_t abortedCheckpointId);
    void close();

protected:
    std::shared_ptr<PreviousSnapshot> snapshotMetaData(
        int64_t checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots) override;

private:
    std::shared_ptr<RocksDBStateUploader> stateUploader;

    class RocksDBNativeFullSnapshotOperation : public RocksDBSnapshotOperation {
    public:
        RocksDBNativeFullSnapshotOperation(
            int64_t checkpointId,
            std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
            std::shared_ptr<SnapshotDirectory> localBackupDirectory,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
            UUID backendUID,
            KeyGroupRange keyGroupRange,
            std::shared_ptr<RocksNativeFullSnapshotStrategy> outerStrategy,
            CheckpointOptions* checkpointOptions,
            std::shared_ptr<TypeSerializer> keySerializer);

        std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(
            std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;

    private:
        int64_t uploadSnapshotFiles(
            std::vector<HandleAndLocalPath>& privateFiles, std::shared_ptr<omnistream::OmniTaskBridge> bridge);

        UUID backendUID_;
        KeyGroupRange keyGroupRange_;
        std::shared_ptr<RocksNativeFullSnapshotStrategy> outerStrategy_;
        CheckpointOptions* checkpointOptions_;
    };
};

#endif // OMNISTREAM_ROCKSNATIVEFULLSNAPSHOTSTRATEGY_H
