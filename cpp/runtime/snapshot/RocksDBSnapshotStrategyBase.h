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

#ifndef OMNISTREAM_ROCKSDBSNAPSHOTSTRATEGYBASE_H
#define OMNISTREAM_ROCKSDBSNAPSHOTSTRATEGYBASE_H

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <jni.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/checkpoint.h>
#include "runtime/state/UUID.h"
#include "typeutils/TypeSerializer.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "state/SnapshotDirectory.h"
#include "state/SnapshotDirectoryFactory.h"
#include "state/LocalRecoveryDirectoryProviderImpl.h"
#include "state/IncrementalKeyedStateHandle.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"
#include "fs/CloseableRegistry.h"
#include "runtime/state/PlaceholderStreamStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "state/IncrementalRemoteKeyedStateHandle.h"
#include "state/IncrementalLocalKeyedStateHandle.h"
#include "runtime/state/SnapshotStrategyRunner.h"
#include "runtime/state/SnapshotStrategy.h"
#include "runtime/checkpoint/CheckpointListener.h"

using HandleAndLocalPath = IncrementalRemoteKeyedStateHandle::HandleAndLocalPath;

class PreviousSnapshot;
class NativeRocksDBSnapshotResources;

class RocksDBSnapshotStrategyBase : public CheckpointListener,
                                    public SnapshotStrategy<KeyedStateHandle, SnapshotResources>,
                                    public std::enable_shared_from_this<RocksDBSnapshotStrategyBase> {
public:
    RocksDBSnapshotStrategyBase(
        std::string description,
        rocksdb::DB* db,
        std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        KeyGroupRange keyGroupRange,
        int keyGroupPrefixBytes,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
        std::string instanceBasePath,
        UUID backendUID);

    std::string getDescription() const;
    std::shared_ptr<SnapshotResources> syncPrepareResources(long checkpointId);
    void cleanupIncompleteSnapshot(std::shared_ptr<SnapshotDirectory> localBackupDirectory);

    virtual std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>& snapshotResources,
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
        CheckpointOptions* checkpointOptions,
        std::string keySerializer = "") = 0;

protected:
    std::shared_ptr<RocksDBSnapshotStrategyBase> retainForAsyncSnapshot()
    {
        return shared_from_this();
    }

    virtual std::shared_ptr<PreviousSnapshot> snapshotMetaData(
        long checkpointId, std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots) = 0;

    std::shared_ptr<SnapshotResult<StreamStateHandle>> materializeMetaData(
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots,
        long checkpointId,
        CheckpointOptions* checkpointOptions,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge,
        std::string keySerializer = "")
    {
        return bridge->CallMaterializeMetaData(
            checkpointId, stateMetaInfoSnapshots, localRecoveryConfig_, checkpointOptions, keySerializer);
    };

    std::shared_ptr<SnapshotDirectory> prepareLocalSnapshotDirectory(long checkpointId);
    void takeDBNativeCheckpoint(std::shared_ptr<SnapshotDirectory> outputDirectory);

    // 成员变量
    std::string description_;
    rocksdb::DB* db_;
    std::shared_ptr<ResourceGuard> rocksDBResourceGuard_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation_;
    KeyGroupRange keyGroupRange_;
    int keyGroupPrefixBytes_;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_;
    std::string instanceBasePath_;
    std::string localDirectoryName_;
    UUID backendUID_;
    std::shared_ptr<TypeSerializer> keySerializer_;
    static const PreviousSnapshot EMPTY_PREVIOUS_SNAPSHOT;

    class RocksDBSnapshotOperation : public SnapshotResultSupplier<KeyedStateHandle> {
    public:
        RocksDBSnapshotOperation(
            long checkpointId,
            std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory,
            std::shared_ptr<SnapshotDirectory> localBackupDirectory,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots,
            std::shared_ptr<TypeSerializer> keySerializer);

        virtual ~RocksDBSnapshotOperation() = default;
        std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(
            std::shared_ptr<omnistream::OmniTaskBridge> bridge) override;

    protected:
        std::shared_ptr<KeyedStateHandle> getLocalSnapshot(
            const std::shared_ptr<RocksDBSnapshotStrategyBase>& strategy,
            std::shared_ptr<StreamStateHandle> localStreamStateHandle,
            std::vector<HandleAndLocalPath> sharedState);

        long checkpointId;
        std::shared_ptr<CheckpointStreamFactory> checkpointStreamFactory;
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots;
        std::shared_ptr<SnapshotDirectory> localBackupDirectory;
        std::shared_ptr<CloseableRegistry> tmpResourcesRegistry;
        std::shared_ptr<TypeSerializer> keySerializer;
    };
};

class PreviousSnapshot {
public:
    explicit PreviousSnapshot(std::vector<HandleAndLocalPath> confirmedSstFiles);
    std::shared_ptr<StreamStateHandle> getUploaded(const std::string& filename);
    static const std::shared_ptr<PreviousSnapshot> EMPTY_PREVIOUS_SNAPSHOT;

private:
    std::map<std::string, std::shared_ptr<StreamStateHandle>> confirmedSstFiles_;
};

// NativeRocksDBSnapshotResources 辅助类
class NativeRocksDBSnapshotResources : public SnapshotResources {
public:
    NativeRocksDBSnapshotResources(
        std::shared_ptr<SnapshotDirectory> snapshotDirectory,
        std::shared_ptr<PreviousSnapshot> previousSnapshot,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots);

    ~NativeRocksDBSnapshotResources() override;

    // SnapshotStrategyRunner owns SnapshotResources through this interface.
    // Keep release() as a compatibility alias, but make cleanup() the single
    // implementation point so temporary RocksDB checkpoint directories are
    // actually reclaimed after the asynchronous snapshot finishes.
    void release();

    void cleanup() override;

    std::shared_ptr<SnapshotDirectory> snapshotDirectory;
    std::shared_ptr<PreviousSnapshot> previousSnapshot;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots;
    std::shared_ptr<TypeSerializer> keySerializer;
};

class SnapshotResultSupplierEmpty : public SnapshotResultSupplier<KeyedStateHandle> {
public:
    std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge> bridge) override
    {
        return SnapshotResult<KeyedStateHandle>::Empty();
    }
};

class BridgeKeyedStateHandle : public DirectoryKeyedStateHandle {
public:
    // 用IncrementalLocalKeyedStateHandle初始化
    explicit BridgeKeyedStateHandle(std::shared_ptr<IncrementalLocalKeyedStateHandle> handle)
        : DirectoryKeyedStateHandle(handle->getDirectoryStateHandle(), handle->GetKeyGroupRange()),
          handle(handle) {};
    std::shared_ptr<IncrementalLocalKeyedStateHandle> handle;
};

#endif // OMNISTREAM_ROCKSDBSNAPSHOTSTRATEGYBASE_H
