/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include "PriorityQueueSetFactory.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "core/fs/CloseableRegistry.h"

#include "state/RocksDBWriteBatchWrapper.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"

#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/RocksdbKeyedStateBackend.h"
#include "runtime/state/RocksDBResourceContainer.h"
#include "runtime/state/restore/RocksDBRestoreOperation.h"
#include "runtime/state/restore/RocksDBNoneRestoreOperation.h"
#include "runtime/state/restore/RocksDBIncrementalRestoreOperation.h"
#include "runtime/state/restore/RocksDBCompatibleFullRestoreOperation.h"
#include "runtime/state/restore/RocksDBFullRestoreOperation.h"
#include "runtime/state/restore/RocksDBHeapTimersFullRestoreOperation.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptorFactory.h"
#include "runtime/snapshot/RocksDBSnapshotStrategyBase.h"
#include "runtime/snapshot/RocksNativeFullSnapshotStrategy.h"
#include "runtime/snapshot/RocksIncrementalSnapshotStrategy.h"
#include "runtime/state/rocksdb/RocksDBStateUploader.h"
#include "runtime/state/StateRestoreValidation.h"

namespace fs = std::filesystem;
template <typename K>
class RocksDBKeyedStateBackendBuilder {
public:
    enum class PriorityQueueStateType {
        HEAP,
        ROCKSDB
    };

    RocksDBKeyedStateBackendBuilder(
        std::string operatorIdentifier_,
        fs::path instanceBasePath_,
        std::shared_ptr<RocksDBResourceContainer> optionsContainer_,
        TypeSerializer* keySerializer_,
        int numberOfKeyGroups_,
        KeyGroupRange* keyGroupRange_,
        std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig_,
        PriorityQueueStateType priorityQueueStateType,
        std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles_,
        std::shared_ptr<TaskStateManagerBridge> bridge_,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge_,
        std::shared_ptr<OperatorID> operatorId,
        int alternativeIdx)
        : keySerializer(keySerializer_, [](TypeSerializer*) {}),
          operatorIdentifier(operatorIdentifier_),
          instanceBasePath(instanceBasePath_),
          optionsContainer(optionsContainer_),
          numberOfKeyGroups(numberOfKeyGroups_),
          keyGroupRange(keyGroupRange_),
          localRecoveryConfig(localRecoveryConfig_),
          priorityQueueStateType_(priorityQueueStateType),
          restoreStateHandles(stateHandles_),
          bridge(bridge_),
          omniTaskBridge(omniTaskBridge_),
          operatorId_(operatorId),
          alternativeIdx_(alternativeIdx)
    {
        instanceRocksDBPath = getInstanceRocksDBPath(instanceBasePath);
    }

    RocksdbKeyedStateBackend<K>* build();

    static std::filesystem::path getInstanceRocksDBPath(const std::filesystem::path& instanceBasePath)
    {
        return instanceBasePath / DB_INSTANCE_DIR_STRING;
    }

    std::shared_ptr<TypeSerializer> keySerializer;

    RocksDBKeyedStateBackendBuilder<K>& setEnableIncrementalCheckpointing(bool enableIncrementalCheckpointing_)
    {
        this->enableIncrementalCheckpointing = enableIncrementalCheckpointing_;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setNumberOfTransferringThreads(int numberOfTransferingThreads_)
    {
        this->numberOfTransferingThreads = numberOfTransferingThreads_;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setWriteBatchSize(long writeBatchSize_)
    {
        this->writeBatchSize = writeBatchSize_;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setFlinkSavepointAdaptorInfo(const FlinkSavepointAdaptorInfo& adaptorInfo)
    {
        this->adaptorInfo_ = adaptorInfo;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setRestoreSavepointMode(RestoreSavepointMode restoreMode)
    {
        this->restoreMode_ = restoreMode;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setOperatorDescription(const nlohmann::json& operatorDescription)
    {
        this->operatorDescription_ = operatorDescription;
        return *this;
    }

    RocksDBKeyedStateBackendBuilder<K>& setTaskType(int taskType)
    {
        this->taskType_ = taskType;
        return *this;
    }

private:
    const std::shared_ptr<CloseableRegistry> cancelStreamRegistry;
    static constexpr const char* DB_INSTANCE_DIR_STRING = "db";
    std::string operatorIdentifier;
    std::filesystem::path instanceBasePath;
    std::shared_ptr<RocksDBResourceContainer> optionsContainer;
    int numberOfKeyGroups;
    KeyGroupRange* keyGroupRange;
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig;
    PriorityQueueStateType priorityQueueStateType_ = PriorityQueueStateType::ROCKSDB;
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles;
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory;
    std::filesystem::path instanceRocksDBPath;
    bool enableIncrementalCheckpointing;
    int numberOfTransferingThreads;
    long writeBatchSize = 2097152;
    std::shared_ptr<rocksdb::DB> injectedTestDB;
    std::shared_ptr<TaskStateManagerBridge> bridge;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge;
    std::shared_ptr<OperatorID> operatorId_;
    int alternativeIdx_;
    FlinkSavepointAdaptorInfo adaptorInfo_;
    RestoreSavepointMode restoreMode_ = RestoreSavepointMode::OMNI_INTERNAL;
    int taskType_ = 1;
    nlohmann::json operatorDescription_;

    static void checkAndCreateDirectory(const fs::path& directory)
    {
        if (fs::exists(directory)) {
            if (!fs::is_directory(directory)) {
                throw std::runtime_error("Not a directory: " + directory.string());
            }
        } else {
            if (!fs::create_directories(directory)) {
                throw std::runtime_error("Could not create RocksDB data directory at " + directory.string());
            }
        }
    }

    void prepareDirectories()
    {
        try {
            checkAndCreateDirectory(instanceBasePath);

            if (fs::exists(instanceRocksDBPath)) {
                fs::remove_all(instanceBasePath);
            }
        } catch (const fs::filesystem_error& e) {
            throw std::runtime_error("Failed to prepare directories: " + std::string(e.what()));
        }
    }

    bool validateRestoreStateHandles()
    {
        return omnistream::validateRestoreStateHandles(restoreStateHandles, omniTaskBridge);
    }

    std::shared_ptr<RocksDBRestoreOperation> getRocksDBRestoreOperation(
        int keyGroupPrefixBytes,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
            registeredPQStates)
    {
        auto dbOptions = optionsContainer->getDbOptions();
        if (restoreStateHandles.empty()) {
            return std::make_shared<RocksDBNoneRestoreOperation>(
                kvStateInformation, instanceRocksDBPath, dbOptions, columnFamilyOptionsFactory);
        }
        std::shared_ptr<KeyedStateHandle> firstStateHandle = restoreStateHandles[0];
        if (restoreMode_ == RestoreSavepointMode::FLINK_COMPATIBLE) {
            if (adaptorInfo_.type == FlinkSavepointAdaptorType::None) {
                INFO_RELEASE("Error:RocksDB compatible restore is unsupported: " << adaptorInfo_.reason);
                throw std::runtime_error("RocksDB compatible restore is not supported: " + adaptorInfo_.reason);
            }
            if (adaptorInfo_.type != FlinkSavepointAdaptorType::OmniIsCompatible) {
                if (std::dynamic_pointer_cast<IncrementalKeyedStateHandle>(firstStateHandle)) {
                    INFO_RELEASE("Error:RocksDB compatible restore does not support incremental/native state handles");
                    throw std::runtime_error(
                        "RocksDB compatible full restore does not support incremental/native state handles");
                }
                auto adaptor = omnistream::OperatorSavepointAdaptorFactory::createAdaptor(adaptorInfo_.type);
                if (adaptor == nullptr) {
                    INFO_RELEASE(
                        "Error:RocksDB compatible restore adaptor factory returned null: " << adaptorInfo_.reason);
                    throw std::runtime_error("RocksDB compatible restore adaptor factory returned null");
                }
                adaptor->prepareForRestore(operatorDescription_);
                return std::make_shared<RocksDBCompatibleFullRestoreOperation<K>>(
                    keyGroupRange,
                    keyGroupPrefixBytes,
                    keySerializer,
                    kvStateInformation,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    restoreStateHandles,
                    writeBatchSize,
                    omniTaskBridge,
                    adaptorInfo_,
                    std::move(adaptor));
            }
        } else if (taskType_ == 1) {
            // sql任务的情况下
            if (adaptorInfo_.type != FlinkSavepointAdaptorType::OmniIsCompatible && !validateRestoreStateHandles()) {
                INFO_RELEASE("Error:RocksDB restore does not support current state handles");
                throw std::runtime_error("RocksDB restore does not support current state handles");
            }
        }

        if (auto incrementalHandle = std::dynamic_pointer_cast<IncrementalKeyedStateHandle>(firstStateHandle)) {
            return std::make_shared<RocksDBIncrementalRestoreOperation<K>>(
                operatorIdentifier,
                keyGroupRange,
                keyGroupPrefixBytes,
                numberOfTransferingThreads,
                kvStateInformation,
                keySerializer,
                instanceBasePath,
                instanceRocksDBPath,
                dbOptions,
                columnFamilyOptionsFactory,
                restoreStateHandles,
                writeBatchSize,
                omniTaskBridge,
                operatorId_,
                alternativeIdx_);
        } else {
            if (priorityQueueStateType_ == PriorityQueueStateType::HEAP) {
                return std::make_shared<RocksDBHeapTimersFullRestoreOperation<K>>(
                    keyGroupRange,
                    numberOfKeyGroups,
                    keySerializer,
                    kvStateInformation,
                    registeredPQStates,
                    instanceRocksDBPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    restoreStateHandles,
                    writeBatchSize,
                    omniTaskBridge);
            }

            return std::make_shared<RocksDBFullRestoreOperation<K>>(
                keyGroupRange,
                keySerializer,
                kvStateInformation,
                instanceRocksDBPath,
                dbOptions,
                columnFamilyOptionsFactory,
                restoreStateHandles,
                writeBatchSize,
                omniTaskBridge);
        }
    }

    std::shared_ptr<RocksDBSnapshotStrategyBase> initializeSavepointAndCheckpointStrategies(
        std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        int keyGroupPrefixBytes,
        rocksdb::DB* db,
        UUID backendUID,
        std::map<long, std::vector<HandleAndLocalPath>> materializedSstFiles,
        long lastCompletedCheckpointId)
    {
        std::shared_ptr<RocksDBSnapshotStrategyBase> checkpointSnapshotStrategy;

        auto stateUploader = std::make_shared<RocksDBStateUploader>(numberOfTransferingThreads);

        if (enableIncrementalCheckpointing) {
            checkpointSnapshotStrategy = std::make_shared<RocksIncrementalSnapshotStrategy>(
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                *keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig,
                instanceBasePath.string(),
                backendUID,
                materializedSstFiles,
                stateUploader,
                lastCompletedCheckpointId);
        } else {
            checkpointSnapshotStrategy = std::make_shared<RocksNativeFullSnapshotStrategy>(
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                *keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig,
                instanceBasePath.string(),
                backendUID,
                stateUploader);
        }

        return checkpointSnapshotStrategy;
    }

    std::shared_ptr<PriorityQueueSetFactory> initPriorityQueueFactory(
        int32_t keyGroupPrefixBytes,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        rocksdb::DB* db,
        std::shared_ptr<RocksDBWriteBatchWrapper> writeBatchWrapper);
};

template <typename K>
RocksdbKeyedStateBackend<K>* RocksDBKeyedStateBackendBuilder<K>::build()
{
    // todo: Memory Management?
    auto kvStateInformation = new std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>();
    auto registeredPQStates = std::make_shared<
        std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>();
    rocksdb::DB* db;
    std::shared_ptr<RocksDBRestoreOperation> restoreOperation;

    int keyGroupPrefixBytes = CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);

    auto capturedOptionsContainer = this->optionsContainer;
    columnFamilyOptionsFactory = [capturedOptionsContainer](const std::string& name) -> rocksdb::ColumnFamilyOptions {
        auto optPtr = capturedOptionsContainer->getColumnOptions();
        return *optPtr;
    };

    try {
        UUID backendUID = UUID::randomUUID();
        std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> materializedSstFiles;
        long lastCompletedCheckpointId = -1L;
        auto rocksDBResourceGuard = std::make_shared<ResourceGuard>();

        prepareDirectories();
        restoreOperation = getRocksDBRestoreOperation(keyGroupPrefixBytes, kvStateInformation, registeredPQStates);
        auto restoreResult = restoreOperation->restore();
        db = restoreResult->getDb();

        if (std::dynamic_pointer_cast<RocksDBIncrementalRestoreOperation<K>>(restoreOperation)) {
            backendUID = restoreResult->getBackendUID();
            materializedSstFiles = restoreResult->getRestoredSstFiles();
            lastCompletedCheckpointId = restoreResult->getLastCompletedCheckpointId();
        }

        auto strategy = initializeSavepointAndCheckpointStrategies(
            rocksDBResourceGuard,
            kvStateInformation,
            keyGroupPrefixBytes,
            db,
            backendUID,
            materializedSstFiles,
            lastCompletedCheckpointId);

        auto writeBatchWrapper =
            std::make_shared<RocksDBWriteBatchWrapper>(db, optionsContainer->getWriteOptions(), writeBatchSize);

        auto priorityQueueSetFactory =
            initPriorityQueueFactory(keyGroupPrefixBytes, kvStateInformation, db, writeBatchWrapper);

        auto keyContext = new InternalKeyContextImpl<K>(keyGroupRange, numberOfKeyGroups);

        return new RocksdbKeyedStateBackend<K>(
            keySerializer.get(),
            keyContext,
            db,
            columnFamilyOptionsFactory,
            std::move(strategy),
            keyGroupRange,
            kvStateInformation,
            std::move(registeredPQStates),
            std::move(rocksDBResourceGuard),
            keyGroupPrefixBytes,
            std::move(writeBatchWrapper),
            std::move(priorityQueueSetFactory),
            bridge,
            omniTaskBridge);
    } catch (const std::exception& e) {
        throw std::runtime_error("build failed." + std::string(e.what()));
    }
}

template <typename K>
std::shared_ptr<PriorityQueueSetFactory> RocksDBKeyedStateBackendBuilder<K>::initPriorityQueueFactory(
    int32_t keyGroupPrefixBytes,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    rocksdb::DB* db,
    std::shared_ptr<RocksDBWriteBatchWrapper> writeBatchWrapper)
{
    if (priorityQueueStateType_ == PriorityQueueStateType::HEAP) {
        INFO_RELEASE("The priority queue type of rocksDB backend is HEAP.");
        return std::make_shared<HeapPriorityQueueSetFactory>(keyGroupRange, numberOfKeyGroups, 128);
    }

    if (priorityQueueStateType_ == PriorityQueueStateType::ROCKSDB) {
        INFO_RELEASE("The priority queue type of rocksDB backend is ROCKSDB.");
        return std::make_shared<RocksDBPriorityQueueSetFactory>(
            keyGroupRange,
            keyGroupPrefixBytes,
            numberOfKeyGroups,
            kvStateInformation,
            db,
            optionsContainer->getReadOptions(),
            writeBatchWrapper,
            columnFamilyOptionsFactory,
            optionsContainer->getWriteBufferManagerCapacity());
    }
    THROW_LOGIC_EXCEPTION("Unsupported priority queue state type: " + static_cast<int>(priorityQueueStateType_));
}
