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

#ifndef OMNISTREAM_ROCKSDBINCREMENTALRESTOREOPERATION_H
#define OMNISTREAM_ROCKSDBINCREMENTALRESTOREOPERATION_H

#include "core/fs/CloseableRegistry.h"
#include "core/memory/DataInputDeserializer.h"

#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/IncrementalKeyedStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/UUID.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/IncrementalLocalKeyedStateHandle.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/rocksdb/RocksDbOperationUtils.h"
#include "runtime/state/RocksDBIncrementalCheckpointUtils.h"
#include "runtime/state/restore/RocksDBRestoreOperation.h"
#include "runtime/state/restore/RocksDBRestoreResult.h"
#include "runtime/state/rocksdb/RocksDBStateDownloader.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/rocksdb/RocksDbStringAppendOperator.h"

template <typename K>
class RocksDBIncrementalRestoreOperation : public RocksDBRestoreOperation {
public:
    RocksDBIncrementalRestoreOperation(
        std::string operatorIdentifier,
        KeyGroupRange* keyGroupRange,
        int keyGroupPrefixBytes,
        int numberOfTransferringThreads,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::filesystem::path& instanceBasePath,
        std::filesystem::path& instanceRocksDBPath,
        std::shared_ptr<rocksdb::DBOptions> dbOptions,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
        std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles,
        long writeBatchSize,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge,
        std::shared_ptr<OperatorID> operatorId,
        int alternativeIdx)
        : operatorIdentifier_(std::move(operatorIdentifier)),
          restoredSstFiles_(),
          lastCompletedCheckpointId_(-1L),
          backendUID_(UUID::randomUUID()),
          writeBatchSize_(writeBatchSize),
          restoreStateHandles_(std::move(restoreStateHandles)),
          keyGroupRange_(keyGroupRange),
          keySerializer_(std::move(keySerializer)),
          instanceBasePath_(instanceBasePath),
          numberOfTransferringThreads_(numberOfTransferringThreads),
          keyGroupPrefixBytes_(keyGroupPrefixBytes),
          overlapFractionThreshold_(0),
          omniTaskBridge_(omniTaskBridge),
          operatorId_(operatorId),
          alternativeIdx_(alternativeIdx)
    {
        this->rocksHandle_ = std::make_unique<RocksDbHandle>(
            kvStateInformation, instanceRocksDBPath, dbOptions, columnFamilyOptionsFactory);
    }

    ~RocksDBIncrementalRestoreOperation() override = default;

    std::shared_ptr<RocksDBRestoreResult> restore() override
    {
        if (restoreStateHandles_.empty()) {
            return nullptr;
        }

        std::shared_ptr<KeyedStateHandle> theFirstStateHandle = restoreStateHandles_.front();

        bool isRescaling = false;

        if (restoreStateHandles_.size() > 1) {
            isRescaling = true;
        } else {
            const auto& firstKeyGroupRange = theFirstStateHandle->GetKeyGroupRange();
            if (!(firstKeyGroupRange == *keyGroupRange_)) {
                isRescaling = true;
            }
        }

        try {
            if (isRescaling) {
                restoreWithRescaling(restoreStateHandles_);
            } else {
                restoreWithoutRescaling(theFirstStateHandle);
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Restore failed: " + std::string(e.what()));
        } catch (...) {
            throw std::runtime_error("Restore failed with unknown exception");
        }

        return std::make_shared<RocksDBRestoreResult>(
            rocksHandle_->getDb(),
            rocksHandle_->getDefaultColumnFamilyHandle(),
            lastCompletedCheckpointId_,
            backendUID_,
            restoredSstFiles_);
    }

private:
    std::string operatorIdentifier_;
    std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> restoredSstFiles_;
    long lastCompletedCheckpointId_;
    UUID backendUID_;
    long writeBatchSize_;
    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles_;
    std::unique_ptr<RocksDbHandle> rocksHandle_;
    KeyGroupRange* keyGroupRange_;
    std::shared_ptr<TypeSerializer> keySerializer_;
    std::filesystem::path instanceBasePath_;
    int numberOfTransferringThreads_;
    int keyGroupPrefixBytes_;
    double overlapFractionThreshold_;
    JNIEnv* env_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    std::shared_ptr<OperatorID> operatorId_;
    int alternativeIdx_;

    void restoreWithoutRescaling(std::shared_ptr<KeyedStateHandle> keyedStateHandle)
    {
        auto remoteHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(keyedStateHandle);
        if (remoteHandle != nullptr) {
            restorePreviousIncrementalFilesStatus(remoteHandle);
            restoreFromRemoteState(remoteHandle);
        } else {
            auto localHandle = std::dynamic_pointer_cast<IncrementalLocalKeyedStateHandle>(keyedStateHandle);
            if (localHandle != nullptr) {
                restorePreviousIncrementalFilesStatus(localHandle);
                restoreFromLocalState(localHandle);
            } else {
                throw std::runtime_error("restoreWithoutRescaling error");
            }
        }
    }

    void restorePreviousIncrementalFilesStatus(std::shared_ptr<IncrementalKeyedStateHandle> localKeyedStateHandle)
    {
        backendUID_ = localKeyedStateHandle->GetBackendIdentifier();
        restoredSstFiles_.emplace(
            localKeyedStateHandle->GetCheckpointId(), localKeyedStateHandle->GetSharedStateHandles());
        lastCompletedCheckpointId_ = localKeyedStateHandle->GetCheckpointId();
    }

    void restoreFromRemoteState(std::shared_ptr<IncrementalRemoteKeyedStateHandle> stateHandle)
    {
        std::filesystem::path tmpRestoreInstancePath =
            std::filesystem::absolute(instanceBasePath_) / UUID::randomUUID().ToString();
        auto restoreStateHandle =
            transferRemoteStateToLocalDirectory(omniTaskBridge_, tmpRestoreInstancePath, stateHandle);
        restoreFromLocalState(restoreStateHandle);
    }

    void restoreFromLocalState(std::shared_ptr<IncrementalLocalKeyedStateHandle> localKeyedStateHandle)
    {
        auto serializerStr = TaskStateSnapshotSerializer::parseIncrementalKeyedStateHandle(localKeyedStateHandle);
        std::vector<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
            omniTaskBridge_->readMetaData(to_string(serializerStr));
        // TTODO: Here use the real stateMetaInfoSnapshots, instead of the fake one.

        Path path = localKeyedStateHandle->GetDirectoryStateHandle()->getDirectory();
        std::filesystem::path restoreSourcePath = path.toString();
        rocksHandle_->openDB(
            createColumnFamilyDescriptors(stateMetaInfoSnapshots, false), stateMetaInfoSnapshots, restoreSourcePath);
    }

    void cleanUpPathQuietly(const std::filesystem::path& path)
    {
        try {
            if (!std::filesystem::exists(path)) {
                return;
            }
            std::filesystem::remove_all(path);
        } catch (const std::exception& ex) {
            // do nothing
        }
    }

    std::shared_ptr<IncrementalLocalKeyedStateHandle> transferRemoteStateToLocalDirectory(
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge,
        fs::path temporaryRestoreInstancePath,
        std::shared_ptr<IncrementalRemoteKeyedStateHandle> restoreStateHandle)
    {
        std::unique_ptr<RocksDBStateDownloader> rocksDBStateDownloader =
            std::make_unique<RocksDBStateDownloader>(numberOfTransferringThreads_);

        rocksDBStateDownloader->transferAllStateDataToDirectory(
            *restoreStateHandle, temporaryRestoreInstancePath, omniTaskBridge);
        Path path(temporaryRestoreInstancePath.string());
        return std::make_shared<IncrementalLocalKeyedStateHandle>(
            restoreStateHandle->GetBackendIdentifier(),
            restoreStateHandle->GetCheckpointId(),
            new DirectoryStateHandle(path, 0),
            restoreStateHandle->GetKeyGroupRange(),
            restoreStateHandle->GetMetaDataStateHandle(),
            restoreStateHandle->GetSharedState());
    }

    /**
     * Recovery from multi incremental states with rescaling. For rescaling, this method creates a
     * temporary RocksDB instance for a key-groups shard. All contents from the temporary instance
     * are copied into the real restore instance and then the temporary instance is discarded.
     */
    void restoreWithRescaling(std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles)
    {
        // 1.handle choose
        std::shared_ptr<KeyedStateHandle> initialPtr = selectInitialStateHandle(restoreStateHandles);

        if (initialPtr) {
            initDBWithRescaling(initialPtr);
        } else {
            rocksHandle_->openDB();
        }
        std::vector<uint8_t> startKeyGroupPrefixBytes(keyGroupPrefixBytes_);
        CompositeKeySerializationUtils::serializeKeyGroup(keyGroupRange_->getStartKeyGroup(), startKeyGroupPrefixBytes);

        std::vector<uint8_t> stopKeyGroupPrefixBytes(keyGroupPrefixBytes_);
        CompositeKeySerializationUtils::serializeKeyGroup(
            keyGroupRange_->getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

        for (const auto& rawStateHandle : restoreStateHandles) {
            auto remoteHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(rawStateHandle);
            if (!remoteHandle) {
                throw unexpectedStateHandleException(
                    typeid(IncrementalRemoteKeyedStateHandle), typeid(*rawStateHandle));
            }
            fs::path temporaryRestoreInstancePath =
                std::filesystem::absolute(instanceBasePath_) / UUID::randomUUID().ToString();
            try {
                auto tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(remoteHandle, temporaryRestoreInstancePath);

                RocksDBWriteBatchWrapper writeBatchWrapper(rocksHandle_->getDb(), writeBatchSize_);
                std::vector<rocksdb::ColumnFamilyDescriptor> tmpColumnFamilyDescriptors =
                    tmpRestoreDBInfo->columnFamilyDescriptors_;
                std::vector<rocksdb::ColumnFamilyHandle*> tmpColumnFamilyHandles =
                    tmpRestoreDBInfo->columnFamilyHandles_;
                for (size_t i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
                    rocksdb::ColumnFamilyHandle* tmpColumnFamilyHandle = tmpColumnFamilyHandles[i];

                    rocksdb::ColumnFamilyHandle* targetColumnFamilyHandle =
                        rocksHandle_
                            ->getOrRegisterStateColumnFamilyHandle(
                                nullptr, tmpRestoreDBInfo->stateMetaInfoSnapshots_[i])
                            ->columnFamilyHandle_;

                    auto iterator = RocksDbOperationUtils::getRocksIterator(
                        tmpRestoreDBInfo->db_, tmpColumnFamilyHandle, *tmpRestoreDBInfo->readOptions_);
                    migrateDataBetweenColumnFamilies(
                        iterator,
                        targetColumnFamilyHandle,
                        startKeyGroupPrefixBytes,
                        stopKeyGroupPrefixBytes,
                        writeBatchWrapper);
                }
            } catch (...) {
                cleanUpPathQuietly(temporaryRestoreInstancePath);
                throw;
            }
            cleanUpPathQuietly(temporaryRestoreInstancePath);
        }
    }

    std::shared_ptr<KeyedStateHandle> selectInitialStateHandle(
        std::vector<std::shared_ptr<KeyedStateHandle>>& restoreStateHandles)
    {
        auto initialHandle = RocksDBIncrementalCheckpointUtils::chooseTheBestStateHandleForInitial(
            restoreStateHandles, *keyGroupRange_, overlapFractionThreshold_);
        std::shared_ptr<KeyedStateHandle> initialPtr;
        if (initialHandle != nullptr) {
            auto it = std::find(restoreStateHandles.begin(), restoreStateHandles.end(), initialHandle);
            if (it != restoreStateHandles.end()) {
                initialPtr = *it;
                restoreStateHandles.erase(it);
            }
        }
        return initialPtr;
    }

    void migrateDataBetweenColumnFamilies(
        std::unique_ptr<RocksIteratorWrapper>& iterator,
        rocksdb::ColumnFamilyHandle* targetCF,
        const std::vector<uint8_t>& startKey,
        const std::vector<uint8_t>& stopKey,
        RocksDBWriteBatchWrapper& writeBatch)
    {
        iterator->seek(startKey);
        while (iterator->isValid()) {
            std::string key = iterator->key();
            std::string stopKeyGroupPrefixBytesSliceStr(stopKey.begin(), stopKey.end());
            if (RocksDBIncrementalCheckpointUtils::beforeThePrefixBytes(key, stopKeyGroupPrefixBytesSliceStr)) {
                writeBatch.Put(targetCF, key, iterator->value());
            } else {
                break;
            }
            iterator->next();
        }
    }

    void initDBWithRescaling(std::shared_ptr<KeyedStateHandle> initialPtr)
    {
        auto remoteHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(initialPtr);
        if (!remoteHandle) {
            throw std::invalid_argument("Initial handle must be IncrementalRemoteKeyedStateHandle");
        }
        restoreFromRemoteState(remoteHandle);

        try {
            RocksDBIncrementalCheckpointUtils::clipDBWithKeyGroupRange(
                rocksHandle_->getDb(),
                rocksHandle_->getColumnFamilyHandles(),
                *keyGroupRange_,
                initialPtr->GetKeyGroupRange(),
                keyGroupPrefixBytes_);
        } catch (...) {
            // do nothing
        }
    }

    class RestoredDBInstance {
    public:
        RestoredDBInstance(
            rocksdb::DB* db,
            std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandles,
            std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors,
            std::vector<StateMetaInfoSnapshot> stateMetaInfoSnapshots)
            : db_(db),
              columnFamilyDescriptors_(std::move(columnFamilyDescriptors)),
              stateMetaInfoSnapshots_(std::move(stateMetaInfoSnapshots)),
              columnFamilyHandles_(columnFamilyHandles),
              readOptions_(std::make_shared<rocksdb::ReadOptions>())
        {
            if (columnFamilyHandles_.empty()) {
                throw std::invalid_argument("columnFamilyHandles cannot be empty");
            }

            defaultColumnFamilyHandle_ = columnFamilyHandles.front();
            columnFamilyHandles.erase(columnFamilyHandles.begin());
            columnFamilyHandles_ = columnFamilyHandles;
        }

        ~RestoredDBInstance()
        {
        }
        rocksdb::DB* db_;
        rocksdb::ColumnFamilyHandle* defaultColumnFamilyHandle_;
        std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandles_;
        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors_;
        std::vector<StateMetaInfoSnapshot> stateMetaInfoSnapshots_;
        std::shared_ptr<rocksdb::ReadOptions> readOptions_;
    };

    // This function is for restoreWithRescaling. It will not be used!
    std::shared_ptr<RestoredDBInstance> restoreDBInstanceFromStateHandle(
        std::shared_ptr<IncrementalRemoteKeyedStateHandle> restoreStateHandle, fs::path temporaryRestoreInstancePath)
    {
        RocksDBStateDownloader rocksDbStateDownloader(numberOfTransferringThreads_);
        rocksDbStateDownloader.transferAllStateDataToDirectory(
            *restoreStateHandle, temporaryRestoreInstancePath, omniTaskBridge_);
        // read meta data
        auto serializerStr = TaskStateSnapshotSerializer::parseIncrementalRemoteKeyedStateHandle(restoreStateHandle);
        auto stateMetaInfoSnapshots = omniTaskBridge_->readMetaData(to_string(serializerStr));

        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors =
            createColumnFamilyDescriptors(stateMetaInfoSnapshots, false);
        std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandles;
        columnFamilyHandles.reserve(stateMetaInfoSnapshots.size() + 1);
        rocksdb::ColumnFamilyOptions columnFamilyOptions =
            RocksDbOperationUtils::createColumnFamilyOptions(rocksHandle_->getColumnFamilyOptionsFactory(), "default");

        std::shared_ptr<rocksdb::DBOptions> dbOptions = rocksHandle_->getDbOptions();
        rocksdb::DB* restoreDb = RocksDbOperationUtils::openDB(
            temporaryRestoreInstancePath,
            columnFamilyDescriptors,
            columnFamilyHandles,
            columnFamilyOptions,
            *dbOptions);
        return std::make_shared<RestoredDBInstance>(
            restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
    };

    std::vector<rocksdb::ColumnFamilyDescriptor> createColumnFamilyDescriptors(
        std::vector<StateMetaInfoSnapshot> stateMetaInfoSnapshots, bool registerTtlCompactFilter)
    {
        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyDescriptors;
        columnFamilyDescriptors.reserve(stateMetaInfoSnapshots.size());

        for (const auto& snapshot : stateMetaInfoSnapshots) {
            auto metaInfoBase = RegisteredStateMetaInfoBase::fromMetaInfoSnapshot(snapshot);
            if (!metaInfoBase) {
                continue;
            }
            rocksdb::ColumnFamilyDescriptor columnFamilyDescriptor =
                RocksDbOperationUtils::createColumnFamilyDescriptor(
                    std::move(metaInfoBase), rocksHandle_->getColumnFamilyOptionsFactory());

            // LIST state uses Merge operations; the column family must have merge_operator
            // set to match the original DB when opening it for restore.
            auto stateTypeString = snapshot.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
            if (stateTypeString == "LIST") {
                columnFamilyDescriptor.options.merge_operator.reset(new RocksDbStringAppendOperator(','));
            }

            columnFamilyDescriptors.push_back(std::move(columnFamilyDescriptor));
        }
        return columnFamilyDescriptors;
    }

    std::runtime_error unexpectedStateHandleException(const std::type_info& expected, const std::type_info& actual)
    {
        return std::runtime_error(
            "Unexpected state handle type: expected " + std::string(expected.name()) + ", but got " +
            std::string(actual.name()));
    }
};
#endif // OMNISTREAM_ROCKSDBINCREMENTALRESTOREOPERATION_H
