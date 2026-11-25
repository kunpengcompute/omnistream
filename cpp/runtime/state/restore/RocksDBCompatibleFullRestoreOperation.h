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

#pragma once

#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "runtime/state/UUID.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/restore/FullSnapshotRestoreOperation.h"
#include "runtime/state/restore/RocksDBRestoreBackendDelegate.h"
#include "runtime/state/restore/RocksDBRestoreOperation.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "typeutils/TypeSerializer.h"

// RocksDB compatible full restore operation 是 full savepoint + compatible recovery 的专用分支。它只消费已经
// prepared 的 adaptor，负责打开 DB、执行 metadata validate/restore 生命周期并返回 RocksDBRestoreResult；
// prepareForRestore 必须由 builder 在创建 adaptor 后完成，不处理 incremental/native/heap timer 路径，
// 也不调用普通 full restore 的 raw direct put 逻辑。
template <typename K>
class RocksDBCompatibleFullRestoreOperation : public RocksDBRestoreOperation {
public:
    using RegisteredPQStatesMap =
        std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>;

    RocksDBCompatibleFullRestoreOperation(
        KeyGroupRange* keyGroupRange,
        int keyGroupPrefixBytes,
        std::shared_ptr<TypeSerializer> keySerializer,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        std::filesystem::path& instanceRocksDBPath,
        std::shared_ptr<rocksdb::DBOptions> dbOptions,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
        std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles,
        long writeBatchSize,
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge,
        FlinkSavepointAdaptorInfo adaptorInfo,
        std::unique_ptr<omnistream::OperatorSavepointAdaptor> preparedAdaptor,
        std::shared_ptr<RegisteredPQStatesMap> registeredPQStates = nullptr)
        : keyGroupRange_(keyGroupRange),
          keyGroupPrefixBytes_(keyGroupPrefixBytes),
          restoreStateHandles_(std::move(restoreStateHandles)),
          savepointRestoreOperation_(
              std::make_unique<FullSnapshotRestoreOperation<K>>(
                  keyGroupRange, restoreStateHandles_, std::move(keySerializer), omniTaskBridge)),
          rocksDbHandle_(
              std::make_unique<RocksDbHandle>(
                  kvStateInformation,
                  instanceRocksDBPath,
                  std::move(dbOptions),
                  std::move(columnFamilyOptionsFactory))),
          writeBatchSize_(writeBatchSize),
          kvStateInformation_(kvStateInformation),
          omniTaskBridge_(std::move(omniTaskBridge)),
          adaptorInfo_(std::move(adaptorInfo)),
          preparedAdaptor_(std::move(preparedAdaptor)),
          registeredPQStates_(std::move(registeredPQStates))
    {
    }

    std::shared_ptr<RocksDBRestoreResult> restore() override
    {
        if (!restoreStateHandles_.empty()) {
            if (omniTaskBridge_ == nullptr) {
                INFO_RELEASE(
                    "Error:RocksDBCompatibleFullRestoreOperation::restore missing OmniTaskBridge, adaptorType="
                    << static_cast<int>(adaptorInfo_.type) << ", reason=" << adaptorInfo_.reason
                    << ", stateHandleCount=" << restoreStateHandles_.size());
                throw std::invalid_argument(
                    "RocksDB compatible restore requires OmniTaskBridge when state handles exist");
            }
            if (preparedAdaptor_ == nullptr) {
                INFO_RELEASE(
                    "Error:RocksDBCompatibleFullRestoreOperation::restore missing prepared adaptor, adaptorType="
                    << static_cast<int>(adaptorInfo_.type) << ", reason=" << adaptorInfo_.reason
                    << ", stateHandleCount=" << restoreStateHandles_.size());
                throw std::runtime_error(
                    "RocksDB compatible restore requires prepared adaptor, type=" +
                    std::to_string(static_cast<int>(adaptorInfo_.type)) +
                    ", builder/factory did not provide a prepared adaptor");
            }

            validateRestoreStateHandles();
        }

        rocksDbHandle_->openDB();
        CompatibleRestoreAbortGuard abortGuard(rocksDbHandle_.get(), kvStateInformation_);

        if (!restoreStateHandles_.empty()) {
            auto restoreIterator = savepointRestoreOperation_->restore();
            RocksDBRestoreBackendDelegate<K> backendDelegate(
                rocksDbHandle_.get(),
                keyGroupRange_->getStartKeyGroup(),
                keyGroupPrefixBytes_,
                writeBatchSize_,
                registeredPQStates_);
            preparedAdaptor_->restore(*restoreIterator, backendDelegate);
        }

        auto result = createRestoreResult();
        abortGuard.release();
        return result;
    }

private:
    class CompatibleRestoreAbortGuard {
    public:
        CompatibleRestoreAbortGuard(
            RocksDbHandle* rocksDbHandle,
            std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation)
            : rocksDbHandle_(rocksDbHandle),
              kvStateInformation_(kvStateInformation)
        {
        }

        ~CompatibleRestoreAbortGuard() noexcept
        {
            if (!active_) {
                return;
            }
            if (kvStateInformation_ != nullptr) {
                kvStateInformation_->clear();
            }
            if (rocksDbHandle_ != nullptr) {
                rocksDbHandle_->closeOpenDbNoThrow();
            }
        }

        CompatibleRestoreAbortGuard(const CompatibleRestoreAbortGuard&) = delete;
        CompatibleRestoreAbortGuard& operator=(const CompatibleRestoreAbortGuard&) = delete;

        void release() noexcept
        {
            active_ = false;
        }

    private:
        RocksDbHandle* rocksDbHandle_;
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation_;
        bool active_ = true;
    };

    void validateRestoreStateHandles()
    {
        // State metadata and kvStateId are local to each KeyGroupsStateHandle.
        // Validate one handle at a time. Keep this probe independent from the iterator consumed by restore().
        SavepointRestoreResultIterator metaProbe(restoreStateHandles_, omniTaskBridge_);
        while (metaProbe.hasNext()) {
            auto restoreResult = metaProbe.next();
            const auto& handleMetaInfos = restoreResult->getStateMetaInfoSnapshots();
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos;
            metaInfos.reserve(handleMetaInfos.size());
            for (const auto& metaInfo : handleMetaInfos) {
                metaInfos.push_back(std::make_shared<StateMetaInfoSnapshot>(metaInfo));
            }
            preparedAdaptor_->validateForRestore(metaInfos);
        }
    }

    std::shared_ptr<RocksDBRestoreResult> createRestoreResult()
    {
        UUID emptyUid{};
        std::map<long, std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>> emptyRestoredSstFiles{};
        return std::make_shared<RocksDBRestoreResult>(
            rocksDbHandle_->getDb(),
            rocksDbHandle_->getDefaultColumnFamilyHandle(),
            -1L,
            emptyUid,
            emptyRestoredSstFiles);
    }

    KeyGroupRange* keyGroupRange_;
    int keyGroupPrefixBytes_;
    long writeBatchSize_;

    std::vector<std::shared_ptr<KeyedStateHandle>> restoreStateHandles_;
    std::unique_ptr<FullSnapshotRestoreOperation<K>> savepointRestoreOperation_;
    std::unique_ptr<RocksDbHandle> rocksDbHandle_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation_;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;
    FlinkSavepointAdaptorInfo adaptorInfo_;
    std::unique_ptr<omnistream::OperatorSavepointAdaptor> preparedAdaptor_;
    std::shared_ptr<RegisteredPQStatesMap> registeredPQStates_;
};
