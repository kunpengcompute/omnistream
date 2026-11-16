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

#include <emhash7.hpp>
#include <map>
#include <filesystem>
#include <future>
#include <functional>
#include <memory>
#include <vector>
#include "AbstractKeyedStateBackend.h"
#include "InternalKeyContext.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/State.h"
#include "runtime/state/heap/HeapListState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "runtime/state/rocksdb/RocksdbStateTable.h"
#include "runtime/state/rocksdb/RocksdbMapState.h"
#include "runtime/state/rocksdb/RocksdbListState.h"
#include "runtime/state/rocksdb/RocksdbMapStateTable.h"
#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/SavepointResources.h"
#include "runtime/state/SnapshotExecutionType.h"
#include "runtime/state/RocksDBFullSnapshotResources.h"
#include "RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/RowData.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "RocksDBConfigurableOptions.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "DefaultConfigurableOptionsFactory.h"
#include "HeapPriorityQueuesManager.h"
#include "PriorityQueueSetFactory.h"
#include "snapshot/RocksDBSnapshotStrategyBase.h"
#include "RegisteredStateMetaInfoBase.h"
#include "heap/HeapPriorityQueueSetFactory.h"
#include "heap/HeapPriorityQueueSnapshotRestoreWrapper.h"
#include "rocksdb/RocksDBPriorityQueueSetFactory.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/bridge/TaskStateManagerBridge.h"
#include <set>
namespace fs = std::filesystem;
using namespace omniruntime::type;
/*
 State's value can be
 (1) basic non-map value (2) pointer to non-map value, like RowData*
 (3) pointer to map, like emhash<RowData*, int>* for Join
 (4) very rarely and don't use it, directly a map

 currently in case (1) we only accepts type that have std::numeric_limits<T>::max(), due to return of nullptr not
 acceptable in V get()
*/

// Very simplified class, reduces a lot of unused variables and functions
template <typename K>
class RocksdbKeyedStateBackend : public AbstractKeyedStateBackend<K> {
public:
    omnistream::StateType getStateType() const noexcept override
    {
        return omnistream::StateType::ROCKSDB;
    }

    // Originally used to create an internal state, not necessary here
    uintptr_t createOrUpdateInternalState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc) override;

    ~RocksdbKeyedStateBackend() override
    {
        RocksdbKeyedStateBackend<K>::dispose();
    };

    RocksdbKeyedStateBackend(
        TypeSerializer* keySerializer,
        InternalKeyContext<K>* context,
        rocksdb::DB* rocksdb,
        std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
        std::shared_ptr<RocksDBSnapshotStrategyBase> rocksdbStrategy,
        KeyGroupRange* keyGroupRange,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
            registeredPQStates,
        std::shared_ptr<ResourceGuard> rocksDBResourceGuard,
        int keyGroupPrefixBytes,
        std::shared_ptr<RocksDBWriteBatchWrapper> writeBatchWrapper,
        std::shared_ptr<PriorityQueueSetFactory> priorityQueueSetFactory,
        std::shared_ptr<TaskStateManagerBridge> bridge,
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge)
        : AbstractKeyedStateBackend<K>(keySerializer, context),
          db(rocksdb),
          columnFamilyOptionsFactory_(std::move(columnFamilyOptionsFactory)),
          strategy(std::move(rocksdbStrategy)),
          kvStateInformation_(kvStateInformation),
          rocksDBResourceGuard_(std::move(rocksDBResourceGuard)),
          keyGroupRange_(keyGroupRange),
          keySerializer_(keySerializer),
          keyGroupPrefixBytes_(keyGroupPrefixBytes),
          writeBatchWrapper_(std::move(writeBatchWrapper)),
          priorityQueueSetFactory_(std::move(priorityQueueSetFactory)),
          bridge_(std::move(bridge)),
          omniTaskBridge_(std::move(omniTaskBridge))
    {
        startGroup_ = keyGroupRange->getStartKeyGroup();
        endGroup_ = keyGroupRange->getEndKeyGroup();
        maxParallelism_ = keyGroupRange->getNumberOfKeyGroups();
        if (auto factory = std::dynamic_pointer_cast<HeapPriorityQueueSetFactory>(priorityQueueSetFactory_)) {
            heapPriorityQueuesManager_ = std::make_shared<HeapPriorityQueuesManager>(
                std::move(registeredPQStates), factory, context->getKeyGroupRange(), context->getNumberOfKeyGroups());
        }
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* options)
    {
        flushFalconCacheBeforeCheckpoint(); // [FALCON] flush falcon cache before snapshot

        writeBatchWrapper_->Flush();

        auto snapshotstrategyrunner = std::make_unique<SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources>>(
            strategy->getDescription(), strategy, SnapshotExecutionType::ASYNCHRONOUS);
        return snapshotstrategyrunner->snapshot(
            checkpointId, timestamp, streamFactory, options, omniTaskBridge_, keySerializer_->toJson());
    }

    void notifyCheckpointComplete(long completedCheckpointId)
    {
        INFO_RELEASE("savepoint: rocksdbKeyedStateBackend notifyCheckpointComplete");
        if (strategy) {
            strategy->notifyCheckpointComplete(completedCheckpointId);
        }
    }
    bool requiresLegacySynchronousTimerSnapshots(SnapshotType* checkpointType) override
    {
        bool requiresLegacy =
            heapPriorityQueuesManager_ != nullptr && checkpointType != nullptr && !checkpointType->IsSavepoint();
        return requiresLegacy;
    }

    std::shared_ptr<SavepointResources> savepoint() override
    {
        flushFalconCacheBeforeCheckpoint(); // [FALCON] flush falcon cache before savepoint

        writeBatchWrapper_->Flush();
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
            registeredPQStates;
        if (heapPriorityQueuesManager_ != nullptr) {
            registeredPQStates = heapPriorityQueuesManager_->getRegisteredPQStates();
        }

        auto snapshotResources = RocksDBFullSnapshotResources::create(
            *kvStateInformation_,
            registeredPQStates,
            db,
            rocksDBResourceGuard_,
            keyGroupRange_,
            keySerializer_,
            keyGroupPrefixBytes_);
        return std::make_shared<SavepointResources>(snapshotResources, SnapshotExecutionType::ASYNCHRONOUS);
    }

    void dispose() override
    {
        if (disposed_) {
            return;
        }
        INFO_RELEASE("Start to dispose RocksDB Keyed State Backend.");
        AbstractKeyedStateBackend<K>::dispose();
        rocksDBResourceGuard_->close();

        if (db != nullptr) {
            try {
                writeBatchWrapper_->close();
            } catch (...) {
                // do nothing
            }

            for (const auto& pair : registeredKvStates) {
                StateDescriptor* desc = std::get<1>(pair.second);
                uintptr_t stateTablePtr = std::get<0>(pair.second);
                BackendDataType nsId = std::get<2>(pair.second);

                if (desc->getType() == StateDescriptor::Type::MAP) {
                    auto keyId = desc->getKeyDataId();
                    auto valueId = desc->getValueDataId();
                    // MAP state 目前只支持 VoidNamespace
                    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, int32_t, int32_t>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, int64_t, int64_t>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, std::string, int32_t>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::VARCHAR_BK) {
                        // JSON_OBJECTAGG: MapView<VoidNamespace, std::string, std::string>.
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, std::string, std::string>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::VARCHAR_BK) {
                        // JSON_ARRAYAGG: MapView<VoidNamespace, long, std::string>.
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, int64_t, std::string>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, RowData*, int32_t>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::INT_BK) {
                        delete reinterpret_cast<
                            RocksdbMapStateTable<K, VoidNamespace, std::shared_ptr<RowData>, int32_t>*>(stateTablePtr);
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, RowData*, RowData*>*>(
                            stateTablePtr);
                    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, TimeWindow, TimeWindow>*>(
                            stateTablePtr);
                    } else if (
                        keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::TUPLE_INT32_INT32) {
                        delete reinterpret_cast<RocksdbMapStateTable<
                            K,
                            VoidNamespace,
                            std::shared_ptr<RowData>,
                            std::tuple<int32_t, int32_t>>*>(stateTablePtr);
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
                        delete reinterpret_cast<
                            RocksdbMapStateTable<K, VoidNamespace, RowData*, std::vector<RowData*>*>*>(stateTablePtr);
                    } else if (
                        (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::BIGINT_BK) ||
                        (keyId == BackendDataType::OBJECT_BK && valueId == BackendDataType::POJO_BK) ||
                        (keyId == BackendDataType::OBJECT_BK && valueId == BackendDataType::OBJECT_BK)) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, Object*, Object*>*>(
                            stateTablePtr);
                    } else if (
                        keyId == BackendDataType::EXTERNAL_BIGINT_BK &&
                        valueId == BackendDataType::EXTERNAL_BIGINT_BK) {
                        delete reinterpret_cast<RocksdbMapStateTable<K, VoidNamespace, int64_t, int64_t>*>(
                            stateTablePtr);
                    } else {
                        GErrorLog(
                            "Unhandled MAP state type in dispose: keyId=" + std::to_string((int)keyId) +
                            " valueId=" + std::to_string((int)valueId));
                    }

                } else if (desc->getType() == StateDescriptor::Type::VALUE) {
                    auto dataId = desc->getBackendId();
                    if (dataId == BackendDataType::ROW_BK) {
                        // namespace 可能是 int64_t, TimeWindow, 或 VoidNamespace
                        if (nsId == BackendDataType::BIGINT_BK) {
                            delete reinterpret_cast<RocksdbStateTable<K, int64_t, RowData*>*>(stateTablePtr);
                        } else if (nsId == BackendDataType::TIME_WINDOW_BK) {
                            delete reinterpret_cast<RocksdbStateTable<K, TimeWindow, RowData*>*>(stateTablePtr);
                        } else {
                            delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, RowData*>*>(stateTablePtr);
                        }
                    } else if (dataId == BackendDataType::INT_BK) {
                        delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, int32_t>*>(stateTablePtr);
                    } else if (dataId == BackendDataType::BIGINT_BK) {
                        delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                    } else if (dataId == BackendDataType::OBJECT_BK || dataId == BackendDataType::POJO_BK) {
                        delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, Object*>*>(stateTablePtr);
                    } else if (dataId == BackendDataType::ROW_LIST_BK) {
                        delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, std::vector<RowData*>*>*>(
                            stateTablePtr);
                    } else if (dataId == BackendDataType::EXTERNAL_BIGINT_BK) {
                        delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                    } else {
                        GErrorLog("Unhandled VALUE state type in dispose: dataId=" + std::to_string((int)dataId));
                    }

                } else if (desc->getType() == StateDescriptor::Type::LIST) {
                    auto dataId = desc->getBackendId();
                    if (dataId == BackendDataType::BIGINT_BK) {
                        if (nsId == BackendDataType::BIGINT_BK) {
                            // createOrUpdateInternalListState<int64_t, int64_t> 注册的是 RocksdbStateTable<K, int64_t,
                            // int64_t>
                            delete reinterpret_cast<RocksdbStateTable<K, int64_t, int64_t>*>(stateTablePtr);
                        } else {
                            // createOrUpdateInternalListState<VoidNamespace, int64_t>
                            delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                        }
                    } else if (dataId == BackendDataType::ROW_BK) {
                        if (nsId == BackendDataType::BIGINT_BK) {
                            delete reinterpret_cast<RocksdbStateTable<K, int64_t, RowData*>*>(stateTablePtr);
                        } else {
                            delete reinterpret_cast<RocksdbStateTable<K, VoidNamespace, RowData*>*>(stateTablePtr);
                        }
                    } else {
                        GErrorLog("Unhandled LIST state type in dispose: dataId=" + std::to_string((int)dataId));
                    }
                }
                delete desc;
            }
            registeredKvStates.clear();

            for (const auto& pair : createdKvState) {
                auto* state = reinterpret_cast<State*>(pair.second);
                delete state;
                state = nullptr;
            }
            createdKvState.clear();

            db->Close();

            INFO_RELEASE("Cleaning up RocksDB working directory " << kDBPath);
            std::error_code ec;
            std::filesystem::remove_all(kDBPath, ec);
            if (ec) {
                GErrorLog(
                    "Could not delete RocksDB working directory " + kDBPath +
                    ", error message = " + ec.message().c_str());
            }
            INFO_RELEASE("RocksDB Keyed State Backend has been disposed.");
            delete db;
            db = nullptr;
        }
        disposed_ = true;
    }

    template <typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
        std::string stateName, TypeSerializer* byteOrderedElementSerializer)
    {
        return this->create<T, Comparator>(stateName, byteOrderedElementSerializer, false);
    }

    template <typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
        std::string stateName, TypeSerializer* byteOrderedElementSerializer, bool allowFutureMetadataUpdates)
    {
        if (heapPriorityQueuesManager_ != nullptr) {
            INFO_RELEASE(
                "TIMER_PQ_CREATE backend=ROCKSDB_KEYED" << ", pqStorage=HEAP"
                                                        << ", backendPtr=" << reinterpret_cast<uintptr_t>(this)
                                                        << ", stateName=" << stateName);
            return heapPriorityQueuesManager_->createOrUpdate<K, T, Comparator>(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        }

        if (auto factory = dynamic_pointer_cast<RocksDBPriorityQueueSetFactory>(priorityQueueSetFactory_)) {
            INFO_RELEASE(
                "TIMER_PQ_CREATE backend=ROCKSDB_KEYED" << ", pqStorage=ROCKSDB"
                                                        << ", backendPtr=" << reinterpret_cast<uintptr_t>(this)
                                                        << ", stateName=" << stateName);
            return factory->create<K, T, Comparator>(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        }
        THROW_LOGIC_EXCEPTION("RocksdbKeyedStateBackend failed to create priority queue.");
    }

private:
    int startGroup_;
    int endGroup_;
    int maxParallelism_;
    ROCKSDB_NAMESPACE::DB* db = nullptr;
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory_;
    bool disposed_ = false; // mark whether the backend is already disposed and prevent duplicate disposing
    std::shared_ptr<RocksDBWriteBatchWrapper> writeBatchWrapper_;
    std::string kDBPath;
    std::shared_ptr<RocksDBSnapshotStrategyBase> strategy;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation_;
    std::shared_ptr<ResourceGuard> rocksDBResourceGuard_;
    KeyGroupRange* keyGroupRange_ = nullptr;
    TypeSerializer* keySerializer_ = nullptr;
    int keyGroupPrefixBytes_;
    std::shared_ptr<HeapPriorityQueuesManager> heapPriorityQueuesManager_;
    std::shared_ptr<PriorityQueueSetFactory> priorityQueueSetFactory_;
    std::shared_ptr<TaskStateManagerBridge> bridge_;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;

    template <typename N, typename S>
    RocksdbStateTable<K, N, S>* tryRegisterStateTable(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename UK, typename UV>
    RocksdbMapStateTable<K, N, UK, UV>* tryRegisterMapStateTable(
        TypeSerializer* namespaceSerializer, MapStateDescriptor<UK, UV>* stateDesc);
    // pointer to StateTable<K, N, V>, StateDescriptor, namespace BackendDataType
    emhash7::HashMap<std::string, std::tuple<uintptr_t, StateDescriptor*, BackendDataType>> registeredKvStates;
    // pointer to intervalKvState
    emhash7::HashMap<std::string, uintptr_t> createdKvState;
    // [FALCON] pointer to intervalKvState that enable falcon cache
    emhash7::HashMap<std::string, uintptr_t> falconKvState;
    // [FALCON] namespace and value type info of each falconKvState
    emhash7::HashMap<std::string, std::pair<BackendDataType, BackendDataType>> falconNvInfo;

    // [FALCON] flush falcon cache before savepoint and snapshot
    void flushFalconCacheBeforeCheckpoint();

    template <typename N, typename UK, typename UV>
    RocksdbMapState<K, N, UK, UV>* createOrUpdateInternalMapState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename V>
    RocksdbValueState<K, N, V>* createOrUpdateInternalValueState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename V>
    RocksdbListState<K, N, V>* createOrUpdateInternalListState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    void registerKvStateInformation(
        StateDescriptor* stateDesc, TypeSerializer* namespaceSerializer, TypeSerializer* stateSerializer);
};

template <typename K>
void RocksdbKeyedStateBackend<K>::flushFalconCacheBeforeCheckpoint()
{
    // If falcon cache is disabled, falconKvState is empty, this function will do nothing.
    for (auto& entry : falconKvState) {
        // get namespace info and value info of current state
        auto namespaceId = falconNvInfo[entry.first].first;
        auto dataId = falconNvInfo[entry.first].second;
        // for each namespace type and value type, reinterpret_cast type and flush falcon cache
        if (namespaceId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
            auto* state = reinterpret_cast<RocksdbValueState<K, int64_t, RowData*>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (namespaceId == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
            auto* state = reinterpret_cast<RocksdbValueState<K, TimeWindow, RowData*>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (dataId == BackendDataType::ROW_BK) {
            auto* state = reinterpret_cast<RocksdbValueState<K, VoidNamespace, RowData*>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (dataId == BackendDataType::INT_BK) {
            auto* state = reinterpret_cast<RocksdbValueState<K, VoidNamespace, int32_t>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (dataId == BackendDataType::BIGINT_BK) {
            auto* state = reinterpret_cast<RocksdbValueState<K, VoidNamespace, int64_t>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (dataId == BackendDataType::POJO_BK || dataId == BackendDataType::OBJECT_BK) {
            auto* state = reinterpret_cast<RocksdbValueState<K, VoidNamespace, Object*>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (dataId == BackendDataType::SET_LONG) {
            auto* state = reinterpret_cast<RocksdbValueState<K, VoidNamespace, std::vector<long>*>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else if (dataId == BackendDataType::ROW_LIST_BK) {
            // ValueState<List<RowData>> (e.g. SinkUpsertMaterializer "values")
            auto* state =
                reinterpret_cast<RocksdbValueState<K, VoidNamespace, std::vector<RowData*>*>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->flush();
                state->stateCache->clearAll();
            }
        } else {
            NOT_IMPL_EXCEPTION;
        }
    }
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::createOrUpdateInternalState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    registerKvStateInformation(stateDesc, namespaceSerializer, stateDesc->getStateSerializer());
    // How to make this general?
    if (stateDesc->getType() == StateDescriptor::Type::MAP) {
        return this->GetMapState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
        return this->GetValueState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::LIST) {
        return this->GetListState(namespaceSerializer, stateDesc);
    } else {
        NOT_IMPL_EXCEPTION;
    }
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto keyId = stateDesc->getKeyDataId();
    auto valueId = stateDesc->getValueDataId();

    INFO_RELEASE("stateDescType is StateDescriptor::Type::MAP " << ", keyId " << keyId << " , value id " << valueId);

    // Currently only StreamingJoinOperator with BinaryRow uses MapState. It's namespace is VoidNamespace
    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        NOT_IMPL_EXCEPTION;
    }
    // <N, UK, UV>
    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int32_t, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int64_t, int64_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, std::string, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::VARCHAR_BK) {
        // JSON_OBJECTAGG: MapView<VoidNamespace, std::string, std::string> (key -> value JSON text).
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, std::string, std::string>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::VARCHAR_BK) {
        // JSON_ARRAYAGG: MapView<VoidNamespace, long, std::string> (insertion index -> element JSON text).
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int64_t, std::string>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, std::shared_ptr<RowData>, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, RowData*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::TUPLE_INT32_INT32) {
        return (uintptr_t)
            createOrUpdateInternalMapState<VoidNamespace, std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
                namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, TimeWindow, TimeWindow>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, std::vector<RowData*>*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, Object*, Object*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::OBJECT_BK && valueId == BackendDataType::POJO_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, Object*, Object*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::OBJECT_BK && valueId == BackendDataType::OBJECT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, Object*, Object*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::EXTERNAL_BIGINT_BK && valueId == BackendDataType::EXTERNAL_BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int64_t, int64_t>(
            namespaceSerializer, stateDesc);
    }
    NOT_IMPL_EXCEPTION;
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto dataId = stateDesc->getBackendId();
    INFO_RELEASE("stateDescType is StateDescriptor::Type::VALUE " << ", dataId " << dataId);
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<int64_t, RowData*>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<TimeWindow, RowData*>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int32_t>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::POJO_BK || dataId == BackendDataType::OBJECT_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, Object*>(namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::SET_LONG) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, std::vector<long>*>(
            namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::ROW_LIST_BK) {
        // ValueState<List<RowData>> (e.g. SinkUpsertMaterializer "values")
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, std::vector<RowData*>*>(
            namespaceSerializer, stateDesc);
    } else if (dataId == BackendDataType::EXTERNAL_BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else {
        NOT_IMPL_EXCEPTION;
    }
}

template <typename K>
uintptr_t RocksdbKeyedStateBackend<K>::GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto dataId = stateDesc->getBackendId();
    INFO_RELEASE("stateDescType is StateDescriptor::Type::LIST " << ", dataId " << dataId);
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalListState<int64_t, int64_t>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
        dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
    } else if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalListState<int64_t, RowData*>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
        dataId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
    } else {
        NOT_IMPL_EXCEPTION;
    }
}

template <typename K>
template <typename N, typename V>
RocksdbListState<K, N, V>* RocksdbKeyedStateBackend<K>::createOrUpdateInternalListState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    RocksdbStateTable<K, N, V>* stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    RocksdbListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = RocksdbListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = RocksdbListState<K, N, V>::update(
            stateDesc, stateTable, reinterpret_cast<RocksdbListState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->createTable(db, stateDesc->getName(), kvStateInformation_, columnFamilyOptionsFactory_);
    return createdState;
}

// 改这里！！！！！！！（讲道理这里往下都要改）
template <typename K>
template <typename N, typename S>
RocksdbStateTable<K, N, S>* RocksdbKeyedStateBackend<K>::tryRegisterStateTable(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<RocksdbStateTable<K, N, S>*>(std::get<0>(it->second)); // 这里转成Rocksdb
        std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(std::move(restoredKvMetaInfo));
        return stateTable;
    } else {
        std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> newMetaInfo = nullptr;
        if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
            newMetaInfo = std::make_unique<RegisteredKeyValueStateBackendMetaInfo>(
                stateDesc->getType(), stateDesc->getName(), namespaceSerializer, newStateSerializer);
        } else {
            newMetaInfo = std::make_unique<RegisteredKeyValueStateBackendMetaInfo>(
                stateDesc->getName(), namespaceSerializer, newStateSerializer);
        }
        RocksdbStateTable<K, N, S>* stateTable =
            new RocksdbStateTable<K, N, S>(this->context, std::move(newMetaInfo), this->keySerializer);
        std::tuple tuple(reinterpret_cast<uintptr_t>(stateTable), stateDesc, namespaceSerializer->getBackendId());
        registeredKvStates[stateDesc->getName()] = tuple;
        return stateTable;
    }
}

template <typename K>
template <typename N, typename UK, typename UV>
RocksdbMapStateTable<K, N, UK, UV>* RocksdbKeyedStateBackend<K>::tryRegisterMapStateTable(
    TypeSerializer* namespaceSerializer, MapStateDescriptor<UK, UV>* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->GetValueSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable =
            reinterpret_cast<RocksdbMapStateTable<K, N, UK, UV>*>(std::get<0>(it->second)); // 这里转成Rocksdb
        std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(std::move(restoredKvMetaInfo));
        return stateTable;
    } else {
        std::unique_ptr<RegisteredKeyValueStateBackendMetaInfo> newMetaInfo =
            std::make_unique<RegisteredKeyValueStateBackendMetaInfo>(
                stateDesc->getName(), namespaceSerializer, newStateSerializer);
        RocksdbMapStateTable<K, N, UK, UV>* stateTable = new RocksdbMapStateTable<K, N, UK, UV>(
            this->context, std::move(newMetaInfo), this->keySerializer, stateDesc->GetUserKeySerializer());
        std::tuple tuple(reinterpret_cast<uintptr_t>(stateTable), stateDesc, namespaceSerializer->getBackendId());
        registeredKvStates[stateDesc->getName()] = tuple;
        return stateTable;
    }
}

template <typename K>
template <typename N, typename V>
RocksdbValueState<K, N, V>* RocksdbKeyedStateBackend<K>::createOrUpdateInternalValueState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    // For Value state, S is the same as V
    RocksdbStateTable<K, N, V>* stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    RocksdbValueState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = RocksdbValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = RocksdbValueState<K, N, V>::update(
            stateDesc, stateTable, reinterpret_cast<RocksdbValueState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->createTable(db, stateDesc->getName(), kvStateInformation_, columnFamilyOptionsFactory_);

    // [FALCON] -------------------------------------------------------------------------------------------
    auto useStateCache =
        reinterpret_cast<Boolean*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_STATE_CACHE));
    auto cacheSizeLimit = reinterpret_cast<Integer*>(
        Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::STATE_CACHE_SIZE_LIMIT));

    int cacheSize = 3000;
    if (cacheSizeLimit != nullptr) {
        cacheSize = cacheSizeLimit->value;
        cacheSizeLimit->putRefCount();
    }

    if (useStateCache != nullptr && useStateCache->value) {
        // todo: ttl state is not implemented in omniStream, thus falcon does not check it
        // store the reference of all the created value states, all of them enable falcon cache
        falconKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
        falconNvInfo[stateDesc->getName()] =
            std::pair<BackendDataType, BackendDataType>(namespaceSerializer->getBackendId(), stateDesc->getBackendId());
        // after this state is created, update cache size limit for all the created states who use falcon cache.
        int newCacheSize = cacheSize / falconKvState.size();
        INFO_RELEASE(
            "[FALCON] <" << stateDesc->getName()
                         << ", ValueState> enable falcon cache, and update cache size "
                            "to "
                         << newCacheSize << ".");
        for (auto& entry : falconKvState) {
            auto* state = reinterpret_cast<RocksdbValueState<K, N, V>*>(entry.second);
            if (state != nullptr && state->stateCache != nullptr) {
                state->stateCache->updateSizeLimit(newCacheSize);
            }
        }
    }

    if (useStateCache != nullptr) {
        useStateCache->putRefCount();
    }
    // [FALCON] -------------------------------------------------------------------------------------------

    return createdState;
}

template <typename K>
template <typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV>* RocksdbKeyedStateBackend<K>::createOrUpdateInternalMapState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    RocksdbMapStateTable<K, N, UK, UV>* stateTable = tryRegisterMapStateTable<N, UK, UV>(
        namespaceSerializer, reinterpret_cast<MapStateDescriptor<UK, UV>*>(stateDesc));
    auto it = createdKvState.find(stateDesc->getName());
    RocksdbMapState<K, N, UK, UV>* createdState;
    if (it == createdKvState.end()) {
        createdState = RocksdbMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = RocksdbMapState<K, N, UK, UV>::update(
            stateDesc, stateTable, reinterpret_cast<RocksdbMapState<K, N, UK, UV>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    createdState->createTable(db, stateDesc->getName(), kvStateInformation_, columnFamilyOptionsFactory_);
    createdState->setMaxParallelism(maxParallelism_);
    return createdState;
}

template <typename K>
void RocksdbKeyedStateBackend<K>::registerKvStateInformation(
    StateDescriptor* stateDesc, TypeSerializer* namespaceSerializer, TypeSerializer* stateSerializer)
{
    auto it = kvStateInformation_->find(stateDesc->getName());
    if (it != kvStateInformation_->end()) {
        auto rocksDbKvStateInfo = it->second;
        auto newRocksDbKvStateInfo = std::make_shared<RocksDbKvStateInfo>(
            rocksDbKvStateInfo->columnFamilyHandle_, rocksDbKvStateInfo->metaInfo_);
        kvStateInformation_->emplace(stateDesc->getName(), newRocksDbKvStateInfo);
    } else {
        auto metaInfo = std::make_shared<RegisteredKeyValueStateBackendMetaInfo>(
            stateDesc->getType(), stateDesc->getName(), namespaceSerializer, stateSerializer);
        auto rocksDbKvStateInfo = std::make_shared<RocksDbKvStateInfo>(nullptr, metaInfo);
        kvStateInformation_->emplace(stateDesc->getName(), rocksDbKvStateInfo);
    }
    auto stateWithVb = stateDesc->getName() + "vb";
    auto itVb = kvStateInformation_->find(stateWithVb);
    if (itVb != kvStateInformation_->end()) {
        auto rocksDbKvStateInfo = itVb->second;
        auto newRocksDbKvStateInfo = std::make_shared<RocksDbKvStateInfo>(
            rocksDbKvStateInfo->columnFamilyHandle_, rocksDbKvStateInfo->metaInfo_);
        kvStateInformation_->emplace(stateWithVb, newRocksDbKvStateInfo);
    } else {
        auto metaInfo = std::make_shared<RegisteredKeyValueStateBackendMetaInfo>(
            stateDesc->getType(), stateWithVb, namespaceSerializer, stateSerializer);
        auto rocksDbKvStateInfo = std::make_shared<RocksDbKvStateInfo>(nullptr, metaInfo);
        kvStateInformation_->emplace(stateWithVb, rocksDbKvStateInfo);
    }
}
