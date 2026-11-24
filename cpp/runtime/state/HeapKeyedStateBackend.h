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
#include <xxhash.h>
#include <map>
#include "common.h"
#include <vector>
#include <set>
#include <unordered_map>

#include "AbstractKeyedStateBackend.h"
#include "HeapPriorityQueuesManager.h"
#include "InternalKeyContext.h"
#include "core/typeutils/TypeSerializer.h"
#include "heap/StateTable.h"
#include "heap/CopyOnWriteStateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/api/common/state/State.h"
#include "heap/HeapMapState.h"
#include "heap/HeapValueState.h"
#include "runtime/state/heap/HeapListState.h"
#include "RegisteredKeyValueStateBackendMetaInfo.h"
#include "table/data/RowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

#include "table/runtime/operators/window/TimeWindow.h"
#include "heap/HeapSingleStateIterator.h"
#include "heap/HeapFullSnapshotResources.h"
#include "heap/HeapSnapshotResourceFactory.h"
#include "heap/HeapSnapshotStrategy.h"
#include "runtime/state/SnapshotStrategyRunner.h"
#include "runtime/state/SavepointResources.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include "table/typeutils/RowDataIntPairSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "table/typeutils/SortedMapValue.h"
#include "core/api/common/state/RestoreStateDescriptor.h"

using namespace omniruntime::type;
/*
 State's value can be
 (1) basic non-map value (2) pointer to non-map value, like RowData*
 (3) pointer to map, like emhash<RowData*, int>* for Join
 (4) very rarely and don't use it, directly a map
*/

// Very simplified class, reduces a lot of unused variables and functions
template <typename K>
class HeapKeyedStateBackend : public AbstractKeyedStateBackend<K> {
public:
    omnistream::StateType getStateType() const noexcept override
    {
        return omnistream::StateType::HEAP;
    }

    HeapKeyedStateBackend(TypeSerializer* keySerializer, InternalKeyContext<K>* context)
        : AbstractKeyedStateBackend<K>(keySerializer, context)
    {
        registeredPQStates_ = std::make_shared<
            std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>();
        auto priorityQueueSetFactory = std::make_shared<HeapPriorityQueueSetFactory>(
            context->getKeyGroupRange(), context->getNumberOfKeyGroups(), 128);
        priorityQueuesManager_ = std::make_shared<HeapPriorityQueuesManager>(
            registeredPQStates_, priorityQueueSetFactory, context->getKeyGroupRange(), context->getNumberOfKeyGroups());

        snapshotResourceFactory_ = std::make_shared<HeapSnapshotResourceFactory<K>>(
            this->keySerializer, this->context, &registeredKvStates, registeredPQStates_);
        checkpointStrategy_ = std::make_shared<HeapSnapshotStrategy<K>>(snapshotResourceFactory_);
    }

    // Originally used to create an internal state, not necessary here
    uintptr_t createOrUpdateInternalState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc) override;

    ~HeapKeyedStateBackend() override
    {
        for (const auto& pair : registeredKvStates) {
            StateDescriptor* desc = std::get<1>(pair.second);
            uintptr_t stateTablePtr = std::get<0>(pair.second);
            BackendDataType nsId = std::get<2>(pair.second);
            if (isVectorBatchSideTableName(pair.first)) {
                auto* stateTable =
                    reinterpret_cast<CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>*>(
                        stateTablePtr);
                auto* keyContext = stateTable->getKeyContext();
                delete stateTable;
                delete keyContext;
                delete desc;
                continue;
            }
            if (desc->getType() == StateDescriptor::Type::MAP) {
                auto keyId = desc->getKeyDataId();
                auto valueId = desc->getValueDataId();
                INFO_RELEASE(
                    "~HeapKeyedStateBackend(), desc->getType():"
                    << static_cast<int>(desc->getType()) << ", desc->getKeyDataId():" << static_cast<int>(keyId)
                    << ", desc->getValueDataId():" << static_cast<int>(valueId));
                if (keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::INT_BK) {
                    auto* stateTable = reinterpret_cast<
                        CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<std::shared_ptr<RowData>, int32_t>*>*>(
                        stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::TUPLE_INT32_INT32) {
                    auto* stateTable = reinterpret_cast<CopyOnWriteStateTable<
                        K,
                        VoidNamespace,
                        emhash7::HashMap<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>*>*>(stateTablePtr);
                    delete stateTable;
                } else if (
                    (keyId == BackendDataType::OBJECT_BK || keyId == BackendDataType::POJO_BK) &&
                    (valueId == BackendDataType::OBJECT_BK || valueId == BackendDataType::POJO_BK)) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<Object*, Object*>*>*>(
                            stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<std::string, int>*>*>(
                            stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::VARCHAR_BK) {
                    // JSON_OBJECTAGG: MapView<VoidNamespace, std::string, std::string>.
                    auto stateTable = reinterpret_cast<
                        CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<std::string, std::string>*>*>(
                        stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::VARCHAR_BK) {
                    // JSON_ARRAYAGG: MapView<VoidNamespace, long, std::string>.
                    auto stateTable = reinterpret_cast<
                        CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int64_t, std::string>*>*>(
                        stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int, int>*>*>(
                            stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int64_t, int64_t>*>*>(
                            stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
                    if (nsId == BackendDataType::BIGINT_BK) {
                        // Window rank "window_rank": MapState<RowData, List<RowData>>
                        // partitioned by the window-end namespace.
                        auto stateTable = reinterpret_cast<CopyOnWriteStateTable<
                            K, int64_t, emhash7::HashMap<RowData*, std::vector<RowData*>*>*>*>(stateTablePtr);
                        delete stateTable;
                    } else {
                        auto stateTable = reinterpret_cast<
                            CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData*, std::vector<RowData*>*>*>*>(
                            stateTablePtr);
                        delete stateTable;
                    }
                } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
                    auto stateTable = reinterpret_cast<CopyOnWriteStateTable<
                        K,
                        VoidNamespace,
                        emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>*>*>(stateTablePtr);
                    delete stateTable;
                } else if (
                    keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
                    auto stateTable = reinterpret_cast<CopyOnWriteStateTable<
                        K,
                        VoidNamespace,
                        emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>*>*>(stateTablePtr);
                    delete stateTable;
                } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
                    auto stateTable = reinterpret_cast<
                        CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<TimeWindow, TimeWindow>*>*>(
                        stateTablePtr);
                    delete stateTable;
                } else if (
                    keyId == BackendDataType::EXTERNAL_BIGINT_BK && valueId == BackendDataType::EXTERNAL_BIGINT_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int64_t, int64_t>*>*>(
                            stateTablePtr);
                    delete stateTable;
                } else {
                    NOT_IMPL_EXCEPTION;
                }
            } else if (desc->getType() == StateDescriptor::Type::VALUE) {
                auto dataId = desc->getBackendId();
                INFO_RELEASE(
                    "~HeapKeyedStateBackend(), desc->getType():"
                    << static_cast<int>(desc->getType()) << ", desc->getBackendId():" << static_cast<int>(dataId));
                if (dataId == BackendDataType::OBJECT_BK || dataId == BackendDataType::POJO_BK ||
                    dataId == BackendDataType::TUPLE_OBJ_OBJ_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object*>*>(stateTablePtr);
                    delete stateTable;
                } else if (dataId == BackendDataType::INT_BK) {
                    auto stateTable = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int>*>(stateTablePtr);
                    delete stateTable;
                } else if (dataId == BackendDataType::BIGINT_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                    delete stateTable;
                } else if (dataId == BackendDataType::ROW_BK) {
                    if (nsId == BackendDataType::BIGINT_BK) {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, int64_t, RowData*>*>(stateTablePtr);
                    } else if (nsId == BackendDataType::TIME_WINDOW_BK) {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, TimeWindow, RowData*>*>(stateTablePtr);
                    } else {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData*>*>(stateTablePtr);
                    }
                } else if (dataId == BackendDataType::ROW_LIST_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<RowData*>*>*>(stateTablePtr);
                    delete stateTable;
                } else if (dataId == BackendDataType::SET_LONG) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<long>*>*>(stateTablePtr);
                    delete stateTable;
                } else if (dataId == BackendDataType::EXTERNAL_BIGINT_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                    delete stateTable;
                } else if (dataId == BackendDataType::SORTED_MAP_BK) {
                    auto stateTable =
                        reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, SortedMapValue*>*>(stateTablePtr);
                    delete stateTable;
                } else {
                    NOT_IMPL_EXCEPTION;
                }
            } else if (desc->getType() == StateDescriptor::Type::LIST) {
                auto dataId = desc->getBackendId();
                INFO_RELEASE(
                    "~HeapKeyedStateBackend(), desc->getType():"
                    << static_cast<int>(desc->getType()) << ", desc->getBackendId():" << static_cast<int>(dataId));
                if (dataId == BackendDataType::BIGINT_BK) {
                    if (nsId == BackendDataType::BIGINT_BK) {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<int64_t>*>*>(
                            stateTablePtr);
                    } else {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t>*>*>(
                            stateTablePtr);
                    }
                } else if (dataId == BackendDataType::ROW_BK) {
                    if (nsId == BackendDataType::BIGINT_BK) {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<RowData*>*>*>(
                            stateTablePtr);
                    } else {
                        delete reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<RowData*>*>*>(
                            stateTablePtr);
                    }
                } else {
                    NOT_IMPL_EXCEPTION;
                }
            }
            delete desc;
        }

        for (const auto& pair : createdKvState) {
            delete reinterpret_cast<State*>(pair.second);
        }
    };

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions)
    {
        auto snapshotRunner = std::make_unique<SnapshotStrategyRunner<KeyedStateHandle, FullSnapshotResources>>(
            "Heap full snapshot", checkpointStrategy_, SnapshotExecutionType::ASYNCHRONOUS);
        return snapshotRunner->snapshot(
            checkpointId, timestamp, streamFactory, checkpointOptions, omniTaskBridge_, this->keySerializer->toJson());
    }

    std::shared_ptr<SavepointResources> savepoint() override
    {
        auto snapshotResources = snapshotResourceFactory_->createSnapshotResources(-1L);
        return std::make_shared<SavepointResources>(snapshotResources, SnapshotExecutionType::ASYNCHRONOUS);
    }

    std::shared_ptr<SavepointResources> compatibleSavepoint() override
    {
        auto snapshotResources = snapshotResourceFactory_->createSnapshotResources(-1L, true);
        return std::make_shared<SavepointResources>(snapshotResources, SnapshotExecutionType::ASYNCHRONOUS);
    }

    void setOmniTaskBridge(const std::shared_ptr<omnistream::OmniTaskBridge>& bridge)
    {
        omniTaskBridge_ = bridge;
    }

    /**
     * Returns the type-erased state table pointer for a given state name.
     * Returns 0 if the state name is not found.
     */
    uintptr_t getStateTablePtr(const std::string& stateName) const
    {
        auto it = registeredKvStates.find(stateName);
        if (it != registeredKvStates.end()) {
            return std::get<0>(it->second);
        }
        return 0;
    }

    /**
     * Returns the registered state descriptor and namespace BackendDataType for a given state name.
     * Returns nullptr if not found.
     */
    std::tuple<uintptr_t, StateDescriptor*, BackendDataType>* getRegisteredState(const std::string& stateName)
    {
        auto it = registeredKvStates.find(stateName);
        if (it != registeredKvStates.end()) {
            return &(it->second);
        }
        return nullptr;
    }

    /**
     * Creates a RestoreStateDescriptor from a StateMetaInfoSnapshot.
     * For MAP states, extracts key/value BackendDataType from MapSerializer.
     * For LIST states, extracts element BackendDataType from ListSerializer.
     * For VALUE states, uses the value serializer's BackendDataType.
     */
    static StateDescriptor* createRestoreDescriptor(
        const StateMetaInfoSnapshot& metaInfo,
        StateDescriptor::Type stateType,
        TypeSerializer* nsSerializer,
        TypeSerializer* valSerializer)
    {
        if (stateType == StateDescriptor::Type::MAP) {
            auto* mapSer = dynamic_cast<MapSerializer*>(valSerializer);
            if (mapSer) {
                return new RestoreStateDescriptor(
                    metaInfo.getName(),
                    stateType,
                    valSerializer,
                    BackendDataType::INVALID_BK,
                    mapSer->getKeySerializer()->getBackendId(),
                    mapSer->getValueSerializer()->getBackendId());
            }
            // Fallback: OBJECT×OBJECT if MapSerializer cast fails
            return new RestoreStateDescriptor(
                metaInfo.getName(),
                stateType,
                valSerializer,
                BackendDataType::INVALID_BK,
                BackendDataType::OBJECT_BK,
                BackendDataType::OBJECT_BK);
        } else if (stateType == StateDescriptor::Type::LIST) {
            auto* listSer = dynamic_cast<ListSerializer*>(valSerializer);
            BackendDataType elemId =
                listSer ? listSer->getElementSerializer()->getBackendId() : valSerializer->getBackendId();
            return new RestoreStateDescriptor(metaInfo.getName(), stateType, valSerializer, elemId);
        } else {
            // VALUE
            return new RestoreStateDescriptor(
                metaInfo.getName(), stateType, valSerializer, valSerializer->getBackendId());
        }
    }

    template <typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
        std::string stateName, TypeSerializer* byteOrderedElementSerializer)
    {
        auto queue = priorityQueuesManager_->createOrUpdate<K, T, Comparator>(stateName, byteOrderedElementSerializer);
        restorePendingPriorityQueueEntries(stateName);
        return queue;
    }

    template <typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
        std::string stateName, TypeSerializer* byteOrderedElementSerializer, bool allowFutureMetadataUpdates)
    {
        auto queue = priorityQueuesManager_->createOrUpdate<K, T, Comparator>(
            stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        restorePendingPriorityQueueEntries(stateName);
        return queue;
    }

    void addRestoredPriorityQueueEntry(
        const std::string& stateName, const std::vector<int8_t>& serializedKey, int keyGroupPrefixBytes)
    {
        auto wrapperIt = registeredPQStates_->find(stateName);
        if (wrapperIt != registeredPQStates_->end() && wrapperIt->second != nullptr) {
            wrapperIt->second->restoreSerializedElement(serializedKey, keyGroupPrefixBytes);
            return;
        }

        pendingRestoredPQEntries_[stateName].push_back(PendingPriorityQueueEntry{serializedKey, keyGroupPrefixBytes});
    }

    size_t getPendingPriorityQueueRestoreEntryCount(const std::string& stateName) const
    {
        auto it = pendingRestoredPQEntries_.find(stateName);
        return it == pendingRestoredPQEntries_.end() ? 0 : it->second.size();
    }

private:
    struct PendingPriorityQueueEntry {
        std::vector<int8_t> serializedKey;
        int keyGroupPrefixBytes;
    };

    void restorePendingPriorityQueueEntries(const std::string& stateName)
    {
        auto pendingIt = pendingRestoredPQEntries_.find(stateName);
        if (pendingIt == pendingRestoredPQEntries_.end()) {
            return;
        }

        auto wrapperIt = registeredPQStates_->find(stateName);
        if (wrapperIt == registeredPQStates_->end() || wrapperIt->second == nullptr) {
            return;
        }

        size_t restoredCount = 0;
        for (const auto& entry : pendingIt->second) {
            wrapperIt->second->restoreSerializedElement(entry.serializedKey, entry.keyGroupPrefixBytes);
            restoredCount++;
        }
        INFO_RELEASE(
            "HeapKeyedStateBackend: restored pending PRIORITY_QUEUE state='" << stateName
                                                                             << "' entries=" << restoredCount);
        pendingRestoredPQEntries_.erase(pendingIt);
    }

    template <typename N, typename S>
    StateTable<K, N, S>* tryRegisterStateTable(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* tryRegisterVectorBatchStateTable(
        StateDescriptor* stateDesc, KeyGroupRange* parentKeyGroupRange, int parentNumberOfKeyGroups);

    static std::string vectorBatchSideTableName(const std::string& logicalStateName)
    {
        return logicalStateName + "vb";
    }

    static bool isVectorBatchSideTableName(const std::string& stateName)
    {
        const std::string suffix = "vb";
        return stateName.size() >= suffix.size() &&
               stateName.compare(stateName.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    // pointer to StateTable<K, N, V>, StateDescriptor, namespace BackendDataType
    emhash7::HashMap<std::string, std::tuple<uintptr_t, StateDescriptor*, BackendDataType>> registeredKvStates;
    // pointer to intervalKvState
    emhash7::HashMap<std::string, uintptr_t> createdKvState;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
        registeredPQStates_;
    std::unordered_map<std::string, std::vector<PendingPriorityQueueEntry>> pendingRestoredPQEntries_;
    std::shared_ptr<HeapPriorityQueuesManager> priorityQueuesManager_;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;
    std::shared_ptr<HeapSnapshotResourceFactory<K>> snapshotResourceFactory_;
    std::shared_ptr<HeapSnapshotStrategy<K>> checkpointStrategy_;

    template <typename N, typename UK, typename UV>
    HeapMapState<K, N, UK, UV>* createOrUpdateInternalMapState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename V>
    HeapValueState<K, N, V>* createOrUpdateInternalValueState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename V>
    HeapListState<K, N, V>* createOrUpdateInternalListState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);
};

template <typename K>
uintptr_t HeapKeyedStateBackend<K>::createOrUpdateInternalState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto stateDescType = stateDesc->getType();
    if (stateDescType == StateDescriptor::Type::MAP) {
        auto keyId = stateDesc->getKeyDataId();
        auto valueId = stateDesc->getValueDataId();

        INFO_RELEASE(
            "stateDescType is StateDescriptor::Type::MAP " << ", keyId " << keyId << " , value id " << valueId);

        if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
            // Window rank: MapState<RowData sortKey, List<RowData>> partitioned by the
            // window-end namespace (LongSerializer), mirroring Flink's
            // WindowMapState<Long, List<RowData>> in WindowRankProcessor.
            if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK &&
                keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
                return (uintptr_t)createOrUpdateInternalMapState<int64_t, RowData*, std::vector<RowData*>*>(
                    namespaceSerializer, stateDesc);
            }
            NOT_IMPL_EXCEPTION;
        }
        //<N, UK, UV>
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
        } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::TUPLE_INT32_INT32) {
            return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, std::tuple<int32_t, int32_t>>(
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
        } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_INT_PAIR_BK) {
            // UpdatableTopNFunction's "data-state-with-update": MapState<rowKey, RowDataIntPair>.
            return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, RowDataIntPair*>(
                namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
            return (uintptr_t)
                createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int64_t>>(
                    namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
            return (uintptr_t)
                createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(
                    namespaceSerializer, stateDesc);
        } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::OBJECT_BK) {
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
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else if (stateDescType == StateDescriptor::Type::VALUE) {
        auto dataId = stateDesc->getBackendId();
        INFO_RELEASE("stateDescType is StateDescriptor::Type::VALUE " << ", dataId " << dataId);
        if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<int64_t, RowData*>(namespaceSerializer, stateDesc);
        } else if (
            namespaceSerializer->getBackendId() == BackendDataType::TIME_WINDOW_BK &&
            dataId == BackendDataType::ROW_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<TimeWindow, RowData*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::ROW_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::ROW_LIST_BK) {
            // ValueState<List<RowData>> (e.g. SinkUpsertMaterializer "values")
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, std::vector<RowData*>*>(
                namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::INT_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int32_t>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::BIGINT_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::OBJECT_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, Object*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::POJO_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, Object*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::TUPLE_OBJ_OBJ_BK) {
            // Tuple2/TupleN 是 Object 子类，与 OBJECT_BK 共用 state table 类型
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, Object*>(namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::SET_LONG) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, std::vector<long>*>(
                namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::SORTED_MAP_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, SortedMapValue*>(
                namespaceSerializer, stateDesc);
        } else if (dataId == BackendDataType::EXTERNAL_BIGINT_BK) {
            return (uintptr_t)createOrUpdateInternalValueState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else if (stateDescType == StateDescriptor::Type::LIST) {
        auto dataId = stateDesc->getBackendId();
        INFO_RELEASE("stateDescType is StateDescriptor::Type::LIST " << ", dataId " << dataId);
        if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
            return (uintptr_t)createOrUpdateInternalListState<int64_t, int64_t>(namespaceSerializer, stateDesc);
        } else if (
            namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
            dataId == BackendDataType::BIGINT_BK) {
            return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);
        } else if (
            namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
            return (uintptr_t)createOrUpdateInternalListState<int64_t, RowData*>(namespaceSerializer, stateDesc);
        } else if (
            namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
            dataId == BackendDataType::ROW_BK) {
            return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
        } else {
            NOT_IMPL_EXCEPTION;
        }
    } else {
        THROW_LOGIC_EXCEPTION("unknown stateDescType: " << static_cast<int>(stateDescType));
    }
}

template <typename K>
template <typename N, typename S>
StateTable<K, N, S>* HeapKeyedStateBackend<K>::tryRegisterStateTable(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<CopyOnWriteStateTable<K, N, S>*>(std::get<0>(it->second));
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        // 必须显式传 stateType，否则会走到 3 参构造内部默认填 Type::UNKNOWN，
        // CP 元数据里 KEYED_STATE_TYPE 永远是 0，restore 时 dispatch 落到 NOT_IMPL。
        RegisteredKeyValueStateBackendMetaInfo* newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo(
            stateDesc->getType(), stateDesc->getName(), namespaceSerializer, newStateSerializer);
        StateTable<K, N, S>* stateTable =
            new CopyOnWriteStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        std::tuple tuple(reinterpret_cast<uintptr_t>(stateTable), stateDesc, namespaceSerializer->getBackendId());
        registeredKvStates[stateDesc->getName()] = tuple;
        return stateTable;
    }
}

template <typename K>
StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>*
HeapKeyedStateBackend<K>::tryRegisterVectorBatchStateTable(
    StateDescriptor* stateDesc, KeyGroupRange* parentKeyGroupRange, int parentNumberOfKeyGroups)
{
    const std::string vbName = vectorBatchSideTableName(stateDesc->getName());
    auto it = registeredKvStates.find(vbName);
    if (it != registeredKvStates.end()) {
        return reinterpret_cast<CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>*>(
            std::get<0>(it->second));
    }

    KeyGroupRange* vbKeyGroupRange =
        new KeyGroupRange(parentKeyGroupRange->getStartKeyGroup(), parentKeyGroupRange->getEndKeyGroup());
    auto* vectorBatchKeyContext = new InternalKeyContextImpl<uint32_t>(vbKeyGroupRange, parentNumberOfKeyGroups);
    RegisteredKeyValueStateBackendMetaInfo* metaInfo = new RegisteredKeyValueStateBackendMetaInfo(
        StateDescriptor::Type::VALUE, vbName, new VoidNamespaceSerializer(), new LongSerializer());
    auto* vectorBatchStateTable = new CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>(
        vectorBatchKeyContext, metaInfo, new IntSerializer());
    auto* vbDesc = new ValueStateDescriptor<omnistream::VectorBatch*>(vbName, new LongSerializer());
    registeredKvStates[vbName] =
        std::make_tuple(reinterpret_cast<uintptr_t>(vectorBatchStateTable), vbDesc, BackendDataType::VOID_NAMESPACE_BK);
    return vectorBatchStateTable;
}

template <typename K>
template <typename N, typename V>
HeapListState<K, N, V>* HeapKeyedStateBackend<K>::createOrUpdateInternalListState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    using S = std::vector<V>*;
    StateTable<K, N, S>* stateTable = tryRegisterStateTable<N, S>(namespaceSerializer, stateDesc);
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable =
        tryRegisterVectorBatchStateTable(stateDesc, stateTable->getKeyGroupRange(), stateTable->getNumberOfKeyGroups());
    auto it = createdKvState.find(stateDesc->getName());
    HeapListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState =
            HeapListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer(), vectorBatchStateTable);
    } else {
        createdState = HeapListState<K, N, V>::update(
            stateDesc, stateTable, reinterpret_cast<HeapListState<K, N, V>*>(it->second), vectorBatchStateTable);
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    return createdState;
}

template <typename K>
template <typename N, typename V>
HeapValueState<K, N, V>* HeapKeyedStateBackend<K>::createOrUpdateInternalValueState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    // For Value state, S is the same as V
    StateTable<K, N, V>* stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable =
        tryRegisterVectorBatchStateTable(stateDesc, stateTable->getKeyGroupRange(), stateTable->getNumberOfKeyGroups());
    auto it = createdKvState.find(stateDesc->getName());
    HeapValueState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState =
            HeapValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer(), vectorBatchStateTable);
    } else {
        createdState = HeapValueState<K, N, V>::update(
            stateDesc, stateTable, reinterpret_cast<HeapValueState<K, N, V>*>(it->second), vectorBatchStateTable);
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    return createdState;
}

template <typename K>
template <typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV>* HeapKeyedStateBackend<K>::createOrUpdateInternalMapState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    using S = emhash7::HashMap<UK, UV>*;
    StateTable<K, N, S>* stateTable = tryRegisterStateTable<N, S>(namespaceSerializer, stateDesc);
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable =
        tryRegisterVectorBatchStateTable(stateDesc, stateTable->getKeyGroupRange(), stateTable->getNumberOfKeyGroups());
    auto it = createdKvState.find(stateDesc->getName());
    HeapMapState<K, N, UK, UV>* createdState;
    if (it == createdKvState.end()) {
        createdState =
            HeapMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer(), vectorBatchStateTable);
    } else {
        createdState = HeapMapState<K, N, UK, UV>::update(
            stateDesc, stateTable, reinterpret_cast<HeapMapState<K, N, UK, UV>*>(it->second), vectorBatchStateTable);
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    return createdState;
}
