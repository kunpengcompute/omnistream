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
#ifndef OMNISTREAM_HEAPSNAPSHOTRESOURCEFACTORY_H
#define OMNISTREAM_HEAPSNAPSHOTRESOURCEFACTORY_H

#include <memory>
#include <tuple>
#include <vector>
#include <unordered_map>
#include <emhash7.hpp>
#include "common.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/InternalKeyContext.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/HeapFullSnapshotResources.h"
#include "runtime/state/heap/HeapSingleStateIterator.h"
#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "table/data/RowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

/**
 * Builds the synchronous Heap snapshot resources for checkpoint/savepoint.
 *
 * This mirrors Flink's heap backend architecture more closely than preparing
 * snapshot resources directly inside HeapKeyedStateBackend::snapshot(). The
 * factory owns the sync-phase responsibility of freezing a point-in-time view
 * for async writing.
 *
 * Current implementation materializes KV state into immutable serialized
 * iterator entries during the sync phase. PQ collection is intentionally kept
 * as a dedicated extension point so future integration can participate in the
 * same syncPrepareResources stage without reshaping the call chain again.
 */
template <typename K>
class HeapSnapshotResourceFactory {
public:
    static std::string describeStateBackendId(StateDescriptor* desc)
    {
        if (desc == nullptr) {
            return "null";
        }
        if (desc->getType() == StateDescriptor::Type::MAP) {
            return std::string("mapKey=") + std::to_string(static_cast<int>(desc->getKeyDataId())) +
                   ",mapValue=" + std::to_string(static_cast<int>(desc->getValueDataId()));
        }
        return std::to_string(static_cast<int>(desc->getBackendId()));
    }

    using RegisteredKvStates = emhash7::HashMap<std::string, std::tuple<uintptr_t, StateDescriptor*, BackendDataType>>;
    using RegisteredPQStates =
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>;

    HeapSnapshotResourceFactory(
        TypeSerializer* keySerializer,
        InternalKeyContext<K>* context,
        RegisteredKvStates* registeredKvStates,
        RegisteredPQStates registeredPQStates)
        : keySerializer_(keySerializer),
          context_(context),
          registeredKvStates_(registeredKvStates),
          registeredPQStates_(registeredPQStates)
    {
    }

    std::shared_ptr<HeapFullSnapshotResources> createSnapshotResources(
        long checkpointId, bool captureVectorBatchAccessorData = false)
    {
        PreparedHeapSnapshotData preparedData = prepareSnapshotData(checkpointId, captureVectorBatchAccessorData);
        return std::make_shared<HeapFullSnapshotResources>(
            std::move(preparedData.metaInfoSnapshots),
            std::move(preparedData.stateIterators),
            context_->getKeyGroupRange(),
            keySerializer_,
            preparedData.keyGroupPrefixBytes,
            std::move(preparedData.snapshotStateDataByName));
    }

private:
    struct PreparedHeapSnapshotData {
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfoSnapshots;
        std::vector<std::unique_ptr<SingleStateIterator>> stateIterators;
        std::unordered_map<std::string, std::shared_ptr<HeapSnapshotStateData>> snapshotStateDataByName;
        int keyGroupPrefixBytes = 0;
    };

    PreparedHeapSnapshotData prepareSnapshotData(long checkpointId, bool captureVectorBatchAccessorData)
    {
        PreparedHeapSnapshotData preparedData;
        preparedData.keyGroupPrefixBytes =
            CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(context_->getNumberOfKeyGroups());

        collectKeyValueStateSnapshots(preparedData, checkpointId, captureVectorBatchAccessorData);
        collectPriorityQueueStateSnapshots(preparedData, checkpointId);
        return preparedData;
    }

    void collectPriorityQueueStateSnapshots(PreparedHeapSnapshotData& preparedData, long checkpointId)
    {
        if (!registeredPQStates_ || registeredPQStates_->empty()) {
            return;
        }

        int pqStateId = static_cast<int>(preparedData.metaInfoSnapshots.size());
        for (const auto& pair : *registeredPQStates_) {
            const std::string& stateName = pair.first;
            const auto& wrapper = pair.second;
            if (!wrapper) {
                INFO_RELEASE(
                    "HeapSnapshotResourceFactory: checkpointId=" << checkpointId
                                                                 << ", skip null PQ wrapper, stateName=" << stateName);
                pqStateId++;
                continue;
            }

            try {
                INFO_RELEASE(
                    "HeapSnapshotResourceFactory: checkpointId=" << checkpointId << ", preparing PQ stateName="
                                                                 << stateName << ", pqStateId=" << pqStateId);

                preparedData.metaInfoSnapshots.push_back(wrapper->snapshotMetaInfo());
                auto iterator = wrapper->createSnapshotIterator(pqStateId, preparedData.keyGroupPrefixBytes);
                if (iterator && iterator->isValid()) {
                    preparedData.stateIterators.push_back(std::move(iterator));
                }

                INFO_RELEASE(
                    "HeapSnapshotResourceFactory: checkpointId="
                    << checkpointId << ", prepared PQ stateName=" << stateName << ", pqStateId=" << pqStateId
                    << ", accumulatedMetaInfoCount=" << preparedData.metaInfoSnapshots.size()
                    << ", accumulatedIteratorCount=" << preparedData.stateIterators.size());
            } catch (const std::exception& e) {
                INFO_RELEASE(
                    "Error: HeapSnapshotResourceFactory: checkpointId="
                    << checkpointId << ", failed while preparing PQ stateName=" << stateName
                    << ", pqStateId=" << pqStateId << ", exception=" << e.what());
                throw;
            } catch (...) {
                INFO_RELEASE(
                    "Error: HeapSnapshotResourceFactory: checkpointId="
                    << checkpointId << ", failed while preparing PQ stateName=" << stateName
                    << ", pqStateId=" << pqStateId << ", exception=unknown");
                throw;
            }
            pqStateId++;
        }
    }

    void collectKeyValueStateSnapshots(
        PreparedHeapSnapshotData& preparedData, long checkpointId, bool captureVectorBatchAccessorData)
    {
        int kvStateId = 0;
        for (const auto& pair : *registeredKvStates_) {
            const auto& stateName = pair.first;
            StateDescriptor* desc = std::get<1>(pair.second);
            uintptr_t stateTablePtr = std::get<0>(pair.second);
            auto nsBackendId = std::get<2>(pair.second);
            // All vb state tables share the same type, handle uniformly before type dispatch
            if (stateName.size() >= 2 && stateName.compare(stateName.size() - 2, 2, "vb") == 0) {
                auto* vbTable =
                    reinterpret_cast<CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>*>(
                        stateTablePtr);
                auto* vbMetaInfo = vbTable->getMetaInfo();
                preparedData.metaInfoSnapshots.push_back(vbMetaInfo->snapshot());
                auto iterator =
                    std::make_unique<HeapSingleStateIterator<uint32_t, VoidNamespace, omnistream::VectorBatch*>>(
                        vbTable,
                        kvStateId,
                        preparedData.keyGroupPrefixBytes,
                        HeapSingleStateIterator<uint32_t, VoidNamespace, omnistream::VectorBatch*>::VbDataTag{},
                        captureVectorBatchAccessorData);
                if (captureVectorBatchAccessorData) {
                    iterator->getSnapshotData()->stateName = stateName;
                    preparedData.snapshotStateDataByName[stateName] = iterator->getSnapshotData();
                }
                preparedData.stateIterators.push_back(std::move(iterator));
                kvStateId++;
                continue;
            }

            try {
                if (desc->getType() == StateDescriptor::Type::VALUE) {
                    auto dataId = desc->getBackendId();
                    if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, RowData*>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, int64_t, RowData*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (nsBackendId == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, TimeWindow, RowData*>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, TimeWindow, RowData*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (
                        dataId == BackendDataType::OBJECT_BK || dataId == BackendDataType::POJO_BK ||
                        dataId == BackendDataType::TUPLE_OBJ_OBJ_BK) {
                        auto* table =
                            reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object*>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, Object*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::INT_BK) {
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, int>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::BIGINT_BK || dataId == BackendDataType::EXTERNAL_BIGINT_BK) {
                        auto* table =
                            reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, int64_t>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::ROW_BK) {
                        auto* table =
                            reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData*>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, RowData*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::ROW_LIST_BK) {
                        auto* table =
                            reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<RowData*>*>*>(
                                stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, std::vector<RowData*>*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::SET_LONG) {
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<long>*>*>(
                            stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, std::vector<long>*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else {
                        INFO_RELEASE(
                            "Error:HeapSnapshotResourceFactory: unsupported VALUE type "
                            << dataId << " for snapshot, skipping state=" << desc->getName());
                    }
                } else if (desc->getType() == StateDescriptor::Type::LIST) {
                    auto dataId = desc->getBackendId();
                    if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
                        auto* table =
                            reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<int64_t>*>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, int64_t, std::vector<int64_t>*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (dataId == BackendDataType::BIGINT_BK) {
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t>*>*>(
                            stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, std::vector<int64_t>*>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else {
                        INFO_RELEASE(
                            "Error:HeapSnapshotResourceFactory: unsupported LIST type "
                            << dataId << " for snapshot, skipping state=" << desc->getName());
                    }
                } else if (desc->getType() == StateDescriptor::Type::MAP) {
                    auto keyId = desc->getKeyDataId();
                    auto valueId = desc->getValueDataId();
                    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<int, int>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (
                        keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK ||
                        keyId == BackendDataType::EXTERNAL_BIGINT_BK &&
                            valueId == BackendDataType::EXTERNAL_BIGINT_BK) {
                        using S = emhash7::HashMap<int64_t, int64_t>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<std::string, int>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::VARCHAR_BK) {
                        // JSON_OBJECTAGG: MapView<VoidNamespace, std::string, std::string>.
                        using S = emhash7::HashMap<std::string, std::string>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::VARCHAR_BK) {
                        // JSON_ARRAYAGG: MapView<VoidNamespace, long, std::string>.
                        using S = emhash7::HashMap<int64_t, std::string>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<RowData*, int32_t>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::INT_BK) {
                        using S = emhash7::HashMap<std::shared_ptr<RowData>, int32_t>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
                        using S = emhash7::HashMap<RowData*, RowData*>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (
                        keyId == BackendDataType::SHARED_ROW_BK && valueId == BackendDataType::TUPLE_INT32_INT32) {
                        using S = emhash7::HashMap<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
                        using S = emhash7::HashMap<TimeWindow, TimeWindow>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
                        if (nsBackendId == BackendDataType::BIGINT_BK) {
                            // Window rank "window_rank": MapState<RowData, List<RowData>>
                            // partitioned by the window-end namespace.
                            using S = emhash7::HashMap<RowData*, std::vector<RowData*>*>*;
                            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, S>*>(stateTablePtr);
                            preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                            preparedData.stateIterators.push_back(
                                std::make_unique<HeapSingleStateIterator<K, int64_t, S>>(
                                    table, kvStateId, preparedData.keyGroupPrefixBytes));
                        } else {
                            using S = emhash7::HashMap<RowData*, std::vector<RowData*>*>*;
                            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                            preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                            preparedData.stateIterators.push_back(
                                std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                    table, kvStateId, preparedData.keyGroupPrefixBytes));
                        }
                    } else if (
                        (keyId == BackendDataType::OBJECT_BK || keyId == BackendDataType::POJO_BK) &&
                        (valueId == BackendDataType::OBJECT_BK || valueId == BackendDataType::POJO_BK)) {
                        using S = emhash7::HashMap<Object*, Object*>*;
                        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, S>*>(stateTablePtr);
                        preparedData.metaInfoSnapshots.push_back(table->getMetaInfo()->snapshot());
                        preparedData.stateIterators.push_back(
                            std::make_unique<HeapSingleStateIterator<K, VoidNamespace, S>>(
                                table, kvStateId, preparedData.keyGroupPrefixBytes));
                    } else {
                        INFO_RELEASE(
                            "Error:HeapSnapshotResourceFactory: unsupported MAP type key="
                            << keyId << " value=" << valueId << " for snapshot, skipping state=" << desc->getName());
                    }
                } else {
                    INFO_RELEASE(
                        "Error:HeapSnapshotResourceFactory: unsupported state type for snapshot, skipping state="
                        << desc->getName());
                }
            } catch (const std::exception& e) {
                INFO_RELEASE(
                    "Error:HeapSnapshotResourceFactory: checkpointId="
                    << checkpointId << ", failed while preparing stateName=" << desc->getName()
                    << ", kvStateId=" << kvStateId << ", exception=" << e.what());
                throw;
            } catch (...) {
                INFO_RELEASE(
                    "Error:HeapSnapshotResourceFactory: checkpointId="
                    << checkpointId << ", failed while preparing stateName=" << desc->getName()
                    << ", kvStateId=" << kvStateId << ", exception=unknown");
                throw;
            }
            kvStateId++;
        }
    }

    TypeSerializer* keySerializer_;
    InternalKeyContext<K>* context_;
    RegisteredKvStates* registeredKvStates_;
    RegisteredPQStates registeredPQStates_;
};

#endif // OMNISTREAM_HEAPSNAPSHOTRESOURCEFACTORY_H
