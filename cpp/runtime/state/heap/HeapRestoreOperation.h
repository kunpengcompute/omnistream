/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include <set>
#include <vector>
#include <unordered_map>

#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/FullSnapshotRestoreOperation.h"
#include "runtime/state/restore/SavepointRestoreResult.h"
#include "runtime/state/restore/SavepointRestoreResultIterator.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptorFactory.h"
#include "core/typeutils/LongSerializer.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "table/data/util/SequenceNumberHelper.h"
#include "runtime/state/VoidNamespace.h"
#include "table/data/RowData.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "core/typeutils/XxH128_hashSerializer.h"
#include "core/utils/key_type_traits.h"

/**
 * CP专用恢复操作，对位Flink HeapRestoreOperation。
 *
 * 与FullSnapshotRestoreOperation的区别：
 * 1. 仅处理KeyGroupsStateHandle（CP句柄）
 * 2. 使用heap-native帧格式解析
 * 3. 通过JNI getKeyGroupEntries获取KV entries（暂复用SP路径）
 * 4. 支持PQ状态的延迟恢复机制
 *
 * 当前实现：复用FullSnapshotRestoreOperation读取entries，后续可优化为JNI getNativeKeyGroupEntries
 */
template <typename K>
class HeapRestoreOperation {
public:
    HeapRestoreOperation(
        HeapKeyedStateBackend<K>* backend,
        const KeyGroupRange* keyGroupRange,
        const std::vector<std::shared_ptr<KeyedStateHandle>>& stateHandles,
        std::shared_ptr<TypeSerializer> keySerializer,
        int numberOfKeyGroups,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge)
        : backend_(backend),
          keyGroupRange_(keyGroupRange),
          stateHandles_(stateHandles),
          keySerializer_(keySerializer),
          numberOfKeyGroups_(numberOfKeyGroups),
          bridge_(bridge)
    {
        keyGroupPrefixBytes_ = CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
    }

    void restore()
    {
        // 复用FullSnapshotRestoreOperation读取entries
        FullSnapshotRestoreOperation<K> restoreOp(
            const_cast<KeyGroupRange*>(keyGroupRange_), stateHandles_, keySerializer_, bridge_);
        auto restoreIterator = restoreOp.restore();

        while (restoreIterator->hasNext()) {
            auto restoreResult = restoreIterator->next();
            restoreFromResult(std::move(restoreResult));
        }
    }

private:
    /** Info collected per state during restore Phase 1, used in Phase 2 for deserialization. */
    struct RestoreStateInfo {
        StateMetaInfoSnapshot::BackendStateType backendStateType;
        std::string stateName;
        StateDescriptor* stateDesc;
        TypeSerializer* namespaceSerializer;
        TypeSerializer* valueSerializer;
    };

    HeapKeyedStateBackend<K>* backend_;
    const KeyGroupRange* keyGroupRange_;
    std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles_;
    std::shared_ptr<TypeSerializer> keySerializer_;
    int numberOfKeyGroups_;
    std::shared_ptr<omnistream::OmniTaskBridge> bridge_;
    int keyGroupPrefixBytes_;

    void restoreFromResult(std::unique_ptr<SavepointRestoreResult> restoreResult)
    {
        auto& metaInfos = restoreResult->getStateMetaInfoSnapshots();
        auto keyGroupIterator = restoreResult->getKeyGroupIterator();

        // Phase 1: 创建状态表，记录PQ状态
        std::vector<RestoreStateInfo> stateInfos;
        stateInfos.reserve(metaInfos.size());

        for (size_t i = 0; i < metaInfos.size(); i++) {
            auto& metaInfo = metaInfos[i];
            auto backendStateType = metaInfo.getBackendStateType();

            if (backendStateType == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
                // PQ状态：记录待恢复，后续timer queue创建时装填
                stateInfos.push_back(
                    {backendStateType, metaInfo.getName(), nullptr, nullptr, metaInfo.getValueSerializer()});
                continue;
            }

            if (backendStateType != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
                ERROR_RELEASE(
                    "HeapRestoreOperation: skipping unsupported backend state type for state '" << metaInfo.getName()
                                                                                                << "'");
                THROW_LOGIC_EXCEPTION(
                    "Unsupported backend state type in heap keyed state restore. state='"
                    << metaInfo.getName() << "', kvStateId=" << i
                    << ", backendStateType=" << static_cast<int>(backendStateType));
            }

            std::string stateTypeStr = metaInfo.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
            StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeStr);

            TypeSerializer* nsSerializer = metaInfo.getNamespaceSerializer();
            TypeSerializer* valSerializer = metaInfo.getValueSerializer();

            if (nsSerializer == nullptr || valSerializer == nullptr) {
                ERROR_RELEASE(
                    "HeapRestoreOperation skipping state '" << metaInfo.getName() << "' — missing serializer(s)");
                THROW_RUNTIME_ERROR(
                    "HeapRestoreOperation skipping state '" << metaInfo.getName() << "' — missing serializer(s)");
            }

            StateDescriptor* desc = backend_->createRestoreDescriptor(metaInfo, stateType, nsSerializer, valSerializer);
            backend_->createOrUpdateInternalState(nsSerializer, desc);
            stateInfos.push_back({backendStateType, metaInfo.getName(), desc, nsSerializer, valSerializer});
        }

        // Phase 2: 遍历KV entries，反序列化并写入状态表
        while (keyGroupIterator->hasNext()) {
            auto keyGroup = keyGroupIterator->next();
            int keyGroupId = keyGroup->getKeyGroupId();
            auto entryIter = keyGroup->getKeyGroupEntries();

            while (entryIter->hasNext()) {
                auto entry = entryIter->next();
                int kvStateId = entry.getKvStateId();

                if (kvStateId < 0 || kvStateId >= static_cast<int>(stateInfos.size())) {
                    ERROR_RELEASE("HeapRestoreOperation: invalid kvStateId " << kvStateId << ", skipping entry");
                    continue;
                }

                auto& info = stateInfos[kvStateId];

                if (info.backendStateType == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
                    // PQ条目 -> 暂存到pendingRestoredPQEntries_
                    backend_->addRestoredPriorityQueueEntry(info.stateName, entry.getKey(), keyGroupPrefixBytes_);
                    continue;
                }

                if (info.stateDesc == nullptr) {
                    continue; // State was skipped in Phase 1
                }

                restoreEntryToHeap(backend_, info, keyGroupId, keyGroupPrefixBytes_, entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Restored objects are stored in heap state, so they must not alias serializer reuse buffers.
     */
    static Object* copyRestoredObjectForState(Object* value)
    {
        return value == nullptr ? nullptr : value->clone();
    }

    template <typename T>
    static T copyRestoredPointerForState(T value)
    {
        if constexpr (std::is_same_v<T, Object*>) {
            return static_cast<T>(copyRestoredObjectForState(value));
        } else if constexpr (KeyTypeTraits<T>::isRowKey) {
            return value == nullptr ? nullptr : static_cast<T>(value->copy());
        } else {
            return value;
        }
    }

    static void releaseRestoredObject(Object* value)
    {
        if (value != nullptr) {
            value->putRefCount();
        }
    }

    /**
     * Deserializes a std::vector from checkpoint bytes using the ListSerializer's
     * element serializer. Format: [int size] [elem_1] [elem_2] ...
     * Mirrors HeapSingleStateIterator::serializeVector().
     */
    template <typename V>
    static std::vector<V>* deserializeVector(TypeSerializer* elemSer, DataInputDeserializer& input)
    {
        int size = input.readInt();
        auto vec = std::make_unique<std::vector<V>>();
        vec->reserve(size);
        for (int i = 0; i < size; i++) {
            if constexpr (std::is_same_v<V, Object*>) {
                Object* buf = elemSer->GetBuffer();
                elemSer->deserialize(buf, input);
                vec->push_back(copyRestoredPointerForState<V>(buf));
                releaseRestoredObject(buf);
            } else if constexpr (std::is_pointer_v<V>) {
                V raw = static_cast<V>(elemSer->deserialize(input));
                vec->push_back(copyRestoredPointerForState<V>(raw));
            } else {
                void* raw = elemSer->deserialize(input);
                vec->push_back(*static_cast<V*>(raw));
                delete static_cast<V*>(raw);
            }
        }
        return vec.release();
    }

    /**
     * Deserializes a single KV entry from checkpoint bytes and inserts it into the correct
     * typed CopyOnWriteStateTable. Mirrors HeapKeyedStateBackendBuilder::restoreEntryToHeap.
     */
    void restoreEntryToHeap(
        HeapKeyedStateBackend<K>* backend,
        const RestoreStateInfo& info,
        int keyGroupId,
        int keyGroupPrefixBytes,
        const std::vector<int8_t>& keyBytes,
        const std::vector<int8_t>& valueBytes)
    {
        // Deserialize key + namespace from keyBytes (skip keyGroupPrefix)
        DataInputDeserializer keyInput(
            reinterpret_cast<const uint8_t*>(keyBytes.data()), static_cast<int>(keyBytes.size()), keyGroupPrefixBytes);

        // Deserialize key
        void* rawKey = nullptr;
        Object* keyObjForObjectKBackend = nullptr;
        if constexpr (std::is_same_v<K, Object*>) {
            keyObjForObjectKBackend = keySerializer_->GetBuffer();
            keySerializer_->deserialize(keyObjForObjectKBackend, keyInput);
            rawKey = new Object*(keyObjForObjectKBackend);
        } else if constexpr (std::is_pointer_v<K>) {
            rawKey = new K(static_cast<K>(keySerializer_->deserialize(keyInput)));
        } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
            using KeyBaseType = unwrap_shared_ptr_t<K>;
            auto* keyBuffer = static_cast<KeyBaseType*>(keySerializer_->deserialize(keyInput));
            if (keyBuffer == nullptr) {
                ERROR_RELEASE("HeapRestoreOperation: Heap keyed state restore deserialized a null shared row key");
                THROW_LOGIC_EXCEPTION("Heap keyed state restore deserialized a null shared row key");
            }
            // BinaryRowDataSerializer reuses its deserialize buffer. Keep an owned copy
            // before restoring the next entry.
            rawKey = new K(std::shared_ptr<KeyBaseType>(static_cast<KeyBaseType*>(keyBuffer->copy())));
        } else if constexpr (is_shared_ptr_v<K>) {
            NOT_IMPL_EXCEPTION;
        } else {
            rawKey = keySerializer_->deserialize(keyInput);
        }

        // Object* key引用RAII守卫
        struct ObjKeyRefGuard {
            Object* obj;
            ~ObjKeyRefGuard()
            {
                if (obj != nullptr) {
                    obj->putRefCount();
                }
            }
        } objKeyGuard{keyObjForObjectKBackend};

        // Deserialize namespace
        void* rawNs = info.namespaceSerializer->deserialize(keyInput);

        // Deserialize value from valueBytes
        DataInputDeserializer valInput(
            reinterpret_cast<const uint8_t*>(valueBytes.data()), static_cast<int>(valueBytes.size()), 0);

        auto* desc = info.stateDesc;
        uintptr_t stateTablePtr = backend->getStateTablePtr(desc->getName());

        if (stateTablePtr == 0) {
            ERROR_RELEASE(
                "HeapRestoreOperation: state table not found for '" << desc->getName() << "', skipping entry");
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
            return;
        }

        auto nsBackendId = info.namespaceSerializer->getBackendId();

        if (desc->getType() == StateDescriptor::Type::VALUE) {
            auto dataId = desc->getBackendId();

            if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
                void* rawVal = info.valueSerializer->deserialize(valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, RowData*>*>(stateTablePtr);
                table->put(
                    *static_cast<K*>(rawKey),
                    keyGroupId,
                    *static_cast<int64_t*>(rawNs),
                    static_cast<RowData*>(rawVal)->copy());
                delete static_cast<K*>(rawKey);
                delete static_cast<int64_t*>(rawNs);
            } else if (nsBackendId == BackendDataType::TIME_WINDOW_BK && dataId == BackendDataType::ROW_BK) {
                void* rawVal = info.valueSerializer->deserialize(valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, TimeWindow, RowData*>*>(stateTablePtr);
                table->put(
                    *static_cast<K*>(rawKey),
                    keyGroupId,
                    *static_cast<TimeWindow*>(rawNs),
                    static_cast<RowData*>(rawVal)->copy());
                delete static_cast<K*>(rawKey);
                delete static_cast<TimeWindow*>(rawNs);
            } else if (
                dataId == BackendDataType::OBJECT_BK || dataId == BackendDataType::POJO_BK ||
                dataId == BackendDataType::TUPLE_OBJ_OBJ_BK) {
                Object* valObj = info.valueSerializer->GetBuffer();
                info.valueSerializer->deserialize(valObj, valInput);
                Object* stateVal = copyRestoredObjectForState(valObj);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, Object*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), stateVal);
                releaseRestoredObject(stateVal);
                releaseRestoredObject(valObj);
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
            } else if (dataId == BackendDataType::INT_BK) {
                void* rawVal = info.valueSerializer->deserialize(valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int>*>(stateTablePtr);
                table->put(
                    *static_cast<K*>(rawKey),
                    keyGroupId,
                    *static_cast<VoidNamespace*>(rawNs),
                    *static_cast<int*>(rawVal));
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
                delete static_cast<int*>(rawVal);
            } else if (dataId == BackendDataType::BIGINT_BK || dataId == BackendDataType::EXTERNAL_BIGINT_BK) {
                void* rawVal = info.valueSerializer->deserialize(valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, int64_t>*>(stateTablePtr);
                table->put(
                    *static_cast<K*>(rawKey),
                    keyGroupId,
                    *static_cast<VoidNamespace*>(rawNs),
                    *static_cast<int64_t*>(rawVal));
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
                delete static_cast<int64_t*>(rawVal);
            } else if (dataId == BackendDataType::ROW_BK) {
                void* rawVal = info.valueSerializer->deserialize(valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData*>*>(stateTablePtr);
                table->put(
                    *static_cast<K*>(rawKey),
                    keyGroupId,
                    *static_cast<VoidNamespace*>(rawNs),
                    static_cast<RowData*>(rawVal)->copy());
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
            } else if (dataId == BackendDataType::SET_LONG) {
                void* rawVal = info.valueSerializer->deserialize(valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<long>*>*>(stateTablePtr);
                table->put(
                    *static_cast<K*>(rawKey),
                    keyGroupId,
                    *static_cast<VoidNamespace*>(rawNs),
                    static_cast<std::vector<long>*>(rawVal));
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
            } else {
                ERROR_RELEASE("HeapRestoreOperation: unsupported VALUE restore type " << dataId);
                delete static_cast<K*>(rawKey);
                if (nsBackendId == BackendDataType::VOID_NAMESPACE_BK) {
                    delete static_cast<VoidNamespace*>(rawNs);
                }
            }

        } else if (desc->getType() == StateDescriptor::Type::LIST) {
            auto dataId = desc->getBackendId();

            auto* listSer = dynamic_cast<ListSerializer*>(info.valueSerializer);
            TypeSerializer* elemSer = listSer ? listSer->getElementSerializer() : info.valueSerializer;

            if (elemSer == nullptr) {
                ERROR_RELEASE("HeapRestoreOperation: LIST state has null element serializer, skipping");
                delete static_cast<K*>(rawKey);
                if (nsBackendId == BackendDataType::VOID_NAMESPACE_BK) {
                    delete static_cast<VoidNamespace*>(rawNs);
                }
                return;
            }

            if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
                auto* vecVal = deserializeVector<int64_t>(elemSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<int64_t>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<int64_t*>(rawNs), vecVal);
                delete static_cast<K*>(rawKey);
                delete static_cast<int64_t*>(rawNs);
            } else if (dataId == BackendDataType::BIGINT_BK) {
                auto* vecVal = deserializeVector<int64_t>(elemSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<int64_t>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), vecVal);
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
            } else {
                ERROR_RELEASE("HeapRestoreOperation: unsupported LIST restore type " << dataId);
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
            }

        } else if (desc->getType() == StateDescriptor::Type::MAP) {
            auto mapKeyId = desc->getKeyDataId();
            auto mapValId = desc->getValueDataId();

            auto* mapSer = dynamic_cast<MapSerializer*>(info.valueSerializer);
            if (!mapSer) {
                ERROR_RELEASE("HeapRestoreOperation: MAP state serializer is not MapSerializer, skipping");
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
                return;
            }
            auto* mapKeySer = mapSer->getKeySerializer();
            auto* mapValSer = mapSer->getValueSerializer();

            // CP MAP format: [int count][key1][null_bool][val1]... (packed format)
            // Deserialize the entire map as one entry
            if (mapKeyId == BackendDataType::INT_BK && mapValId == BackendDataType::INT_BK) {
                auto* map = deserializeEmhashMap<int, int>(mapKeySer, mapValSer, valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int, int>*>*>(
                    stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (
                (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::BIGINT_BK) ||
                (mapKeyId == BackendDataType::EXTERNAL_BIGINT_BK && mapValId == BackendDataType::EXTERNAL_BIGINT_BK)) {
                auto* map = deserializeEmhashMap<int64_t, int64_t>(mapKeySer, mapValSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int64_t, int64_t>*>*>(
                        stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::INT_BK) {
                auto* map = deserializeEmhashMap<std::string, int>(mapKeySer, mapValSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<std::string, int>*>*>(
                        stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::VARCHAR_BK) {
                // JSON_OBJECTAGG: MapView<VoidNamespace, std::string, std::string>.
                auto* map = deserializeEmhashMap<std::string, std::string>(mapKeySer, mapValSer, valInput);
                auto* table = reinterpret_cast<
                    CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<std::string, std::string>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::VARCHAR_BK) {
                // JSON_ARRAYAGG: MapView<VoidNamespace, long, std::string>.
                auto* map = deserializeEmhashMap<int64_t, std::string>(mapKeySer, mapValSer, valInput);
                auto* table = reinterpret_cast<
                    CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<int64_t, std::string>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (
                (mapKeyId == BackendDataType::OBJECT_BK || mapKeyId == BackendDataType::POJO_BK) &&
                (mapValId == BackendDataType::OBJECT_BK || mapValId == BackendDataType::POJO_BK)) {
                auto* map = deserializeEmhashMap<Object*, Object*>(mapKeySer, mapValSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<Object*, Object*>*>*>(
                        stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::INT_BK) {
                auto* map = deserializeEmhashMap<RowData*, int32_t>(mapKeySer, mapValSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData*, int32_t>*>*>(
                        stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_BK) {
                auto* map = deserializeEmhashMap<RowData*, RowData*>(mapKeySer, mapValSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData*, RowData*>*>*>(
                        stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT64) {
                auto* map =
                    deserializeEmhashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>(mapKeySer, mapValSer, valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<
                    K,
                    VoidNamespace,
                    emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int64_t>>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (
                mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT32_INT64) {
                auto* map = deserializeEmhashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(
                    mapKeySer, mapValSer, valInput);
                auto* table = reinterpret_cast<CopyOnWriteStateTable<
                    K,
                    VoidNamespace,
                    emhash7::HashMap<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::TIME_WINDOW_BK && mapValId == BackendDataType::TIME_WINDOW_BK) {
                auto* map = deserializeEmhashMap<TimeWindow, TimeWindow>(mapKeySer, mapValSer, valInput);
                auto* table = reinterpret_cast<
                    CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<TimeWindow, TimeWindow>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
            } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_LIST_BK) {
                auto* map = deserializeEmhashMap<RowData*, std::vector<RowData*>*>(mapKeySer, mapValSer, valInput);
                if (nsBackendId == BackendDataType::BIGINT_BK) {
                    // Window rank "window_rank": MapState<RowData, List<RowData>>
                    // partitioned by the window-end namespace.
                    auto* table = reinterpret_cast<CopyOnWriteStateTable<
                        K, int64_t, emhash7::HashMap<RowData*, std::vector<RowData*>*>*>*>(stateTablePtr);
                    table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<int64_t*>(rawNs), map);
                } else {
                    auto* table = reinterpret_cast<
                        CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<RowData*, std::vector<RowData*>*>*>*>(
                        stateTablePtr);
                    table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), map);
                }
            } else {
                ERROR_RELEASE(
                    "HeapRestoreOperation: unsupported MAP restore type key=" << mapKeyId << " value=" << mapValId);
            }
            delete static_cast<K*>(rawKey);
            if (nsBackendId == BackendDataType::BIGINT_BK && mapKeyId == BackendDataType::ROW_BK &&
                mapValId == BackendDataType::ROW_LIST_BK) {
                // The window-rank map state is the only BIGINT-namespace map; its
                // namespace was deserialized as int64_t, not VoidNamespace.
                delete static_cast<int64_t*>(rawNs);
            } else {
                delete static_cast<VoidNamespace*>(rawNs);
            }
        } else {
            ERROR_RELEASE("HeapRestoreOperation: unsupported state type for restore, skipping");
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        }
    }

    /**
     * Deserializes an emhash7::HashMap from checkpoint bytes using the MapSerializer's
     * sub-serializers. Format: [int size] [key + bool isNull + value per entry].
     * Mirrors HeapSingleStateIterator::serializeEmhashMap().
     *
     * For Object* types, uses GetBuffer()+deserialize(Object*,...) since PojoSerializer's
     * void* deserialize is NOT_IMPL.
     */
    template <typename UK, typename UV>
    static emhash7::HashMap<UK, UV>* deserializeEmhashMap(
        TypeSerializer* keySer, TypeSerializer* valSer, DataInputDeserializer& input)
    {
        int size = input.readInt();
        if (size < 0 || size > input.Available()) {
            ERROR_RELEASE("Exception: Invalid emhash map size " << size << ", available bytes " << input.Available());
            throw std::runtime_error("Invalid emhash map size");
        }
        auto map = std::make_unique<emhash7::HashMap<UK, UV>>();
        map->reserve(size);
        for (int i = 0; i < size; i++) {
            UK key;
            if constexpr (std::is_same_v<UK, Object*>) {
                // Object* path: PojoSerializer::deserialize(void*) is NOT_IMPL
                Object* buf = keySer->GetBuffer();
                keySer->deserialize(buf, input);
                key = copyRestoredPointerForState<UK>(buf);
                releaseRestoredObject(buf);
            } else if constexpr (std::is_pointer_v<UK>) {
                UK rawKey = static_cast<UK>(keySer->deserialize(input));
                key = copyRestoredPointerForState<UK>(rawKey);
            } else {
                void* rawK = keySer->deserialize(input);
                key = *static_cast<UK*>(rawK);
                delete static_cast<UK*>(rawK);
            }
            bool isNull = input.readBoolean();
            UV val{};
            if constexpr (std::is_pointer_v<UV>) {
                if (isNull) {
                    val = nullptr;
                } else {
                    if constexpr (std::is_same_v<UV, Object*>) {
                        Object* buf = valSer->GetBuffer();
                        valSer->deserialize(buf, input);
                        val = copyRestoredPointerForState<UV>(buf);
                        releaseRestoredObject(buf);
                    } else {
                        UV rawVal = static_cast<UV>(valSer->deserialize(input));
                        val = copyRestoredPointerForState<UV>(rawVal);
                    }
                }
            } else {
                if (isNull) {
                    ERROR_RELEASE("HeapRestoreOperation: unexpected null MAP value for non-pointer type");
                    throw std::runtime_error("unexpected null MAP value for non-pointer type");
                }
                void* rawV = valSer->deserialize(input);
                val = *static_cast<UV*>(rawV);
                delete static_cast<UV*>(rawV);
            }
            (*map)[key] = val;
        }
        return map.release();
    }
};
