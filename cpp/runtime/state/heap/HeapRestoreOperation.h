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
     * Deserializes a std::vector from checkpoint bytes using the ListDelimitedSerializer format.
     * Format: [elem_1][','][elem_2][',']...[elem_N]  (no size prefix, comma-separated)
     * This is the unified savepoint value format Flink uses for ListState.
     * Mirrors HeapSingleStateIterator::serializeVector() and
     * HeapKeyedStateBackendBuilder::deserializeVector().
     */
    template <typename V>
    static std::vector<V>* deserializeVector(TypeSerializer* elemSer, DataInputDeserializer& input)
    {
        auto* vec = new std::vector<V>();
        auto readElement = [&]() {
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
        };
        while (input.Available() > 0) {
            readElement();
            if (input.Available() > 0) {
                input.readByte(); // consume ',' delimiter
            }
        }
        return vec;
    }

    /**
     * Deserializes a single map entry from per-entry checkpoint bytes and inserts it into
     * the typed CopyOnWriteStateTable. The map key is deserialized from keyInput (remaining
     * bytes after sourceKey + namespace), and the map value from valInput.
     *
     * Format (per-entry MAP):
     *   key   = [keyGroupPrefix][sourceKey][namespace][mapKey]
     *   value = [null_bool]([mapValue] if !null)
     *
     * Mirrors HeapKeyedStateBackendBuilder::addMapEntryToStateTable().
     */
    template <typename UK, typename UV>
    static void addMapEntryToStateTable(
        HeapKeyedStateBackend<K>* backend,
        const RestoreStateInfo& info,
        int keyGroupId,
        void* rawKey,
        void* rawNs,
        TypeSerializer* mapKeySer,
        TypeSerializer* mapValSer,
        DataInputDeserializer& keyInput,
        DataInputDeserializer& valInput)
    {
        auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, emhash7::HashMap<UK, UV>*>*>(
            backend->getStateTablePtr(info.stateDesc->getName()));
        if (table == nullptr) {
            return;
        }

        const bool isMapValNull = valInput.readBoolean();

        // Deserialize map key from the remaining bytes in keyInput (after sourceKey + namespace)
        UK uk;
        if constexpr (std::is_same_v<UK, Object*>) {
            Object* buf = mapKeySer->GetBuffer();
            mapKeySer->deserialize(buf, keyInput);
            uk = copyRestoredPointerForState<UK>(buf);
            releaseRestoredObject(buf);
        } else if constexpr (std::is_pointer_v<UK>) {
            UK raw = static_cast<UK>(mapKeySer->deserialize(keyInput));
            uk = copyRestoredPointerForState<UK>(raw);
        } else if constexpr (is_shared_ptr_v<UK>) {
            using KeyBaseType = unwrap_shared_ptr_t<UK>;
            auto* keyBuffer = static_cast<KeyBaseType*>(mapKeySer->deserialize(keyInput));
            if (keyBuffer == nullptr) {
                ERROR_RELEASE("HeapRestoreOperation: Deserialized null shared row map key");
                throw std::runtime_error("Deserialized null shared row map key");
            }
            if constexpr (std::is_same_v<KeyBaseType, RowData>) {
                uk = std::shared_ptr<RowData>(static_cast<RowData*>(keyBuffer)->copy());
            } else {
                NOT_IMPL_EXCEPTION;
            }
        } else {
            void* raw = mapKeySer->deserialize(keyInput);
            uk = *static_cast<UK*>(raw);
            delete static_cast<UK*>(raw);
        }

        // Deserialize map value
        UV uv{};
        if constexpr (std::is_pointer_v<UV>) {
            if (isMapValNull) {
                uv = nullptr;
            } else {
                if constexpr (std::is_same_v<UV, Object*>) {
                    Object* buf = mapValSer->GetBuffer();
                    mapValSer->deserialize(buf, valInput);
                    uv = copyRestoredPointerForState<UV>(buf);
                    releaseRestoredObject(buf);
                } else {
                    UV raw = static_cast<UV>(mapValSer->deserialize(valInput));
                    uv = copyRestoredPointerForState<UV>(raw);
                }
            }
        } else {
            if (isMapValNull) {
                ERROR_RELEASE("HeapRestoreOperation: unexpected null MAP value for non-pointer type");
                throw std::runtime_error("unexpected null MAP value for non-pointer type");
            }
            void* raw = mapValSer->deserialize(valInput);
            uv = *static_cast<UV*>(raw);
            delete static_cast<UV*>(raw);
        }

        // Add to existing or new HashMap
        auto* kvMap = table->get(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs));
        if (kvMap == nullptr) {
            kvMap = new emhash7::HashMap<UK, UV>();
            (*kvMap)[uk] = uv;
            table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), kvMap);
        } else {
            (*kvMap)[uk] = uv;
        }
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
            } else if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
                auto* vecVal = deserializeVector<RowData*>(elemSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<RowData*>*>*>(stateTablePtr);
                table->put(*static_cast<K*>(rawKey), keyGroupId, *static_cast<int64_t*>(rawNs), vecVal);
                delete static_cast<K*>(rawKey);
                delete static_cast<int64_t*>(rawNs);
            } else if (dataId == BackendDataType::ROW_BK) {
                auto* vecVal = deserializeVector<RowData*>(elemSer, valInput);
                auto* table =
                    reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<RowData*>*>*>(stateTablePtr);
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

            // MAP state per-entry format: each map entry is a separate state entry.
            // keyInput (after sourceKey+namespace) contains the serialized mapKey.
            // valInput contains [null_bool][mapValue].
            auto* mapSer = dynamic_cast<MapSerializer*>(info.valueSerializer);
            if (!mapSer) {
                ERROR_RELEASE("HeapRestoreOperation: MAP state serializer is not MapSerializer, skipping");
                delete static_cast<K*>(rawKey);
                delete static_cast<VoidNamespace*>(rawNs);
                return;
            }
            auto* mapKeySer = mapSer->getKeySerializer();
            auto* mapValSer = mapSer->getValueSerializer();

            if (mapKeyId == BackendDataType::INT_BK && mapValId == BackendDataType::INT_BK) {
                addMapEntryToStateTable<int, int>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (
                (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::BIGINT_BK) ||
                (mapKeyId == BackendDataType::EXTERNAL_BIGINT_BK && mapValId == BackendDataType::EXTERNAL_BIGINT_BK)) {
                addMapEntryToStateTable<int64_t, int64_t>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::INT_BK) {
                addMapEntryToStateTable<std::string, int>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (
                (mapKeyId == BackendDataType::OBJECT_BK || mapKeyId == BackendDataType::POJO_BK) &&
                (mapValId == BackendDataType::OBJECT_BK || mapValId == BackendDataType::POJO_BK)) {
                addMapEntryToStateTable<Object*, Object*>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::INT_BK) {
                addMapEntryToStateTable<RowData*, int32_t>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::SHARED_ROW_BK && mapValId == BackendDataType::INT_BK) {
                addMapEntryToStateTable<std::shared_ptr<RowData>, int32_t>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_BK) {
                addMapEntryToStateTable<RowData*, RowData*>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::SHARED_ROW_BK && mapValId == BackendDataType::TUPLE_INT32_INT32) {
                addMapEntryToStateTable<std::shared_ptr<RowData>, std::tuple<int32_t, int32_t>>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::TIME_WINDOW_BK && mapValId == BackendDataType::TIME_WINDOW_BK) {
                addMapEntryToStateTable<TimeWindow, TimeWindow>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_LIST_BK) {
                addMapEntryToStateTable<RowData*, std::vector<RowData*>*>(
                    backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
            } else {
                ERROR_RELEASE(
                    "HeapRestoreOperation: unsupported MAP restore type key=" << mapKeyId << " value=" << mapValId);
            }
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        } else {
            ERROR_RELEASE("HeapRestoreOperation: unsupported state type for restore, skipping");
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        }
    }
};
