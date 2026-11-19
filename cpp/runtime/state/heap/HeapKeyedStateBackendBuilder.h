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
#include <set>
#include <memory>
#include <stdexcept>
#include <utility>
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/restore/FullSnapshotRestoreOperation.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptorFactory.h"
#include "runtime/state/heap/HeapCompatibleFullRestoreOperation.h"
#include "runtime/state/heap/HeapRestoreOperation.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/api/common/state/RestoreStateDescriptor.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/utils/key_type_traits.h"
#include "CopyOnWriteStateTable.h"
#include "table/data/RowData.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "table/typeutils/SortedVectorLong.h"
#include "core/utils/MathUtils.h"
#include "runtime/state/StateRestoreValidation.h"

template <typename K>
class HeapKeyedStateBackendBuilder {
public:
    HeapKeyedStateBackendBuilder(TypeSerializer* keySerializer, int numberOfKeyGroups, KeyGroupRange* keyGroupRange)
        : keySerializer(keySerializer),
          numberOfKeyGroups(numberOfKeyGroups),
          keyGroupRange(keyGroupRange) {};

    HeapKeyedStateBackendBuilder& setOmniTaskBridge(std::shared_ptr<omnistream::OmniTaskBridge> bridge)
    {
        omniTaskBridge = bridge;
        return *this;
    }

    HeapKeyedStateBackendBuilder& setStateHandles(const std::set<std::shared_ptr<KeyedStateHandle>>& handles)
    {
        restoreStateHandles = handles;
        return *this;
    }

    HeapKeyedStateBackendBuilder& setFlinkSavepointAdaptorInfo(const FlinkSavepointAdaptorInfo& info)
    {
        adaptorInfo_ = info;
        return *this;
    }

    HeapKeyedStateBackendBuilder& setRestoreSavepointMode(RestoreSavepointMode mode)
    {
        restoreMode_ = mode;
        return *this;
    }

    HeapKeyedStateBackendBuilder& setOperatorDescription(const nlohmann::json& operatorDescription)
    {
        operatorDescription_ = operatorDescription;
        return *this;
    }

    HeapKeyedStateBackendBuilder& setTaskType(int taskType)
    {
        this->taskType_ = taskType;
        return *this;
    }

    bool validateRestoreStateHandles()
    {
        std::vector<std::shared_ptr<KeyedStateHandle>> handleVec(
            restoreStateHandles.begin(), restoreStateHandles.end());
        return omnistream::validateRestoreStateHandles(handleVec, omniTaskBridge);
    }

    HeapKeyedStateBackend<K>* build();

protected:
    TypeSerializer* keySerializer;
    int numberOfKeyGroups;
    KeyGroupRange* keyGroupRange;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge;
    std::set<std::shared_ptr<KeyedStateHandle>> restoreStateHandles;
    FlinkSavepointAdaptorInfo adaptorInfo_;
    RestoreSavepointMode restoreMode_ = RestoreSavepointMode::OMNI_INTERNAL;
    int taskType_ = 1;
    nlohmann::json operatorDescription_;

private:
    /** Info collected per state during restore Phase 1, used in Phase 2 for deserialization. */
    struct RestoreStateInfo {
        StateMetaInfoSnapshot::BackendStateType backendStateType;
        std::string stateName;
        StateDescriptor* stateDesc;
        TypeSerializer* namespaceSerializer;
        TypeSerializer* valueSerializer;
    };

    // Restored objects are stored in heap state, so they must not alias serializer reuse buffers.
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
     * Deserializes a single KV entry from checkpoint bytes and inserts it into the correct
     * typed CopyOnWriteStateTable. Mirrors the type dispatch in createFullSnapshotResources().
     */
    void restoreEntryToHeap(
        HeapKeyedStateBackend<K>* backend,
        const RestoreStateInfo& info,
        int keyGroupId,
        int keyGroupPrefixBytes,
        const std::vector<int8_t>& keyBytes,
        const std::vector<int8_t>& valueBytes);

    void restoreVbEntryToHeap(
        HeapKeyedStateBackend<K>* backend,
        const RestoreStateInfo& info,
        int keyGroupPrefixBytes,
        const std::vector<int8_t>& keyBytes,
        const std::vector<int8_t>& valueBytes);

    /**
     * Deserializes an emhash7::HashMap from checkpoint bytes using the MapSerializer's
     * sub-serializers. Format: [int size] [key + bool isNull + value per entry].
     * Mirrors HeapSingleStateIterator::serializeEmhashMap().
     * For Object* types, uses GetBuffer()+deserialize(Object*,...) since PojoSerializer's
     * void* deserialize is NOT_IMPL.
     */

    /**
     * Deserializes a std::vector from checkpoint bytes using the ListDelimitedSerializer format.
     * Format: [elem_1][','][elem_2][',']...[elem_N]  (no size prefix, comma-separated)
     * Mirrors HeapSingleStateIterator::serializeVector().
     */
    template <typename V>
    static std::vector<V>* deserializeVector(TypeSerializer* elemSer, DataInputDeserializer& input)
    {
        auto* vec = new std::vector<V>();
        while (input.Available() > 0) {
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
            if (input.Available() > 0) {
                input.readByte(); // consume ',' delimiter
            }
        }
        return vec;
    }

    /**
     * Adds a single map entry to an existing or new HashMap for per-entry MAP restore.
     * Called once per map entry (each map entry is a separate state entry in the new format).
     * The keyInput should be positioned after sourceKey+namespace, with mapKey bytes remaining.
     * The valInput contains [null_bool][mapValue].
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
            using BaseType = unwrap_shared_ptr_t<UK>;
            auto* keyBuffer = static_cast<BaseType*>(mapKeySer->deserialize(keyInput));
            if (keyBuffer == nullptr) {
                throw std::runtime_error("HeapKeyedStateBackendBuilder: deserialized null shared row map key");
            }
            if constexpr (std::is_same_v<BaseType, RowData>) {
                uk = std::shared_ptr<RowData>(static_cast<RowData*>(keyBuffer)->copy());
                if (!mapKeySer->isReusable()) {
                    delete keyBuffer;
                }
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
                ERROR_RELEASE("HeapKeyedStateBackendBuilder: unexpected null MAP value for non-pointer type");
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
};

template <typename K>
HeapKeyedStateBackend<K>* HeapKeyedStateBackendBuilder<K>::build()
{
    std::unique_ptr<omnistream::OperatorSavepointAdaptor> compatiblePreparedAdaptor;
    if (!restoreStateHandles.empty()) {
        if (restoreMode_ == RestoreSavepointMode::FLINK_COMPATIBLE) {
            if (adaptorInfo_.type == FlinkSavepointAdaptorType::None) {
                INFO_RELEASE("Error:Heap compatible restore is unsupported: " << adaptorInfo_.reason);
                throw std::runtime_error("Heap compatible restore is not supported: " + adaptorInfo_.reason);
            }
            if (adaptorInfo_.type != FlinkSavepointAdaptorType::OmniIsCompatible) {
                if (omniTaskBridge == nullptr) {
                    INFO_RELEASE(
                        "Error:Heap compatible restore missing OmniTaskBridge, adaptorType="
                        << static_cast<int>(adaptorInfo_.type) << ", reason=" << adaptorInfo_.reason
                        << ", stateHandleCount=" << restoreStateHandles.size());
                    throw std::invalid_argument(
                        "Heap compatible restore requires OmniTaskBridge when state handles exist");
                }
                //  flink cp/native sp
                auto firstHandle = *restoreStateHandles.begin();
                if (!std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(firstHandle)) {
                    INFO_RELEASE("Error:Heap compatible restore does not support incremental/native state handles");
                    throw std::runtime_error(
                        "Heap compatible restore does not support incremental/native state handles");
                }
                compatiblePreparedAdaptor =
                    omnistream::OperatorSavepointAdaptorFactory::createAdaptor(adaptorInfo_.type);
                if (compatiblePreparedAdaptor == nullptr) {
                    INFO_RELEASE(
                        "Error:Heap compatible restore adaptor factory returned null: " << adaptorInfo_.reason);
                    throw std::runtime_error("Heap compatible restore adaptor factory returned null");
                }
                compatiblePreparedAdaptor->prepareForRestore(operatorDescription_);
            }
        } else if (taskType_ == 1) {
            if (adaptorInfo_.type != FlinkSavepointAdaptorType::OmniIsCompatible && !validateRestoreStateHandles()) {
                INFO_RELEASE("Error:Heap restore does not support current state handles");
                throw std::runtime_error("Heap restore does not support current state handles");
            }
        }
    }

    auto keyContext = std::make_unique<InternalKeyContextImpl<K>>(keyGroupRange, numberOfKeyGroups);
    auto backend = std::make_unique<HeapKeyedStateBackend<K>>(keySerializer, keyContext.get());

    if (omniTaskBridge) {
        backend->setOmniTaskBridge(omniTaskBridge);
    }

    if (compatiblePreparedAdaptor != nullptr) {
        std::vector<std::shared_ptr<KeyedStateHandle>> handleVec(
            restoreStateHandles.begin(), restoreStateHandles.end());
        auto keySerializerPtr = std::shared_ptr<TypeSerializer>(keySerializer, [](TypeSerializer*) {});
        HeapCompatibleFullRestoreOperation<K> restoreOp(
            backend.get(),
            keyGroupRange,
            handleVec,
            keySerializerPtr,
            numberOfKeyGroups,
            omniTaskBridge,
            adaptorInfo_,
            std::move(compatiblePreparedAdaptor));
        restoreOp.restore();
        auto* builtBackend = backend.release();
        keyContext.release();
        return builtBackend;
    }

    // Restore from state handles if available
    if (!restoreStateHandles.empty() && omniTaskBridge) {
        // 根据handle类型分发到不同的恢复路径
        auto firstHandle = *restoreStateHandles.begin();
        if (!std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(firstHandle)) {
            // CP路径：使用HeapRestoreOperation
            std::vector<std::shared_ptr<KeyedStateHandle>> handleVec(
                restoreStateHandles.begin(), restoreStateHandles.end());
            auto keySerializerPtr = std::shared_ptr<TypeSerializer>(keySerializer, [](TypeSerializer*) {});

            HeapRestoreOperation<K> restoreOp(
                backend.get(), keyGroupRange, handleVec, keySerializerPtr, numberOfKeyGroups, omniTaskBridge);
            restoreOp.restore();

            auto* builtBackend = backend.release();
            keyContext.release();
            return builtBackend;
        }

        // SP路径：使用FullSnapshotRestoreOperation（现有逻辑
        int keyGroupPrefixBytes =
            CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);

        std::vector<std::shared_ptr<KeyedStateHandle>> handleVec(
            restoreStateHandles.begin(), restoreStateHandles.end());

        auto keySerializerPtr = std::shared_ptr<TypeSerializer>(keySerializer, [](TypeSerializer*) {});
        FullSnapshotRestoreOperation<K> restoreOp(keyGroupRange, handleVec, keySerializerPtr, omniTaskBridge);
        auto restoreIterator = restoreOp.restore();

        while (restoreIterator->hasNext()) {
            auto restoreResult = restoreIterator->next();
            auto& metaInfos = restoreResult->getStateMetaInfoSnapshots();
            auto keyGroupIterator = restoreResult->getKeyGroupIterator();

            // Phase 1: For each StateMetaInfoSnapshot, create state table via createOrUpdateInternalState()
            std::vector<RestoreStateInfo> stateInfos;
            stateInfos.reserve(metaInfos.size());

            for (size_t i = 0; i < metaInfos.size(); i++) {
                auto& metaInfo = metaInfos[i];
                auto backendStateType = metaInfo.getBackendStateType();

                if (backendStateType == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
                    INFO_RELEASE(
                        "HeapKeyedStateBackendBuilder: discovered PRIORITY_QUEUE state '"
                        << metaInfo.getName() << "' at kvStateId=" << i
                        << ", entries will be restored when the typed timer queue is created");
                    stateInfos.push_back(
                        {backendStateType, metaInfo.getName(), nullptr, nullptr, metaInfo.getValueSerializer()});
                    continue;
                }

                if (backendStateType != StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
                    INFO_RELEASE(
                        "HeapKeyedStateBackendBuilder: skipping unsupported backend state type for state '"
                        << metaInfo.getName() << "'");
                    stateInfos.push_back({backendStateType, metaInfo.getName(), nullptr, nullptr, nullptr});
                    THROW_LOGIC_EXCEPTION(
                        "Unsupported backend state type in heap keyed state restore. state='"
                        << metaInfo.getName() << "', kvStateId=" << i
                        << ", backendStateType=" << static_cast<int>(backendStateType));
                }

                std::string stateTypeStr =
                    metaInfo.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
                StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeStr);

                TypeSerializer* nsSerializer = metaInfo.getNamespaceSerializer();
                TypeSerializer* valSerializer = metaInfo.getValueSerializer();

                if (nsSerializer == nullptr || valSerializer == nullptr) {
                    INFO_RELEASE(
                        "Error HeapKeyedStateBackendBuilder::build skipping state '" << metaInfo.getName()
                                                                                     << "' — missing serializer(s)");
                    THROW_RUNTIME_ERROR(
                        "HeapKeyedStateBackendBuilder::build skipping state '" << metaInfo.getName()
                                                                               << "' — missing serializer(s)");
                }
                auto stateName = metaInfo.getName();
                if (stateName.size() >= 2 && stateName.compare(stateName.size() - 2, 2, "vb") == 0) {
                    StateDescriptor* desc =
                        backend->createRestoreDescriptor(metaInfo, stateType, nsSerializer, valSerializer);
                    stateInfos.push_back({backendStateType, stateName, desc, nsSerializer, valSerializer});
                    continue;
                }

                StateDescriptor* desc =
                    backend->createRestoreDescriptor(metaInfo, stateType, nsSerializer, valSerializer);

                // Create the state table via the existing type dispatch mechanism
                backend->createOrUpdateInternalState(nsSerializer, desc);

                stateInfos.push_back({backendStateType, metaInfo.getName(), desc, nsSerializer, valSerializer});
            }

            // Phase 2: Iterate KV entries, deserialize, and write into state tables
            while (keyGroupIterator->hasNext()) {
                auto keyGroup = keyGroupIterator->next();
                int keyGroupId = keyGroup->getKeyGroupId();
                auto entryIter = keyGroup->getKeyGroupEntries();

                while (entryIter->hasNext()) {
                    auto entry = entryIter->next();
                    int kvStateId = entry.getKvStateId();

                    if (kvStateId < 0 || kvStateId >= static_cast<int>(stateInfos.size())) {
                        INFO_RELEASE(
                            "Error:HeapKeyedStateBackendBuilder: invalid kvStateId " << kvStateId
                                                                                     << ", skipping entry");
                        continue;
                    }

                    auto& info = stateInfos[kvStateId];
                    if (info.backendStateType == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE) {
                        backend->addRestoredPriorityQueueEntry(info.stateName, entry.getKey(), keyGroupPrefixBytes);
                        continue;
                    }

                    if (info.stateDesc == nullptr) {
                        continue; // State was skipped in Phase 1
                    }
                    if (info.stateName.size() >= 2 && info.stateName.substr(info.stateName.size() - 2) == "vb") {
                        restoreVbEntryToHeap(
                            backend.get(), info, keyGroupPrefixBytes, entry.getKey(), entry.getValue());
                    } else {
                        restoreEntryToHeap(
                            backend.get(), info, keyGroupId, keyGroupPrefixBytes, entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    auto* builtBackend = backend.release();
    keyContext.release();
    return builtBackend;
}

template <typename K>
void HeapKeyedStateBackendBuilder<K>::restoreEntryToHeap(
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

    // Deserialize key.
    // 关键分支：当 backend 的 K 是 Object*（DataStream 标准场景，例如 reduce 的 String key），
    // 各类 serializer 的 void* deserialize 返回的并不是 Object**，而是具体子类指针（StringSerializer
    // 返回 std::u32string*）。直接 *static_cast<K*>(rawKey) 会按 Object** 解 std::u32string 头 8 字节
    // → 野指针 → CopyOnWriteStateMap::put 里 SIGSEGV。
    // 这里改成走 Object* 路径：GetBuffer + deserialize(Object*, ...)，再用一个 heap-allocated
    // Object** 包一层，让下游 *static_cast<K*>(rawKey) 与 delete 的写法保持统一。
    void* rawKey = nullptr;
    Object* keyObjForObjectKBackend = nullptr;
    if constexpr (std::is_same_v<K, Object*>) {
        keyObjForObjectKBackend = keySerializer->GetBuffer();
        keySerializer->deserialize(keyObjForObjectKBackend, keyInput);
        rawKey = new Object*(keyObjForObjectKBackend); // K = Object*; new Object*(value)
    } else if constexpr (std::is_pointer_v<K>) {
        // 非 Object* 的指针 key（如 RowData*）：serializer 的 void* deserialize 返回的
        // 直接是对象指针本身（K），而非 K*。这里包一层 K*，使下游统一的
        // *static_cast<K*>(rawKey) 取值与 delete static_cast<K*>(rawKey) 释放都成立。
        // 对象本体由 serializer 的复用缓冲持有、put() 内部会 copy() 克隆，故不能在此 delete。
        rawKey = new K(static_cast<K>(keySerializer->deserialize(keyInput)));
    } else if constexpr (KeyTypeTraits<K>::isSharedRowKey) {
        using KeyBaseType = unwrap_shared_ptr_t<K>;
        auto* keyBuffer = static_cast<KeyBaseType*>(keySerializer->deserialize(keyInput));
        if (keyBuffer == nullptr) {
            THROW_LOGIC_EXCEPTION("Heap keyed state restore deserialized a null shared row key");
        }
        // BinaryRowDataSerializer reuses its deserialize buffer. Keep an owned copy
        // before restoring the next entry.
        rawKey = new K(std::shared_ptr<KeyBaseType>(static_cast<KeyBaseType*>(keyBuffer->copy())));
    } else if constexpr (is_shared_ptr_v<K>) {
        NOT_IMPL_EXCEPTION;
    } else {
        rawKey = keySerializer->deserialize(keyInput);
    }
    // Object* key 的引用 RAII 守卫：state map 的 put() 内部对 Object* key 会 clone() 一份并由
    // map 自己持有引用；我们 GetBuffer 拿来的这份用完应当 putRefCount 释放，否则每条 entry 都会
    // 泄漏一次引用（reuseBuffer 路径）或泄漏一个 Object（newInstance 路径）。
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
        INFO_RELEASE(
            "Error:HeapKeyedStateBackendBuilder: state table not found for '" << desc->getName()
                                                                              << "', skipping entry");
        delete static_cast<K*>(rawKey);
        delete static_cast<VoidNamespace*>(rawNs);
        return;
    }

    auto nsBackendId = info.namespaceSerializer->getBackendId();

    if (desc->getType() == StateDescriptor::Type::VALUE) {
        auto dataId = desc->getBackendId();

        if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
            // valueSerializer 返回的是共享复用缓冲，状态表对 RowData* 值不克隆而是直接持有指针，
            // 故必须在存入前 copy() 一份独立副本，否则所有 entry 会别名到同一块缓冲。
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
            // 同上：必须 copy() 一份，避免别名到 valueSerializer 的共享复用缓冲。
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
            // PojoSerializer/Tuple2Serializer::deserialize(void*) is NOT_IMPL, use Object* path
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
                *static_cast<K*>(rawKey), keyGroupId, *static_cast<VoidNamespace*>(rawNs), *static_cast<int*>(rawVal));
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
            // 同上：必须 copy() 一份，避免别名到 valueSerializer 的共享复用缓冲。
            void* rawVal = info.valueSerializer->deserialize(valInput);
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, RowData*>*>(stateTablePtr);
            table->put(
                *static_cast<K*>(rawKey),
                keyGroupId,
                *static_cast<VoidNamespace*>(rawNs),
                static_cast<RowData*>(rawVal)->copy());
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        } else if (dataId == BackendDataType::ROW_LIST_BK) {
            void* rawVal = info.valueSerializer->deserialize(valInput);
            auto* table =
                reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<RowData*>*>*>(stateTablePtr);
            table->put(
                *static_cast<K*>(rawKey),
                keyGroupId,
                *static_cast<VoidNamespace*>(rawNs),
                static_cast<std::vector<RowData*>*>(rawVal));
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        } else if (dataId == BackendDataType::SET_LONG) {
            void* rawVal = info.valueSerializer->deserialize(valInput);
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, VoidNamespace, std::vector<long>*>*>(stateTablePtr);
            table->put(
                *static_cast<K*>(rawKey),
                keyGroupId,
                *static_cast<VoidNamespace*>(rawNs),
                static_cast<std::vector<long>*>(rawVal));
            delete static_cast<K*>(rawKey);
            delete static_cast<VoidNamespace*>(rawNs);
        } else {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported VALUE restore type " << dataId);
            delete static_cast<K*>(rawKey);
            if (nsBackendId == BackendDataType::VOID_NAMESPACE_BK) {
                delete static_cast<VoidNamespace*>(rawNs);
            }
        }

    } else if (desc->getType() == StateDescriptor::Type::LIST) {
        auto dataId = desc->getBackendId();

        // ListSerializer::deserialize(void*) is NOT_IMPL, use element serializer directly.
        // If valueSerializer is ListSerializer, extract the element serializer;
        // otherwise, use it directly as the element serializer (e.g. WindowJoin stores LongSerializer).
        auto* listSer = dynamic_cast<ListSerializer*>(info.valueSerializer);
        TypeSerializer* elemSer = listSer ? listSer->getElementSerializer() : info.valueSerializer;

        if (elemSer == nullptr) {
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: LIST state has null element serializer, skipping");
            delete static_cast<K*>(rawKey);
            if (nsBackendId == BackendDataType::VOID_NAMESPACE_BK) {
                delete static_cast<VoidNamespace*>(rawNs);
            }
            return;
        }

        if (nsBackendId == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
            auto* vecVal = deserializeVector<int64_t>(elemSer, valInput);
            auto* table = reinterpret_cast<CopyOnWriteStateTable<K, int64_t, std::vector<int64_t>*>*>(stateTablePtr);
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
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported LIST restore type " << dataId);
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
            INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: MAP state serializer is not MapSerializer, skipping");
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
        } else if (mapKeyId == BackendDataType::VARCHAR_BK && mapValId == BackendDataType::VARCHAR_BK) {
            // JSON_OBJECTAGG: MapView<VoidNamespace, std::string, std::string>.
            addMapEntryToStateTable<std::string, std::string>(
                backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
        } else if (mapKeyId == BackendDataType::BIGINT_BK && mapValId == BackendDataType::VARCHAR_BK) {
            // JSON_ARRAYAGG: MapView<VoidNamespace, long, std::string>.
            addMapEntryToStateTable<int64_t, std::string>(
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
        } else if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT64) {
            addMapEntryToStateTable<XXH128_hash_t, std::tuple<int32_t, int64_t>>(
                backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
        } else if (mapKeyId == BackendDataType::XXHASH128_BK && mapValId == BackendDataType::TUPLE_INT32_INT32_INT64) {
            addMapEntryToStateTable<XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(
                backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
        } else if (mapKeyId == BackendDataType::TIME_WINDOW_BK && mapValId == BackendDataType::TIME_WINDOW_BK) {
            addMapEntryToStateTable<TimeWindow, TimeWindow>(
                backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
        } else if (mapKeyId == BackendDataType::ROW_BK && mapValId == BackendDataType::ROW_LIST_BK) {
            addMapEntryToStateTable<RowData*, std::vector<RowData*>*>(
                backend, info, keyGroupId, rawKey, rawNs, mapKeySer, mapValSer, keyInput, valInput);
        } else {
            INFO_RELEASE(
                "Error:HeapKeyedStateBackendBuilder: unsupported MAP restore type key=" << mapKeyId
                                                                                        << " value=" << mapValId);
        }

        delete static_cast<K*>(rawKey);
        delete static_cast<VoidNamespace*>(rawNs);

    } else {
        INFO_RELEASE("Error:HeapKeyedStateBackendBuilder: unsupported state type for restore, skipping");
        delete static_cast<K*>(rawKey);
        delete static_cast<VoidNamespace*>(rawNs);
    }
}

template <typename K>
void HeapKeyedStateBackendBuilder<K>::restoreVbEntryToHeap(
    HeapKeyedStateBackend<K>* backend,
    const RestoreStateInfo& info,
    int keyGroupPrefixBytes,
    const std::vector<int8_t>& keyBytes,
    const std::vector<int8_t>& valueBytes)
{
    // vb key format: [keyGroup] + [8 bytes sequenceNumber via LongSerializer]
    const auto expectedKeySize = static_cast<size_t>(keyGroupPrefixBytes) + sizeof(int64_t);
    if (keyGroupPrefixBytes <= 0 || keyBytes.size() != expectedKeySize) {
        THROW_RUNTIME_ERROR(
            "Invalid VectorBatch key size while restoring to heap: expected " << expectedKeySize << ", actual "
                                                                              << keyBytes.size());
    }
    const auto* keyData = reinterpret_cast<const uint8_t*>(keyBytes.data());
    int vbKeyGroup = CompositeKeySerializationUtils::readKeyGroup(keyData, keyGroupPrefixBytes);
    DataInputDeserializer keyInput(keyData, static_cast<int>(keyBytes.size()), keyGroupPrefixBytes);

    LongSerializer longSerializer;
    void* rawSequenceNumber = longSerializer.deserialize(keyInput);
    int64_t sequenceNumber = *static_cast<int64_t*>(rawSequenceNumber);
    delete static_cast<int64_t*>(rawSequenceNumber);
    if (sequenceNumber < 0 || sequenceNumber > static_cast<int64_t>(SequenceNumberHelper::MAX_SEQUENCE_NUMBER)) {
        THROW_RUNTIME_ERROR("Invalid VectorBatch sequenceNumber while restoring: " << sequenceNumber);
    }
    auto sequenceNumberU32 = static_cast<uint32_t>(sequenceNumber);
    auto nextSequenceNumber = sequenceNumberU32 + 1;

    // Deserialize VectorBatch from value bytes
    std::vector<uint8_t> valueBuf(valueBytes.size());
    for (size_t i = 0; i < valueBytes.size(); i++) {
        valueBuf[i] = static_cast<uint8_t>(valueBytes[i]);
    }
    if (valueBuf.size() <= sizeof(int8_t) || valueBuf[0] != static_cast<uint8_t>(StreamElementTag::VECTOR_BATCH)) {
        ERROR_RELEASE(
            "Invalid VectorBatch state value while restoring state '" << info.stateName << "': size=" << valueBuf.size()
                                                                      << ", missing VectorBatch tag");
        THROW_RUNTIME_ERROR(
            "Invalid VectorBatch state value while restoring state '" << info.stateName << "': size=" << valueBuf.size()
                                                                      << ", missing VectorBatch tag");
    }
    uint8_t* cursor = valueBuf.data() + sizeof(int8_t);
    omnistream::VectorBatch* vectorBatch = nullptr;
    try {
        vectorBatch = VectorBatchDeserializationUtils::deserializeVectorBatch(cursor, valueBuf.size() - sizeof(int8_t));
    } catch (const std::exception& error) {
        ERROR_RELEASE(
            "Heap VectorBatch restore failed: state='"
            << info.stateName << "', keyGroup=" << vbKeyGroup << ", sequenceNumber=" << sequenceNumberU32
            << ", serializedSize=" << valueBuf.size() << ", reason=" << error.what());
        throw;
    }

    uintptr_t vbStateTablePtr = backend->getStateTablePtr(info.stateName);
    if (vbStateTablePtr == 0) {
        INFO_RELEASE(
            "Error:HeapKeyedStateBackendBuilder: vb state table not found for '" << info.stateName
                                                                                 << "', skipping entry");
        delete vectorBatch;
        return;
    }

    auto* table =
        reinterpret_cast<CopyOnWriteStateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>*>(vbStateTablePtr);
    VoidNamespace ns;
    table->put(sequenceNumberU32, vbKeyGroup, ns, vectorBatch);
    table->updateNextSequenceNumber(vbKeyGroup, nextSequenceNumber);
}
