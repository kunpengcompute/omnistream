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

#include <deque>
#include <unordered_map>
#include <vector>

#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/VoidSerializer.h"
#include "core/api/common/state/RestoreStateDescriptor.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/OperatorRuntimeStateSchemaProvider.h"
#include "runtime/state/heap/HeapRestoreKVState.h"
#include "runtime/state/heap/HeapRestorePQState.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"

namespace omnistream {

// ============================================================================
// HeapRestoreBackendDelegate — compatible restore 的 heap writer 工厂
// ============================================================================

template <typename K>
class HeapRestoreBackendDelegate : public RestoreBackendDelegate {
public:
    // State registration info（全局注册一次，多个 writer 共享引用）
    struct RestoreStateInfo {
        StateMetaInfoSnapshot::BackendStateType backendStateType;
        std::string stateName;
        StateDescriptor::Type stateType = StateDescriptor::Type::VALUE;
        StateDescriptor* mainStateDesc = nullptr;
        TypeSerializer* namespaceSerializer = nullptr;
        TypeSerializer* valueSerializer = nullptr;
        TypeSerializer* mapKeySerializer = nullptr;
        TypeSerializer* mapValueSerializer = nullptr;
        int64_t mainEntryCount = 0;
        uintptr_t mainTablePtr = 0;
    };

    HeapRestoreBackendDelegate(
        HeapKeyedStateBackend<K>* backend,
        std::shared_ptr<TypeSerializer> keySerializer,
        int keyGroupPrefixBytes,
        const OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider = nullptr);

    // --- RestoreBackendDelegate 工厂接口 ---

    std::unique_ptr<RestoreKVState> createKVState(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo) override;

    std::unique_ptr<RestorePQState> createPQState(int kvStateId, const StateMetaInfoSnapshot& metaInfo) override;

    // --- 统计 ---
    const std::deque<RestoreStateInfo>& getStateInfos() const
    {
        return stateInfos_;
    }

    // --- 供 writer 使用的访问器 ---
    HeapKeyedStateBackend<K>* getBackend() const
    {
        return backend_;
    }
    int getKeyGroupPrefixBytes() const
    {
        return keyGroupPrefixBytes_;
    }
    const std::shared_ptr<TypeSerializer>& getKeySerializer() const
    {
        return keySerializer_;
    }

    static StateDescriptor* createMainTableDescriptor(const StateMetaInfoSnapshot& mainMetaInfo);

    static StateDescriptor* createMainTableDescriptor(
        const StateMetaInfoSnapshot& mainMetaInfo, const OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider);

private:
    RestoreStateInfo& ensureStateRegistered(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo);

    HeapKeyedStateBackend<K>* backend_;
    std::shared_ptr<TypeSerializer> keySerializer_;
    int keyGroupPrefixBytes_;
    const OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider_;
    std::deque<RestoreStateInfo> stateInfos_;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
HeapRestoreBackendDelegate<K>::HeapRestoreBackendDelegate(
    HeapKeyedStateBackend<K>* backend,
    std::shared_ptr<TypeSerializer> keySerializer,
    int keyGroupPrefixBytes,
    const OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider)
    : backend_(backend),
      keySerializer_(std::move(keySerializer)),
      keyGroupPrefixBytes_(keyGroupPrefixBytes),
      runtimeSchemaProvider_(runtimeSchemaProvider)
{
}

template <typename K>
typename HeapRestoreBackendDelegate<K>::RestoreStateInfo& HeapRestoreBackendDelegate<K>::ensureStateRegistered(
    int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo)
{
    while (static_cast<int>(stateInfos_.size()) <= kvStateId) {
        stateInfos_.emplace_back();
    }

    auto& info = stateInfos_[kvStateId];
    if (info.mainStateDesc != nullptr) {
        return info;
    }

    TypeSerializer* nsSerializer = mainMetaInfo.getNamespaceSerializer();
    if (nsSerializer == nullptr) {
        nsSerializer = VoidSerializer::INSTANCE;
    }

    auto stateTypeString = mainMetaInfo.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
    StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeString);
    if (stateType == StateDescriptor::Type::UNKNOWN) {
        stateType = StateDescriptor::Type::VALUE;
    }

    info.backendStateType = mainMetaInfo.getBackendStateType();
    info.stateName = mainMetaInfo.getName();
    info.stateType = stateType;
    info.namespaceSerializer = nsSerializer;
    info.valueSerializer = mainMetaInfo.getValueSerializer();

    if (stateType == StateDescriptor::Type::MAP) {
        auto* mapSer = dynamic_cast<MapSerializer*>(info.valueSerializer);
        if (mapSer != nullptr) {
            info.mapKeySerializer = mapSer->getKeySerializer();
            info.mapValueSerializer = mapSer->getValueSerializer();
        }
    }

    info.mainStateDesc = createMainTableDescriptor(mainMetaInfo, runtimeSchemaProvider_);
    if (nsSerializer != nullptr) {
        backend_->createOrUpdateInternalState(nsSerializer, info.mainStateDesc);
    }
    return info;
}

template <typename K>
std::unique_ptr<RestoreKVState> HeapRestoreBackendDelegate<K>::createKVState(
    int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo)
{
    auto& info = ensureStateRegistered(kvStateId, mainMetaInfo);
    return std::make_unique<HeapRestoreKVState<K>>(*this, info);
}

// --- PQ State ---

template <typename K>
std::unique_ptr<RestorePQState> HeapRestoreBackendDelegate<K>::createPQState(
    int kvStateId, const StateMetaInfoSnapshot& metaInfo)
{
    return std::make_unique<HeapRestorePQState<K>>(backend_, metaInfo.getName(), keyGroupPrefixBytes_);
}

template <typename K>
StateDescriptor* HeapRestoreBackendDelegate<K>::createMainTableDescriptor(const StateMetaInfoSnapshot& mainMetaInfo)
{
    return createMainTableDescriptor(mainMetaInfo, nullptr);
}

template <typename K>
StateDescriptor* HeapRestoreBackendDelegate<K>::createMainTableDescriptor(
    const StateMetaInfoSnapshot& mainMetaInfo, const OperatorRuntimeStateSchemaProvider* runtimeSchemaProvider)
{
    auto stateTypeString = mainMetaInfo.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
    StateDescriptor::Type stateType = StateDescriptor::StringToType(stateTypeString);
    if (stateType == StateDescriptor::Type::UNKNOWN) {
        stateType = StateDescriptor::Type::VALUE;
    }

    TypeSerializer* valSerializer = mainMetaInfo.getValueSerializer();
    BackendDataType valueType = valSerializer ? valSerializer->getBackendId() : BackendDataType::BIGINT_BK;

    if (stateType == StateDescriptor::Type::MAP) {
        auto* mapSer = dynamic_cast<MapSerializer*>(valSerializer);
        if (mapSer) {
            BackendDataType keyBackendType = mapSer->getKeySerializer()->getBackendId();
            BackendDataType valueBackendType = mapSer->getValueSerializer()->getBackendId();
            if (runtimeSchemaProvider != nullptr) {
                auto runtimeSchema = runtimeSchemaProvider->resolveMapStateSchema(
                    mainMetaInfo.getName(), keyBackendType, valueBackendType);
                if (runtimeSchema.has_value()) {
                    keyBackendType = runtimeSchema->keyBackendType;
                    valueBackendType = runtimeSchema->valueBackendType;
                }
            }
            return new RestoreStateDescriptor(
                mainMetaInfo.getName(),
                stateType,
                valSerializer,
                BackendDataType::INVALID_BK,
                keyBackendType,
                valueBackendType);
        }
        return new RestoreStateDescriptor(
            mainMetaInfo.getName(),
            stateType,
            valSerializer,
            BackendDataType::INVALID_BK,
            BackendDataType::OBJECT_BK,
            BackendDataType::OBJECT_BK);
    } else if (stateType == StateDescriptor::Type::LIST) {
        auto* listSer = dynamic_cast<ListSerializer*>(valSerializer);
        BackendDataType elemId = listSer ? listSer->getElementSerializer()->getBackendId() : valueType;
        return new RestoreStateDescriptor(mainMetaInfo.getName(), stateType, valSerializer, elemId);
    } else {
        return new RestoreStateDescriptor(mainMetaInfo.getName(), stateType, valSerializer, valueType);
    }
}

} // namespace omnistream
