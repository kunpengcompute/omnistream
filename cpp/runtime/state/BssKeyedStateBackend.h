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

#ifndef OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#define OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#ifdef WITH_OMNISTATESTORE

#include <stdint-gcc.h>
#include "AbstractKeyedStateBackend.h"
#include "state/bss/BssValueState.h"
#include "state/bss/BssStateTable.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "config.h"
#include "boost_state_db.h"
#include "bss_types.h"
#include "state/bss/BssListState.h"
#include "state/bss/BssMapState.h"
#include <random>
#include <cstdint>
#include <stdexcept>

class UUIDGenerator {
public:
    static uint32_t generateUUID()
    {
        // 每个线程有独立的随机数生成器
        thread_local std::random_device rd;                                      // 随机种子
        thread_local std::mt19937 gen(rd());                                     // 梅森旋转算法生成器
        thread_local std::uniform_int_distribution<uint32_t> dis(1, UINT32_MAX); // 1 到 uint32_t 最大值
        return dis(gen);
    }
};

template <typename K>
class BssKeyedStateBackend : public AbstractKeyedStateBackend<K> {
public:
    BssKeyedStateBackend(
        TypeSerializer* keySerializer, InternalKeyContext<K>* context, int startGroup, int endGroup, int maxParallelism)
        : AbstractKeyedStateBackend<K>(keySerializer, context),
          startGroup_(startGroup),
          endGroup_(endGroup),
          maxParallelism_(maxParallelism)
    {
    }
    omnistream::StateType getStateType() const noexcept override
    {
        return omnistream::StateType::BSS;
    }

    uintptr_t createOrUpdateInternalState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc) override;

    ~BssKeyedStateBackend() override = default;

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions)
    {
        return nullptr;
    }

    std::shared_ptr<SavepointResources> savepoint() override
    {
        return nullptr;
    }

private:
    int startGroup_;
    int endGroup_;
    int maxParallelism_;
    // pointer to StateTable<K, N, V>
    emhash7::HashMap<std::string, uintptr_t> registeredKvStates;
    // pointer to intervalKvState
    emhash7::HashMap<std::string, uintptr_t> createdKvState;

    uintptr_t GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    uintptr_t GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename S>
    BssStateTable<K, N, S>* tryRegisterStateTable(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename S>
    BssListStateTable<K, N, S>* tryRegisterListStateTable(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename UK, typename UV>
    BssMapStateTable<K, N, UK, UV>* tryRegisterMapStateTable(
        TypeSerializer* namespaceSerializer, MapStateDescriptor<UK, UV>* stateDesc);

    template <typename N, typename V>
    BssValueState<K, N, V>* createOrUpdateInternalValueState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);

    template <typename N, typename UK, typename UV>
    BssMapState<K, N, UK, UV>* createOrUpdateInternalMapState(
        TypeSerializer* namespaceSerializer, StateDescriptor* descriptor);

    template <typename N, typename V>
    BssListState<K, N, V>* createOrUpdateInternalListState(
        TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc);
};

template <typename K>
template <typename N, typename S>
BssListStateTable<K, N, S>* BssKeyedStateBackend<K>::tryRegisterListStateTable(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<BssListStateTable<K, N, S>*>(it->second);
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        auto newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssListStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetListState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto dataId = stateDesc->getBackendId();
    if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalListState<int64_t, int64_t>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
        dataId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, int64_t>(namespaceSerializer, stateDesc);

    } else if (namespaceSerializer->getBackendId() == BackendDataType::BIGINT_BK && dataId == BackendDataType::ROW_BK) {
        // This has not yet been tested with BackendDataType::ROW_BK
        return (uintptr_t)createOrUpdateInternalListState<int64_t, RowData*>(namespaceSerializer, stateDesc);
    } else if (
        namespaceSerializer->getBackendId() == BackendDataType::VOID_NAMESPACE_BK &&
        dataId == BackendDataType::ROW_BK) {
        // This has not yet been tested with BackendDataType::ROW_BK
        return (uintptr_t)createOrUpdateInternalListState<VoidNamespace, RowData*>(namespaceSerializer, stateDesc);
    } else {
        LOG("not support these backendId");
        THROW_LOGIC_EXCEPTION("not support these backendId");
    }
}

template <typename K>
template <typename N, typename UK, typename UV>
BssMapState<K, N, UK, UV>* BssKeyedStateBackend<K>::createOrUpdateInternalMapState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    BssMapStateTable<K, N, UK, UV>* stateTable = tryRegisterMapStateTable<N, UK, UV>(
        namespaceSerializer, reinterpret_cast<MapStateDescriptor<UK, UV>*>(stateDesc));
    auto it = createdKvState.find(stateDesc->getName());
    BssMapState<K, N, UK, UV>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssMapState<K, N, UK, UV>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssMapState<K, N, UK, UV>::update(
            stateDesc, stateTable, reinterpret_cast<BssMapState<K, N, UK, UV>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);

    auto _dbPtr = ock::bss::BoostStateDBFactory::Create();
    ock::bss::ConfigRef config = std::make_shared<ock::bss::Config>();
    config->Init(ock::bss::NO_0, ock::bss::NO_127, ock::bss::NO_128);
    config->mMemorySegmentSize = ock::bss::IO_SIZE_64M;
    config->SetEvictMinSize(ock::bss::IO_SIZE_1K);
    uint32_t uuid = UUIDGenerator::generateUUID();
    config->SetTaskSlotFlag(uuid);
    _dbPtr->Open(config);
    createdState->CreateTable(_dbPtr);
    return createdState;
}

template <typename K>
template <typename N, typename UK, typename UV>
BssMapStateTable<K, N, UK, UV>* BssKeyedStateBackend<K>::tryRegisterMapStateTable(
    TypeSerializer* namespaceSerializer, MapStateDescriptor<UK, UV>* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->GetValueSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<BssMapStateTable<K, N, UK, UV>*>(it->second); // 这里转成Rocksdb
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        auto newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssMapStateTable<K, N, UK, UV>(
            this->context, this->keySerializer, stateDesc->GetUserKeySerializer(), newMetaInfo);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetMapState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto keyId = stateDesc->getKeyDataId();
    auto valueId = stateDesc->getValueDataId();
    STD_LOG("stateType_ is StateDescriptor::Type::MAP " << ", keyId " << keyId_ << " , value id " << valueId_);

    if (namespaceSerializer->getBackendId() != BackendDataType::VOID_NAMESPACE_BK) {
        LOG("backendID: VOID_NAMESPACE_BK not support");
        NOT_IMPL_EXCEPTION;
    }
    if (keyId == BackendDataType::INT_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int32_t, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::BIGINT_BK && valueId == BackendDataType::BIGINT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, int64_t, int64_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::VARCHAR_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, std::string, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::INT_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, int32_t>(
            namespaceSerializer, stateDesc);
    } else if (
        keyId == BackendDataType::SHARED_ROW_BK &&
        (valueId == BackendDataType::INT_BK || valueId == BackendDataType::TUPLE_INT32_INT32)) {
        THROW_LOGIC_EXCEPTION(
            "BSS does not support StreamingJoin shared RowData MapState; use Heap or RocksDB backend");
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, RowData*>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT64) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int64_t>>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::XXHASH128_BK && valueId == BackendDataType::TUPLE_INT32_INT32_INT64) {
        return (uintptr_t)
            createOrUpdateInternalMapState<VoidNamespace, XXH128_hash_t, std::tuple<int32_t, int32_t, int64_t>>(
                namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::TIME_WINDOW_BK && valueId == BackendDataType::TIME_WINDOW_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, TimeWindow, TimeWindow>(
            namespaceSerializer, stateDesc);
    } else if (keyId == BackendDataType::ROW_BK && valueId == BackendDataType::ROW_LIST_BK) {
        return (uintptr_t)createOrUpdateInternalMapState<VoidNamespace, RowData*, std::vector<RowData*>*>(
            namespaceSerializer, stateDesc);
    }
    return 0;
}

template <typename K>
template <typename N, typename V>
BssListState<K, N, V>* BssKeyedStateBackend<K>::createOrUpdateInternalListState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    BssListStateTable<K, N, V>* stateTable = tryRegisterListStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    BssListState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssListState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState =
            BssListState<K, N, V>::update(stateDesc, stateTable, reinterpret_cast<BssListState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);

    auto _dbPtr = ock::bss::BoostStateDBFactory::Create();
    ock::bss::ConfigRef config = std::make_shared<ock::bss::Config>();
    config->Init(ock::bss::NO_0, ock::bss::NO_127, ock::bss::NO_128);
    config->mMemorySegmentSize = ock::bss::IO_SIZE_64M;
    config->SetEvictMinSize(ock::bss::IO_SIZE_1K);
    uint32_t uuid = UUIDGenerator::generateUUID();
    config->SetTaskSlotFlag(uuid);
    _dbPtr->Open(config);
    createdState->CreateTable(_dbPtr);
    return createdState;
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::createOrUpdateInternalState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    if (stateDesc->getType() == StateDescriptor::Type::MAP) {
        return this->GetMapState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::VALUE) {
        return this->GetValueState(namespaceSerializer, stateDesc);
    } else if (stateDesc->getType() == StateDescriptor::Type::LIST) {
        return this->GetListState(namespaceSerializer, stateDesc);
    } else {
        THROW_LOGIC_EXCEPTION("bss has not support this state yet");
    }
}

template <typename K>
uintptr_t BssKeyedStateBackend<K>::GetValueState(TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    // For Agg and JoinKeyContainsUniqueKeys
    auto dataId = stateDesc->getBackendId();
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
    } else {
        LOG("not support these backendId");
        THROW_LOGIC_EXCEPTION("not support these backendId");
    }
}

template <typename K>
template <typename N, typename V>
BssValueState<K, N, V>* BssKeyedStateBackend<K>::createOrUpdateInternalValueState(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    // For Value state, S is the same as V
    BssStateTable<K, N, V>* stateTable = tryRegisterStateTable<N, V>(namespaceSerializer, stateDesc);
    auto it = createdKvState.find(stateDesc->getName());
    BssValueState<K, N, V>* createdState;
    if (it == createdKvState.end()) {
        createdState = BssValueState<K, N, V>::create(stateDesc, stateTable, this->getKeySerializer());
    } else {
        createdState = BssValueState<K, N, V>::updateState(
            stateDesc, stateTable, reinterpret_cast<BssValueState<K, N, V>*>(it->second));
    }
    createdKvState[stateDesc->getName()] = reinterpret_cast<uintptr_t>(createdState);
    auto _dbPtr = ock::bss::BoostStateDBFactory::Create();
    ock::bss::ConfigRef config = std::make_shared<ock::bss::Config>();
    config->Init(ock::bss::NO_0, ock::bss::NO_127, ock::bss::NO_128);
    config->mMemorySegmentSize = ock::bss::IO_SIZE_64M;
    config->SetEvictMinSize(ock::bss::IO_SIZE_1K);
    uint32_t uuid = UUIDGenerator::generateUUID();
    config->SetTaskSlotFlag(uuid);
    _dbPtr->Open(config);
    createdState->CreateTable(_dbPtr);
    return createdState;
}

template <typename K>
template <typename N, typename S>
BssStateTable<K, N, S>* BssKeyedStateBackend<K>::tryRegisterStateTable(
    TypeSerializer* namespaceSerializer, StateDescriptor* stateDesc)
{
    auto it = registeredKvStates.find(stateDesc->getName());
    TypeSerializer* newStateSerializer = stateDesc->getStateSerializer();
    if (it != registeredKvStates.end()) {
        auto stateTable = reinterpret_cast<BssStateTable<K, N, S>*>(it->second);
        RegisteredKeyValueStateBackendMetaInfo* restoredKvMetaInfo = stateTable->getMetaInfo();
        restoredKvMetaInfo->setNamespaceSerializer(namespaceSerializer);
        restoredKvMetaInfo->setStateSerializer(newStateSerializer);
        stateTable->setMetaInfo(restoredKvMetaInfo);
        return stateTable;
    } else {
        auto newMetaInfo =
            new RegisteredKeyValueStateBackendMetaInfo(stateDesc->getName(), namespaceSerializer, newStateSerializer);
        auto stateTable = new BssStateTable<K, N, S>(this->context, newMetaInfo, this->keySerializer);
        registeredKvStates[stateDesc->getName()] = reinterpret_cast<uintptr_t>(stateTable);
        return stateTable;
    }
}

#endif // OMNISTREAM_BSSKEYEDSTATEBACKEND_H
#endif
