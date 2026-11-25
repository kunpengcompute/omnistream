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

#include "core/typeutils/TypeSerializer.h"
#include "StateTable.h"
#include "CopyOnWriteStateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "state/internal/InternalValueState.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "table/data/vectorbatch/VectorBatch.h"

template <typename K, typename N, typename V>
class HeapValueState : public InternalValueState<K, N, V> {
public:
    HeapValueState(
        StateTable<K, N, V>* stateTable,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer,
        V defaultValue);

    ~HeapValueState() override;

    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    };
    TypeSerializer* getNamespaceSerializer()
    {
        return namespaceSerializer;
    };
    TypeSerializer* getValueSerializer()
    {
        return valueSerializer;
    };
    void setNamespaceSerializer(TypeSerializer* serializer)
    {
        namespaceSerializer = serializer;
    };
    void setValueSerializer(TypeSerializer* serializer)
    {
        valueSerializer = serializer;
    };
    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };
    V value() override;
    void update(const V& value, bool copyKey = false) override;
    void clear() override
    {
        // removed container's elements from the running counter and free it before removing the key.
        if constexpr (StateSizeUtil::is_container_value<V>::value) {
            V old = stateTable->get(currentNamespace);
            if (old != nullptr) {
                stateTable->liveNumElements_ -= static_cast<int64_t>(old->size());
                delete old;
            }
        }
        stateTable->remove(currentNamespace);
    };
    void setDefaultValue(V value)
    {
        defaultValue = value;
    };

    void clearVectorBatches(int64_t currentTimestamp) override;
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override;
    uint32_t getNextSequenceNumber(int32_t keyGroup) override;
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override;
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override;
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override;
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override;

    static HeapValueState<K, N, V>* create(
        StateDescriptor* stateDesc,
        StateTable<K, N, V>* stateTable,
        TypeSerializer* keySerializer,
        StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable);
    static HeapValueState<K, N, V>* update(
        StateDescriptor* stateDesc,
        StateTable<K, N, V>* stateTable,
        HeapValueState<K, N, V>* existingState,
        StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable);

private:
    StateTable<K, N, V>* stateTable;
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable = nullptr;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

template <typename K, typename N, typename V>
HeapValueState<K, N, V>::HeapValueState(
    StateTable<K, N, V>* stateTable,
    TypeSerializer* keySerializer,
    TypeSerializer* valueSerializer,
    TypeSerializer* namespaceSerializer,
    V defaultValue)
    : stateTable(stateTable),
      keySerializer(keySerializer),
      valueSerializer(valueSerializer),
      namespaceSerializer(namespaceSerializer),
      defaultValue(defaultValue)
{
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V>::~HeapValueState()
{
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::update(const V& value, bool copyKey)
{
    if (copyKey) {
        stateTable->copyCurrentKey();
    }
    if constexpr (std::is_same_v<V, Object*>) {
        if (value != nullptr) {
            auto newValue = static_cast<Object*>(value);
            stateTable->put(currentNamespace, newValue);
        }
    } else if constexpr (StateSizeUtil::is_container_value<V>::value) {
        // container-valued VALUE state (SET_LONG, std::vector<long>*) is a full replace
        // through this API. Maintain liveNumElements_ with an exact before/after size diff (task thread
        // only) so the data-size metric needs no CopyOnWriteStateMap walk. update() owns freeing the
        // replaced container (callers no longer pre-delete the old value).
        V oldValue = stateTable->get(currentNamespace);
        int64_t before = (oldValue != nullptr) ? static_cast<int64_t>(oldValue->size()) : 0;
        stateTable->put(currentNamespace, value);
        int64_t after = (value != nullptr) ? static_cast<int64_t>(value->size()) : 0;
        stateTable->liveNumElements_ += after - before;
        if (oldValue != nullptr && oldValue != value) {
            delete oldValue;
        }
    } else {
        stateTable->put(currentNamespace, value);
    }
    // bytes from cached atomics without ever touching a live map entry. No-op once sampled.
    stateTable->refreshSampledWidthsIfNeeded();
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::clearVectorBatches(int64_t currentTimestamp)
{
    VoidNamespace nameSpace;
    auto* keyGroupRange = vectorBatchStateTable->getKeyContext()->getKeyGroupRange();
    for (int32_t keyGroup = keyGroupRange->getStartKeyGroup(); keyGroup <= keyGroupRange->getEndKeyGroup();
         ++keyGroup) {
        auto nextSequenceNumber = this->getNextSequenceNumber(keyGroup);
        for (uint32_t sequenceNumber = 0; sequenceNumber < nextSequenceNumber; ++sequenceNumber) {
            State::releaseVbStatistic(vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace));
            vectorBatchStateTable->remove(sequenceNumber, keyGroup, nameSpace);
        }
    }
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
{
    VoidNamespace nameSpace;
    for (auto sequenceNumber : sequenceNumbersToDelete) {
        State::releaseVbStatistic(vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace));
        vectorBatchStateTable->remove(sequenceNumber, keyGroup, nameSpace);
    }
}

template <typename K, typename N, typename V>
uint32_t HeapValueState<K, N, V>::getNextSequenceNumber(int32_t keyGroup)
{
    return vectorBatchStateTable->getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
{
    VoidNamespace nameSpace;
    auto sequenceNumber = vectorBatchStateTable->getNextSequenceNumber(keyGroup);
    vectorBatchStateTable->put(sequenceNumber, keyGroup, nameSpace, vectorBatch);
    vectorBatchStateTable->addNextSequenceNumber(keyGroup);
    State::recordVbStatistic(vectorBatch);
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::addVectorBatches(
    const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
{
    VoidNamespace nameSpace;
    for (const auto& [keyGroup, vectorBatch] : vectorBatchByKeyGroup) {
        auto nextSequenceNumber = vectorBatchStateTable->getNextSequenceNumber(keyGroup);
        vectorBatchStateTable->put(nextSequenceNumber, keyGroup, nameSpace, vectorBatch);
        vectorBatchStateTable->addNextSequenceNumber(keyGroup);
        State::recordVbStatistic(vectorBatch);
    }
}

template <typename K, typename N, typename V>
omnistream::VectorBatch* HeapValueState<K, N, V>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    VoidNamespace nameSpace;
    return vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace);
}

template <typename K, typename N, typename V>
std::vector<omnistream::VectorBatch*> HeapValueState<K, N, V>::getVectorBatches(int32_t keyGroup)
{
    VoidNamespace nameSpace;
    auto nextSequenceNumber = getNextSequenceNumber(keyGroup);
    std::vector<omnistream::VectorBatch*> vectorBatches;
    vectorBatches.reserve(nextSequenceNumber);
    for (uint32_t sequenceNumber = 0; sequenceNumber < nextSequenceNumber; ++sequenceNumber) {
        auto vectorBatch = vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace);
        vectorBatches.push_back(vectorBatch);
    }
    return vectorBatches;
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V>* HeapValueState<K, N, V>::create(
    StateDescriptor* stateDesc,
    StateTable<K, N, V>* stateTable,
    TypeSerializer* keySerializer,
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchSideTable)
{
    auto* createdState = new HeapValueState<K, N, V>(
        stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
    createdState->vectorBatchStateTable = vectorBatchSideTable;
    return createdState;
}

template <typename K, typename N, typename V>
HeapValueState<K, N, V>* HeapValueState<K, N, V>::update(
    StateDescriptor* stateDesc,
    StateTable<K, N, V>* stateTable,
    HeapValueState<K, N, V>* existingState,
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchSideTable)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    existingState->vectorBatchStateTable = vectorBatchSideTable;
    return existingState;
}

template <typename K, typename N, typename V>
V HeapValueState<K, N, V>::value()
{
    V result = stateTable->get(currentNamespace);
    if constexpr (std::is_same_v<V, Object*>) {
        if (result != nullptr) {
            reinterpret_cast<Object*>(result)->getRefCount();
        }
        return result;
    } else if constexpr (std::is_pointer<V>::value) {
        return result == nullptr ? defaultValue : result;
    } else {
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}
