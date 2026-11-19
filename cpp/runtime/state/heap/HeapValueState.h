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
    bool isEmpty() override;
    bool getIfPresent(V& out) override;
    void clear() override;
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
    } else {
        stateTable->put(currentNamespace, value);
    }
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::clear()
{
    stateTable->remove(currentNamespace);
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
            vectorBatchStateTable->remove(sequenceNumber, keyGroup, nameSpace);
        }
    }
}

template <typename K, typename N, typename V>
void HeapValueState<K, N, V>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
{
    VoidNamespace nameSpace;
    for (auto sequenceNumber : sequenceNumbersToDelete) {
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

template <typename K, typename N, typename V>
bool HeapValueState<K, N, V>::isEmpty()
{
    return !stateTable->containsKey(currentNamespace);
}

template <typename K, typename N, typename V>
bool HeapValueState<K, N, V>::getIfPresent(V& out)
{
    if (!stateTable->containsKey(currentNamespace)) {
        return false;
    }
    // Raw read: StateTable::get returns numeric_limits<V>::max() only when the
    // entry is absent, so a present LONG_MAX is returned unchanged here.
    out = stateTable->get(currentNamespace);
    // Match value()'s refcount contract for Object* values.
    if constexpr (std::is_same_v<V, Object*>) {
        if (out != nullptr) {
            reinterpret_cast<Object*>(out)->getRefCount();
        }
    }
    return true;
}
