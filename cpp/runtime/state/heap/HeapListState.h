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

#include <vector>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/ListState.h"
#include "runtime/state/VoidNamespace.h"
#include "StateTable.h"
#include "CopyOnWriteStateTable.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include "runtime/state/internal/InternalListState.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "core/typeutils/LongSerializer.h"

// The state is a list. In the InternalKvState, the state is stored as a pointer to a std::vector
template <typename K, typename N, typename UV>
class HeapListState : public InternalListState<K, N, UV> {
public:
    HeapListState(
        StateTable<K, N, std::vector<UV>*>* stateTable,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer);

    ~HeapListState();

    TypeSerializer* getNamespaceSerializer() const
    {
        return namespaceSerializer;
    };

    TypeSerializer* getValueSerializer() const
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

    void add(const UV& value) override;

    void addAll(const std::vector<UV>& values) override;

    void update(const std::vector<UV>& values) override;

    std::vector<UV>* get() override;

    void merge(const std::vector<UV>& other) override;

    void setCurrentNamespace(N nameSpace) override;

    void clear() override;

    uint32_t getNextSequenceNumber(int32_t keyGroup) override;
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override;
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override;
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override;
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override;
    void clearVectorBatches(int64_t currentTimestamp) override;
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override;

    static HeapListState<K, N, UV>* create(
        StateDescriptor* stateDesc,
        StateTable<K, N, std::vector<UV>*>* stateTable,
        TypeSerializer* keySerializer,
        StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable);

    static HeapListState<K, N, UV>* update(
        StateDescriptor* stateDesc,
        StateTable<K, N, std::vector<UV>*>* stateTable,
        HeapListState<K, N, UV>* existingState,
        StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable);

private:
    StateTable<K, N, std::vector<UV>*>* stateTable;
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable = nullptr;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    N currentNamespace;
};

template <typename K, typename N, typename UV>
HeapListState<K, N, UV>::HeapListState(
    StateTable<K, N, std::vector<UV>*>* stateTable,
    TypeSerializer* valueSerializer,
    TypeSerializer* namespaceSerializer)
{
    this->valueSerializer = valueSerializer;
    this->namespaceSerializer = namespaceSerializer;
    this->stateTable = stateTable;
}

template <typename K, typename N, typename UV>
HeapListState<K, N, UV>::~HeapListState()
{
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::clear()
{
    std::vector<UV>* userList = stateTable->get(currentNamespace);
    if (userList != nullptr) {
        stateTable->liveNumElements_ -= static_cast<int64_t>(userList->size());
    }
    stateTable->remove(currentNamespace);
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::clearVectorBatches(int64_t currentTimestamp)
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

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
{
    VoidNamespace nameSpace;
    for (auto sequenceNumber : sequenceNumbersToDelete) {
        State::releaseVbStatistic(vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace));
        vectorBatchStateTable->remove(sequenceNumber, keyGroup, nameSpace);
    }
}

template <typename K, typename N, typename UV>
uint32_t HeapListState<K, N, UV>::getNextSequenceNumber(int32_t keyGroup)
{
    return vectorBatchStateTable->getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
{
    VoidNamespace nameSpace;
    auto sequenceNumber = vectorBatchStateTable->getNextSequenceNumber(keyGroup);
    vectorBatchStateTable->put(sequenceNumber, keyGroup, nameSpace, vectorBatch);
    vectorBatchStateTable->addNextSequenceNumber(keyGroup);
    State::recordVbStatistic(vectorBatch);
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::addVectorBatches(
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

template <typename K, typename N, typename UV>
omnistream::VectorBatch* HeapListState<K, N, UV>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    VoidNamespace nameSpace;
    return vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace);
}

template <typename K, typename N, typename UV>
std::vector<omnistream::VectorBatch*> HeapListState<K, N, UV>::getVectorBatches(int32_t keyGroup)
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

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::add(const UV& value)
{
    std::vector<UV>* userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>();
        stateTable->put(currentNamespace, userList);
    }
    userList->push_back(value);
    stateTable->liveNumElements_ += 1;          // CORRECTION 11: one element added
    stateTable->refreshSampledWidthsIfNeeded(); // CORRECTION 13: task-thread width sampling
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::addAll(const std::vector<UV>& values)
{
    std::vector<UV>* userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(values);
        stateTable->put(currentNamespace, userList);
    } else {
        userList->insert(userList->end(), values.begin(), values.end());
    }
    stateTable->liveNumElements_ += static_cast<int64_t>(values.size()); // CORRECTION 11
    stateTable->refreshSampledWidthsIfNeeded();                          // CORRECTION 13: task-thread width sampling
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::update(const std::vector<UV>& values)
{
    std::vector<UV>* userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(values);
        stateTable->put(currentNamespace, userList);
        stateTable->liveNumElements_ += static_cast<int64_t>(values.size()); // CORRECTION 11
    } else {
        // CORRECTION 11: full replace -> adjust by the size delta before overwriting.
        stateTable->liveNumElements_ += static_cast<int64_t>(values.size()) - static_cast<int64_t>(userList->size());
        *userList = values;
    }
    stateTable->refreshSampledWidthsIfNeeded(); // CORRECTION 13: task-thread width sampling
}

template <typename K, typename N, typename UV>
std::vector<UV>* HeapListState<K, N, UV>::get()
{
    return stateTable->get(currentNamespace);
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::merge(const std::vector<UV>& other)
{
    std::vector<UV>* userList = stateTable->get(currentNamespace);
    if (userList == nullptr) {
        userList = new std::vector<UV>(other);
        stateTable->put(currentNamespace, userList);
    } else {
        userList->insert(userList->end(), other.begin(), other.end());
    }
    stateTable->liveNumElements_ += static_cast<int64_t>(other.size()); // CORRECTION 11
    stateTable->refreshSampledWidthsIfNeeded();                         // CORRECTION 13: task-thread width sampling
}

template <typename K, typename N, typename UV>
void HeapListState<K, N, UV>::setCurrentNamespace(N nameSpace)
{
    currentNamespace = nameSpace;
}

template <typename K, typename N, typename UV>
HeapListState<K, N, UV>* HeapListState<K, N, UV>::create(
    StateDescriptor* stateDesc,
    StateTable<K, N, std::vector<UV>*>* stateTable,
    TypeSerializer* keySerializer,
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable)
{
    auto* createdState =
        new HeapListState<K, N, UV>(stateTable, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
    createdState->vectorBatchStateTable = vectorBatchStateTable;
    return createdState;
}

template <typename K, typename N, typename UV>
HeapListState<K, N, UV>* HeapListState<K, N, UV>::update(
    StateDescriptor* stateDesc,
    StateTable<K, N, std::vector<UV>*>* stateTable,
    HeapListState<K, N, UV>* existingState,
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    existingState->vectorBatchStateTable = vectorBatchStateTable;
    return existingState;
}
