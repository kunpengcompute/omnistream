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

#ifndef OMNISTREAM_BSSLISTSTATE_H
#define OMNISTREAM_BSSLISTSTATE_H
#ifdef WITH_OMNISTATESTORE

#include "BssListStateTable.h"
#include "state/internal/InternalListState.h"
#include "api/common/state/StateDescriptor.h"

template <typename K, typename N, typename UV>
class BssListState : public InternalListState<K, N, UV> {
public:
    BssListState(
        BssListStateTable<K, N, UV>* _stateTable,
        TypeSerializer* _valueSerializer,
        TypeSerializer* _namespaceSerializer)
        : stateTable(_stateTable),
          valueSerializer(_valueSerializer),
          namespaceSerializer(_namespaceSerializer) {};

    ~BssListState() = default;

    void merge(const std::vector<UV>& other) override {};

    void addAll(const std::vector<UV>& values) override
    {
        stateTable->addAll(currentNamespace, values);
    };

    void CreateTable(ock::bss::BoostStateDBPtr& _dbPtr)
    {
        stateTable->createTable(_dbPtr);
    };

    TypeSerializer* getNameSpaceSerializer() const
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

    void add(const UV& value) override
    {
        stateTable->add(currentNamespace, value);
    };

    void update(const std::vector<UV>& values) override {};

    static BssListState<K, N, UV>* update(
        StateDescriptor* stateDesc, BssListStateTable<K, N, UV>* stateTable, BssListState<K, N, UV>* existingState)
    {
        existingState->setValueSerializer(stateTable->getStateSerializer());
        existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
        return existingState;
    };

    std::vector<UV>* get() override
    {
        return stateTable->get(currentNamespace);
    };

    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };

    void clear() override
    {
        stateTable->clear(currentNamespace);
    };

    static BssListState<K, N, UV>* create(
        StateDescriptor* stateDesc, BssListStateTable<K, N, UV>* stateTable, TypeSerializer* keySerializer)
    {
        return new BssListState<K, N, UV>(
            stateTable, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
    };

    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override
    {
        stateTable->addVectorBatch(keyGroup, vectorBatch);
    };

    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override
    {
        stateTable->addVectorBatches(vectorBatchByKeyGroup);
    };

    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override
    {
        return stateTable->getVectorBatch(keyGroup, sequenceNumber);
    };

    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override
    {
        return stateTable->getVectorBatches(keyGroup);
    };

    uint32_t getNextSequenceNumber(int32_t keyGroup) override
    {
        return stateTable->getNextSequenceNumber(keyGroup);
    };

    void clearVectorBatches(int64_t currentTimestamp) override
    {
        stateTable->clearVectorBatches(currentTimestamp);
    }

    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override
    {
        stateTable->clearVectorBatches(keyGroup, sequenceNumbersToDelete);
    }

private:
    BssListStateTable<K, N, UV>* stateTable;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    N currentNamespace;
};

#endif // OMNISTREAM_BSSLISTSTATE_H
#endif
