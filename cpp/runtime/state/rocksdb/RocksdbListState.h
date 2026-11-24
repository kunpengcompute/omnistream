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

#ifndef OMNISTREAM_ROCKSDBLISTSTATE_H
#define OMNISTREAM_ROCKSDBLISTSTATE_H

#include <functional>
#include <vector>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/ListState.h"
#include "runtime/state/VoidNamespace.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "runtime/state/internal/InternalListState.h"
#include "RocksdbStateTable.h"

// The state is a list. In the InternalKvState, the state is stored as a pointer to a std::vector
template <typename K, typename N, typename UV>
class RocksdbListState : public InternalListState<K, N, UV> {
public:
    RocksdbListState(
        RocksdbStateTable<K, N, UV>* stateTable, TypeSerializer* valueSerializer, TypeSerializer* namespaceSerializer);

    ~RocksdbListState() = default;

    void createTable(
        ROCKSDB_NAMESPACE::DB* db,
        std::string cfName,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory);

    [[nodiscard]] TypeSerializer* getNamespaceSerializer() const
    {
        return namespaceSerializer;
    };

    [[nodiscard]] TypeSerializer* getValueSerializer() const
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

    // to be impl
    void update(const std::vector<UV>& values) override {};

    std::vector<UV>* get() override;

    // to be impl
    void merge(const std::vector<UV>& other) override {};

    void setCurrentNamespace(N nameSpace) override
    {
        currentNamespace = nameSpace;
    };

    void clear() override;

    static RocksdbListState<K, N, UV>* create(
        StateDescriptor* stateDesc, RocksdbStateTable<K, N, UV>* stateTable, TypeSerializer* keySerializer);

    static RocksdbListState<K, N, UV>* update(
        StateDescriptor* stateDesc, RocksdbStateTable<K, N, UV>* stateTable, RocksdbListState<K, N, UV>* existingState);

    uint32_t getNextSequenceNumber(int32_t keyGroup) override;
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override;
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override;
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override;
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override;
    void clearVectorBatches(int64_t currentTimestamp) override;
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override;

private:
    RocksdbStateTable<K, N, UV>* stateTable;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    N currentNamespace;
};

template <typename K, typename N, typename UV>
RocksdbListState<K, N, UV>* RocksdbListState<K, N, UV>::create(
    StateDescriptor* stateDesc, RocksdbStateTable<K, N, UV>* stateTable, TypeSerializer* keySerializer)
{
    return new RocksdbListState<K, N, UV>(
        stateTable, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
}

template <typename K, typename N, typename UV>
RocksdbListState<K, N, UV>::RocksdbListState(
    RocksdbStateTable<K, N, UV>* stateTable, TypeSerializer* valueSerializer, TypeSerializer* namespaceSerializer)
    : stateTable(stateTable),
      valueSerializer(valueSerializer),
      namespaceSerializer(namespaceSerializer)
{
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::createTable(
    rocksdb::DB* db,
    std::string cfName,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory)
{
    stateTable->createTable(db, cfName, kvStateInformation, columnFamilyOptionsFactory);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::add(const UV& value)
{
    stateTable->add(currentNamespace, value);
}

template <typename K, typename N, typename UV>
std::vector<UV>* RocksdbListState<K, N, UV>::get()
{
    return stateTable->getList(currentNamespace);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::addAll(const std::vector<UV>& values)
{
    stateTable->addAll(currentNamespace, values);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::clear()
{
    stateTable->clear(currentNamespace);
}

template <typename K, typename N, typename UV>
RocksdbListState<K, N, UV>* RocksdbListState<K, N, UV>::update(
    StateDescriptor* stateDesc, RocksdbStateTable<K, N, UV>* stateTable, RocksdbListState<K, N, UV>* existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename UV>
uint32_t RocksdbListState<K, N, UV>::getNextSequenceNumber(int32_t keyGroup)
{
    return stateTable->getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
{
    stateTable->addVectorBatch(keyGroup, vectorBatch);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::addVectorBatches(
    const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
{
    stateTable->addVectorBatches(vectorBatchByKeyGroup);
}

template <typename K, typename N, typename UV>
omnistream::VectorBatch* RocksdbListState<K, N, UV>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    return stateTable->getVectorBatch(keyGroup, sequenceNumber);
}

template <typename K, typename N, typename UV>
std::vector<omnistream::VectorBatch*> RocksdbListState<K, N, UV>::getVectorBatches(int32_t keyGroup)
{
    return stateTable->getVectorBatches(keyGroup);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::clearVectorBatches(int64_t currentTimestamp)
{
    return stateTable->clearVectorBatches(currentTimestamp);
}

template <typename K, typename N, typename UV>
void RocksdbListState<K, N, UV>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
{
    return stateTable->clearVectorBatches(keyGroup, sequenceNumbersToDelete);
}

#endif // OMNISTREAM_ROCKSDBLISTSTATE_H
