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

#include <emhash7.hpp>
#include <functional>
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "RocksdbMapStateTable.h"
#include "state/RocksIteratorWrapper.h"
#include "state/RocksDBWriteBatchWrapper.h"
#include "RocksDbOperationUtils.h"

// The state is a map. in the InternalKvState, the state is stored as a pointer to emhash7
template <typename K, typename N, typename UK, typename UV>
class RocksdbMapState : public MapState<UK, UV>, public InternalKvState<K, N, emhash7::HashMap<UK, UV>*> {
    friend struct RankStateTestAccess;

public:
    RocksdbMapState(
        RocksdbMapStateTable<K, N, UK, UV>* stateTable,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer);

    ~RocksdbMapState() = default;

    TypeSerializer* getKeySerializer() const
    {
        return keySerializer;
    };

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

    std::optional<UV> get(const UK& userKey) override;

    // for DataStream used
    Object* Get(Object* userKey) override;
    void GetByBatch(
        std::unordered_map<K, std::unordered_set<XXH128_hash_t>>& dataToGet,
        std::unordered_map<std::pair<K, XXH128_hash_t>, UV>& result);

    void put(const UK& userKey, const UV& userValue) override;
    // Encode while the caller's current partition key is set. WriteBatch
    // owns copied bytes; no pointer to a row or serializer escapes this call.
    void appendPut(ROCKSDB_NAMESPACE::WriteBatch& batch, const UK& key, const UV& value)
    {
        stateTable->putAllCurrentKey(batch, currentNamespace, {{key, value}});
    }

    void appendRemove(ROCKSDB_NAMESPACE::WriteBatch& batch, const UK& key)
    {
        stateTable->removeAllCurrentKey(batch, currentNamespace, {key});
    }

    void commitBatch(ROCKSDB_NAMESPACE::WriteBatch& batch)
    {
        if (batch.Count() != 0) {
            stateTable->writeBatch(batch, true, "rank batch");
        }
    }
    // Not virtual: one WriteBatch for the current key. Kept off the MapState vtable.
    void putEntries(const std::vector<std::pair<UK, UV>>& entries);
    void removeEntries(const std::vector<UK>& keys);
    void putByBatch(const K& key, const std::unordered_map<UK, UV>& dataToAdd);
    void putByBatch(std::unordered_map<K, std::unordered_map<UK, UV>>& dataToAdd);
    void putByBatch(std::unordered_map<K, std::vector<std::tuple<UK, UV>>>& dataToAdd);
    void putByBatch(std::vector<std::shared_ptr<std::tuple<K, UK, std::shared_ptr<std::string>>>>& dataToAdd);
    void putByBatch(std::vector<std::shared_ptr<std::tuple<K, UK, UV>>>& dataToAdd);
    void putByBatch(std::vector<std::tuple<K, UK, UV>>& dataToAdd);

    void remove(const UK& userKey) override;
    void removeByBatch(std::unordered_map<K, std::unordered_set<UK>>& dataToRemove);

    bool contains(const UK& userKey) override;

    void update(const UK& key, const UV& value) override;

    void setCurrentNamespace(N nameSpace) override;

    void clear() override;
    void clearEntriesCache();
    static RocksdbMapState<K, N, UK, UV>* create(
        StateDescriptor* stateDesc, RocksdbMapStateTable<K, N, UK, UV>* stateTable, TypeSerializer* keySerializer);

    static RocksdbMapState<K, N, UK, UV>* update(
        StateDescriptor* stateDesc,
        RocksdbMapStateTable<K, N, UK, UV>* stateTable,
        RocksdbMapState<K, N, UK, UV>* existingState);

    // This gets the pointer to the actual map (a value for state),
    // like Join's emhash<RowData*, int> with currentNamespace and currentKey
    emhash7::HashMap<UK, UV>* entries() override;

    // for DataStream used.
    java_util_Iterator* iterator() override;

    // for common scenario
    std::unique_ptr<typename MapState<UK, UV>::IteratorV2> iteratorV2() override;

    uint32_t getNextSequenceNumber(int32_t keyGroup) override;
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override;
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override;
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override;
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override;
    void clearVectorBatches(int64_t currentTimestamp) override;
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& indicesToDelete) override;

    void createTable(
        ROCKSDB_NAMESPACE::DB* db,
        std::string cfName,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory);
    std::shared_ptr<std::string> getRawBytes(UK& uk);
    void putRaw(UK& uk, const uint8_t* data, int length);

    void setMaxParallelism(const int32_t maxParallelism)
    {
        maxParallelism_ = maxParallelism;
    }

private:
    RocksdbMapStateTable<K, N, UK, UV>* stateTable;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    N currentNamespace;
    int32_t maxParallelism_;
};

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::clear()
{
    std::unique_ptr<RocksIteratorWrapper> iterator = RocksDbOperationUtils::getRocksIterator(
        stateTable->getRocksDB(), stateTable->getColumnFamily(), stateTable->getReadOptions());

    auto batchSizeObj =
        reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::WRITE_BATCH_SIZE));
    auto batchSize = MemorySize::parseBytes(batchSizeObj->getData());
    if (batchSizeObj != nullptr) {
        delete batchSizeObj;
    }
    // getWriteOptions() returns the table member, not a heap object.
    std::shared_ptr<rocksdb::WriteOptions> writeOptions(stateTable->getWriteOptions(), [](rocksdb::WriteOptions*) {});
    RocksDBWriteBatchWrapper rocksDBWriteBatchWrapper(
        stateTable->getRocksDB(), std::move(writeOptions), 500, batchSize);
    DataOutputSerializer outputSerializer;
    OutputBufferStatus outputBufferStatus;
    outputSerializer.setBackendBuffer(&outputBufferStatus);
    ROCKSDB_NAMESPACE::Slice keyPrefixBytes = stateTable->GetKeyNameSpaceSlice(outputSerializer, currentNamespace);
    iterator->seek(keyPrefixBytes);

    while (iterator->isValid()) {
        ROCKSDB_NAMESPACE::Slice keyBytes = iterator->key();
        if (keyBytes.starts_with(keyPrefixBytes)) {
            rocksDBWriteBatchWrapper.Delete(stateTable->getColumnFamily(), keyBytes);
        } else {
            break;
        }
        iterator->next();
    }
}

template <typename K, typename N, typename UK, typename UV>
emhash7::HashMap<UK, UV>* RocksdbMapState<K, N, UK, UV>::entries()
{
    return stateTable->entries(currentNamespace);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::clearEntriesCache()
{
    stateTable->entriesCache.clear();
}

template <typename K, typename N, typename UK, typename UV>
java_util_Iterator* RocksdbMapState<K, N, UK, UV>::iterator()
{
    if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
        return stateTable->iterator(currentNamespace);
    } else {
        THROW_LOGIC_EXCEPTION("type is not Object in RocksdbMapState::iterator()");
    }
}

template <typename K, typename N, typename UK, typename UV>
std::unique_ptr<typename MapState<UK, UV>::IteratorV2> RocksdbMapState<K, N, UK, UV>::iteratorV2()
{
    return stateTable->iteratorV2(currentNamespace);
}

template <typename K, typename N, typename UK, typename UV>
std::optional<UV> RocksdbMapState<K, N, UK, UV>::get(const UK& userKey)
{
    if (stateTable == nullptr) {
        throw std::runtime_error("RocksdbMapStateTable is not initialized.");
    }
    UV uv = stateTable->get(currentNamespace, userKey);
    if constexpr (std::is_pointer_v<UV>) {
        if (uv == nullptr) {
            return std::nullopt;
        }
    } else {
        if (uv == std::numeric_limits<UV>::max()) {
            return std::nullopt;
        }
    }
    return std::make_optional<UV>(uv);
}

template <typename K, typename N, typename UK, typename UV>
Object* RocksdbMapState<K, N, UK, UV>::Get(Object* userKey)
{
    if (stateTable == nullptr) {
        throw std::runtime_error("RocksdbMapStateTable is not initialized.");
    }
    if constexpr (std::is_same_v<UV, Object*>) {
        UV uv = stateTable->get(currentNamespace, userKey);
        return uv;
    } else {
        THROW_LOGIC_EXCEPTION("type is not Object in RocksdbMapState::get()");
    }
}

template <typename K, typename N, typename UK, typename UV>
std::shared_ptr<std::string> RocksdbMapState<K, N, UK, UV>::getRawBytes(UK& uk)
{
    return stateTable->getRawBytes(currentNamespace, uk);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putRaw(UK& uk, const uint8_t* data, int length)
{
    stateTable->putRaw(currentNamespace, uk, data, length);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::GetByBatch(
    std::unordered_map<K, std::unordered_set<XXH128_hash_t>>& dataToGet,
    std::unordered_map<std::pair<K, XXH128_hash_t>, UV>& result)
{
    if (stateTable == nullptr) {
        throw std::runtime_error("RocksdbMapStateTable is not initialized.");
    }
    stateTable->GetByBatch(currentNamespace, dataToGet, result);
}

template <typename K, typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV>::RocksdbMapState(
    RocksdbMapStateTable<K, N, UK, UV>* stateTable,
    TypeSerializer* keySerializer,
    TypeSerializer* valueSerializer,
    TypeSerializer* namespaceSerializer)
{
    this->keySerializer = keySerializer;
    this->namespaceSerializer = namespaceSerializer;
    this->valueSerializer = valueSerializer;
    this->stateTable = stateTable;
    currentNamespace = N();
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::put(const UK& userKey, const UV& userValue)
{
    stateTable->put(currentNamespace, userKey, userValue);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putEntries(const std::vector<std::pair<UK, UV>>& entries)
{
    if (entries.empty()) {
        return;
    }
    ROCKSDB_NAMESPACE::WriteBatch putBatch;
    stateTable->putAllCurrentKey(putBatch, currentNamespace, entries);
    stateTable->writeBatch(putBatch, true, "putAll");
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::removeEntries(const std::vector<UK>& keys)
{
    if (keys.empty()) {
        return;
    }
    ROCKSDB_NAMESPACE::WriteBatch deleteBatch;
    stateTable->removeAllCurrentKey(deleteBatch, currentNamespace, keys);
    stateTable->writeBatch(deleteBatch, false, "removeAll");
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putByBatch(const K& key, const std::unordered_map<UK, UV>& dataToAdd)
{
    stateTable->putByBatch(currentNamespace, key, dataToAdd);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putByBatch(std::unordered_map<K, std::unordered_map<UK, UV>>& dataToAdd)
{
    stateTable->putByBatch(currentNamespace, dataToAdd);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putByBatch(std::unordered_map<K, std::vector<std::tuple<UK, UV>>>& dataToAdd)
{
    stateTable->putByBatch(currentNamespace, dataToAdd);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putByBatch(
    std::vector<std::shared_ptr<std::tuple<K, UK, std::shared_ptr<std::string>>>>& dataToAdd)
{
    stateTable->putByBatch(currentNamespace, dataToAdd);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putByBatch(std::vector<std::shared_ptr<std::tuple<K, UK, UV>>>& dataToAdd)
{
    stateTable->putByBatch(currentNamespace, dataToAdd);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::putByBatch(std::vector<std::tuple<K, UK, UV>>& dataToAdd)
{
    stateTable->putByBatch(currentNamespace, dataToAdd);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::remove(const UK& userKey)
{
    stateTable->remove(currentNamespace, userKey);
}
template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::removeByBatch(std::unordered_map<K, std::unordered_set<UK>>& dataToRemove)
{
    stateTable->removeByBatch(currentNamespace, dataToRemove);
}
template <typename K, typename N, typename UK, typename UV>
bool RocksdbMapState<K, N, UK, UV>::contains(const UK& userKey)
{
    return stateTable->contains(currentNamespace, userKey);
}

template <typename K, typename N, typename UK, typename UV>
inline void RocksdbMapState<K, N, UK, UV>::update(const UK& userKey, const UV& userValue)
{
    stateTable->put(currentNamespace, userKey, userValue);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::setCurrentNamespace(N nameSpace)
{
    currentNamespace = nameSpace;
}

template <typename K, typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV>* RocksdbMapState<K, N, UK, UV>::create(
    StateDescriptor* stateDesc, RocksdbMapStateTable<K, N, UK, UV>* stateTable, TypeSerializer* keySerializer)
{
    return new RocksdbMapState<K, N, UK, UV>(
        stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
}

template <typename K, typename N, typename UK, typename UV>
RocksdbMapState<K, N, UK, UV>* RocksdbMapState<K, N, UK, UV>::update(
    StateDescriptor* stateDesc,
    RocksdbMapStateTable<K, N, UK, UV>* stateTable,
    RocksdbMapState<K, N, UK, UV>* existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename UK, typename UV>
uint32_t RocksdbMapState<K, N, UK, UV>::getNextSequenceNumber(int32_t keyGroup)
{
    return stateTable->getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
{
    stateTable->addVectorBatch(keyGroup, vectorBatch);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::addVectorBatches(
    const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
{
    stateTable->addVectorBatches(vectorBatchByKeyGroup);
}

template <typename K, typename N, typename UK, typename UV>
omnistream::VectorBatch* RocksdbMapState<K, N, UK, UV>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    return stateTable->getVectorBatch(keyGroup, sequenceNumber);
};

template <typename K, typename N, typename UK, typename UV>
std::vector<omnistream::VectorBatch*> RocksdbMapState<K, N, UK, UV>::getVectorBatches(int32_t keyGroup)
{
    return stateTable->getVectorBatches(keyGroup);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::createTable(
    ROCKSDB_NAMESPACE::DB* db,
    std::string cfName,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory)
{
    stateTable->createTable(db, cfName, kvStateInformation, columnFamilyOptionsFactory);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::clearVectorBatches(int64_t currentTimestamp)
{
    return stateTable->clearVectorBatches(currentTimestamp);
}

template <typename K, typename N, typename UK, typename UV>
void RocksdbMapState<K, N, UK, UV>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& indicesToDelete)
{
    return stateTable->clearVectorBatches(keyGroup, indicesToDelete);
}
