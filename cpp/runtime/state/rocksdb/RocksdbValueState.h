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

#include <functional>
#include <unordered_map>

#include "core/typeutils/TypeSerializer.h"
#include "../AbstractKeyedStateBackend.h"
#include "RocksdbStateTable.h"
#include "state/RocksDbKvStateInfo.h"
#include "runtime/state/rocksdb/falcon/ValueStateFalconKey.h"
#include "runtime/state/rocksdb/falcon/FalconValue.h"
#include "runtime/state/rocksdb/falcon/ValueStateCache.h"
#include "runtime/state/internal/InternalValueState.h"

const float CACHE_SIZE_UPPER_LIMIT = 1.2;

template <typename K, typename N, typename V>
class ValueStateLRUCache;

template <typename K, typename N, typename V>
class RocksdbValueState : public InternalValueState<K, N, V> {
public:
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

    void setDefaultValue(V value)
    {
        defaultValue = value;
    };

    static RocksdbValueState<K, N, V>* create(
        StateDescriptor* stateDesc, RocksdbStateTable<K, N, V>* stateTable, TypeSerializer* keySerializer);

    static RocksdbValueState<K, N, V>* update(
        StateDescriptor* stateDesc, RocksdbStateTable<K, N, V>* stateTable, RocksdbValueState<K, N, V>* existingState);

    RocksdbValueState(
        RocksdbStateTable<K, N, V>* stateTable,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer,
        V defaultValue);

    ~RocksdbValueState()
    {
        delete stateCache;
        stateCache = nullptr; // [FALCON] delete falcon cache corresponding to this ValueState
    };

    void createTable(
        ROCKSDB_NAMESPACE::DB* db,
        std::string cfName,
        std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
        const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory);

    void clear() override;
    uint32_t getNextSequenceNumber(int32_t keyGroup) override;
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override;
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override;
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override;
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override;
    void updateByBatch(std::unordered_map<K, V>& pendingUpdates);
    void clearVectorBatches(int64_t currentTimestamp) override;
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override;
    // [FALCON] -------------------------------------------------------------------------------------------
    // function to get defaultValue, get currentNamespace and set currentKey & namespace
    V getDefaultValue()
    {
        return defaultValue;
    };
    K getCurrentKey()
    {
        return stateTable->getCurrentKey();
    }
    N getNamespace()
    {
        return currentNamespace;
    }
    void setKeyAndNamespace(K key, N ns)
    {
        stateTable->setCurrentKey(key);
        setCurrentNamespace(ns);
    }

    // rename RocksDBValueState.value(), RocksDBValueState.update() and AbstractRocksDBState.clear()
    V getValue();
    void writeValue(const V& value);
    void deleteValue();

    bool isFalconEnabled() const;

    ValueStateLRUCache<K, N, V>* stateCache; // falcon cache corresponding to this ValueState
    // [FALCON] -------------------------------------------------------------------------------------------

private:
    RocksdbStateTable<K, N, V>* stateTable;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    V defaultValue;
    N currentNamespace;
};

/**
 * ValueStateCache implementation using LRU method, which is bind with a deterministic ColumnFamily.
 *
 * <p> For states(FalconKey-FalconValue pairs) corresponding to this columnFamily, we use a LinkedHashMap to store them,
 * and hot state are moved to the head of link head, while cold state are moved to the tail of link. Using this data
 * structure, read、write、hot state insertion and cold state elimination are all O(1) time complexity.
 *
 * @param [K] Type of ValueState's key
 * @param [N] Type of ValueState's namespace
 * @param [V] Type of ValueState's value
 */

template <typename K, typename N, typename V>
class ValueStateLRUCache : public ValueStateCache<K, N, V> {
public:
    // 类型别名
    using KeyType = ValueStateFalconKey<K, N>;
    using ValType = FalconValue<V>;

    explicit ValueStateLRUCache(RocksdbValueState<K, N, V>* dbAccessor)
    {
        cacheSizeLimit = 0;
        valueState = dbAccessor;
    }

    ~ValueStateLRUCache()
    {
        clearAll();
    }

    bool contains(const K& key, const N& ns) const
    {
        KeyType lookupKey(key, ns);
        return cache.find(&lookupKey) != cache.end();
    }

    V get(const K& key, const N& ns) override
    {
        // use new operator to create falconKey and falconValue, so that their life circle are bind with falcon cache.
        // when remove elements from cache, e.g., using erase() or clear(), make sure delete operation is called.
        auto falconKey = new KeyType(key, ns);
        auto it = cache.find(falconKey);
        if (it != cache.end()) { // falcon cache hit
            V value = it->second->getValue();
            // [refcount] udf and omniStream will interaction through RocksdbValueState.value(), after udf have used
            // the return value, it will decrease value's refcount, thus we should increase value's refcount here.
            if constexpr (std::is_same_v<V, Object*>) {
                if (value != nullptr) {
                    reinterpret_cast<Object*>(value)->getRefCount();
                }
            }
            delete falconKey; // if we do not insert falconKey into falcon cache, directly delete it.
            return value;
        } else { // falcon cache miss, get from rocksdb and insert into falcon cache
            V value = valueState->getValue();
            V defaultValue = valueState->getDefaultValue();
            // V type can be Object*, RowData*, std::vector<long>* or int type
            if constexpr (std::is_pointer_v<V>) {
                // nullptr should not be inserted into cache, directly return
                if (value == nullptr) {
                    delete falconKey;
                    return value;
                }
                // if value and default value are not nullptr, compare them and decide whether to insert into cache
                if (value != nullptr && defaultValue != nullptr) {
                    if constexpr (std::is_same_v<V, Object*>) {
                        if (reinterpret_cast<Object*>(value)->equals(defaultValue)) {
                            delete falconKey;
                            return value;
                        }
                    } else if constexpr (std::is_same_v<V, std::vector<long>*>) {
                        if (*value == *defaultValue) { // dereference value and default value and compare
                            delete falconKey;
                            return value;
                        }
                    } else {
                        // RowData* and int type, use == to compare. todo: GenericRowData == method is not implemented
                        if (value == defaultValue) {
                            delete falconKey;
                            return value;
                        }
                    }
                }
            } else { // int type
                if (value == defaultValue) {
                    delete falconKey;
                    return value;
                }
            }
            auto falconValue = new ValType(value, false);
            cache.emplace(falconKey, falconValue);
            if (cache.size() > cacheSizeLimit) {
                removeEldestState();
            }
            return value;
        }
    }

    void put(const K& key, const N& ns, const V& value) override
    {
        auto falconKey = new KeyType(key, ns);
        auto falconValue = new ValType(value, false);
        auto it = cache.find(falconKey);
        if (it != cache.end()) { // falcon cache hit
            // delete old state and then insert new ones, if we directly update falconValue, old falconValue will leak
            delete it->first;
            delete it->second;
            cache.erase(it);
            falconValue->markAsDirty();
            cache.emplace(falconKey, falconValue);
        } else { // falcon cache miss, put to rocksdb and insert into falcon cache
            valueState->writeValue(value);
            cache.emplace(falconKey, falconValue);
            if (cache.size() > cacheSizeLimit) {
                removeEldestState();
            }
        }
    }

    void remove(const K& key, const N& ns) override
    {
        auto falconKey = new KeyType(key, ns);
        auto it = cache.find(falconKey);
        if (it != cache.end()) {
            delete it->first;
            delete it->second; // delete falconKey and falconValue object
            cache.erase(it);   // delete falconKey and falconValue pointer
        }
        delete falconKey;
        // the state is going to be deleted from rocksdb, thus we do not need to flush it from falcon cache to rocksdb
        valueState->deleteValue();
    }

    void removeEldestState() override
    {
        flush();
        clearAll();
    }

    void flush() override
    {
        K currentKey = valueState->getCurrentKey();
        N currentNamespace = valueState->getNamespace();
        // [refcount] currentKey's refcount will -1 when setKeyAndNamespace during flush loop, if we directly restore it
        // after flush loop, currentKey will be pointed to null object.
        if constexpr (std::is_same_v<K, Object*>) {
            if (currentKey != nullptr) {
                reinterpret_cast<Object*>(currentKey)->getRefCount();
            }
        }
        for (auto& [falconKey, falconValue] : cache) {
            if (falconValue->isDirty()) {
                valueState->setKeyAndNamespace(falconKey->getKey(), falconKey->getNamespace());
                valueState->writeValue(falconValue->getValue());
                falconValue->markAsClean();
            }
        }
        // after flush, restore key and namespace. Note that when flush in snapshot, key and namespace may be null, thus
        // we do not restore them after flush.
        if constexpr (std::is_pointer_v<K>) {
            if (currentKey == nullptr) {
                return;
            }
        }
        if constexpr (std::is_pointer_v<N>) {
            if (currentNamespace == nullptr) {
                return;
            }
        }
        valueState->setKeyAndNamespace(currentKey, currentNamespace);
        // [refcount] get refcount +1 when get currentKey from valueState, then refcount -1 after setKeyAndNamespace
        if constexpr (std::is_same_v<K, Object*>) {
            reinterpret_cast<Object*>(currentKey)->putRefCount();
        }
    }

    void clearAll() override
    {
        for (auto& [falconKey, falconValue] : cache) {
            delete falconKey;
            delete falconValue;
        }
        cache.clear();
    }

    [[nodiscard]] int getSizeLimit() const override
    {
        return cacheSizeLimit;
    }

    void updateSizeLimit(int newSizeLimit) override
    {
        cacheSizeLimit = newSizeLimit;
        if (cache.size() > cacheSizeLimit) {
            flush();
            clearAll();
        }
    }

private:
    int cacheSizeLimit{}; // stateCache size limit, 0 means disable falcon cache, otherwise enable falcon cache
    RocksdbValueState<K, N, V>* valueState = {};
    std::unordered_map<KeyType*, ValType*> cache = {};
};

// [FALCON] -------------------------------------------------------------------------------------------
template <typename K, typename N, typename V>
bool RocksdbValueState<K, N, V>::isFalconEnabled() const
{
    return stateCache != nullptr && stateCache->getSizeLimit() != 0;
}

template <typename K, typename N, typename V>
V RocksdbValueState<K, N, V>::getValue()
{
    auto result = stateTable->get(currentNamespace);
    // For pointer types (e.g. BinaryRowData*, set<long>*): return defaultValue when null.
    // For BinaryRowData*, the underlying implementation uses a shared instance to deserialize
    // from RocksDB, so we must copy it to avoid it being overwritten by the next get.
    if constexpr (std::is_pointer<V>::value) {
        if (result == nullptr) {
            return defaultValue;
        }
        if constexpr (std::is_base_of_v<RowData, std::remove_pointer_t<V>>) {
            auto* br = dynamic_cast<BinaryRowData*>(result);
            if (br == nullptr) {
                return result;
            }
            auto* copied = br->copy();
            return static_cast<V>(copied);
        } else {
            // Other pointer types (e.g. set<long>*) are not shared; return as-is.
            return result;
        }
    } else {
        // S can only be RowData*, cowMap<RowData*, int>*, or int-like types for now.
        return result == std::numeric_limits<V>::max() ? defaultValue : result;
    }
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::writeValue(const V& value)
{
    if constexpr (std::is_pointer_v<V>) {
        if (value == nullptr) {
            clear();
            return;
        }
    }

    // V进来序列化一下，key也要序列化一下
    // copy key
    stateTable->put(currentNamespace, value);
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::deleteValue()
{
    stateTable->clear(currentNamespace);
}
// [FALCON] -------------------------------------------------------------------------------------------

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V>* RocksdbValueState<K, N, V>::create(
    StateDescriptor* stateDesc, RocksdbStateTable<K, N, V>* stateTable, TypeSerializer* keySerializer)
{
    return new RocksdbValueState<K, N, V>(
        stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer(), V());
}

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V>::RocksdbValueState(
    RocksdbStateTable<K, N, V>* stateTable,
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
    stateCache = new ValueStateLRUCache(this); // [FALCON] init falcon cache corresponding to this ValueState
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::createTable(
    ROCKSDB_NAMESPACE::DB* db,
    std::string cfName,
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
    const std::function<ROCKSDB_NAMESPACE::ColumnFamilyOptions(const std::string&)>& columnFamilyOptionsFactory)
{
    stateTable->createTable(db, cfName, kvStateInformation, columnFamilyOptionsFactory);
}

template <typename K, typename N, typename V>
V RocksdbValueState<K, N, V>::value()
{
    if (stateCache->getSizeLimit() == 0) {
        return getValue();
    } else {
        return stateCache->get(getCurrentKey(), getNamespace());
    }
}

template <typename K, typename N, typename V>
bool RocksdbValueState<K, N, V>::isEmpty()
{
    if (stateCache->getSizeLimit() == 0) {
        return !stateTable->containsKey(currentNamespace);
    }
    // With the falcon cache a freshly written entry may sit dirty in the cache
    // (not yet flushed to RocksDB); presence is true if either side holds it.
    return !(stateCache->contains(getCurrentKey(), getNamespace()) ||
             stateTable->containsKey(currentNamespace));
}

template <typename K, typename N, typename V>
bool RocksdbValueState<K, N, V>::getIfPresent(V& out)
{
    if (isEmpty()) {
        return false;
    }
    if (stateCache->getSizeLimit() == 0) {
        if constexpr (std::is_pointer_v<V>) {
            // Pointer types: go through getValue() to keep the RowData-derived
            // (e.g. BinaryRowData) copy() semantics. RocksdbStateTable::get()
            // hands back a shared deserialize buffer that the next read
            // overwrites, so returning it directly would allow use-after-overwrite.
            out = getValue();
        } else {
            // Raw read: RocksdbStateTable::get returns max() only for an absent entry,
            // so a present LONG_MAX is returned unchanged here.
            out = stateTable->get(currentNamespace);
        }
    } else if (stateCache->contains(getCurrentKey(), getNamespace())) {
        out = stateCache->get(getCurrentKey(), getNamespace());
    } else {
        if constexpr (std::is_pointer_v<V>) {
            out = getValue();
        } else {
            out = stateTable->get(currentNamespace);
        }
    }
    return true;
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::update(const V& value, bool copyKey)
{
    if (stateCache->getSizeLimit() == 0) {
        writeValue(value);
    } else {
        if constexpr (std::is_pointer_v<V>) {
            if (value == nullptr) {
                stateCache->remove(getCurrentKey(), getNamespace());
                return;
            }
        }
        stateCache->put(getCurrentKey(), getNamespace(), value);
    }
}

template <typename K, typename N, typename V>
RocksdbValueState<K, N, V>* RocksdbValueState<K, N, V>::update(
    StateDescriptor* stateDesc, RocksdbStateTable<K, N, V>* stateTable, RocksdbValueState<K, N, V>* existingState)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    return existingState;
}

template <typename K, typename N, typename V>
uint32_t RocksdbValueState<K, N, V>::getNextSequenceNumber(int32_t keyGroup)
{
    return stateTable->getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
{
    stateTable->addVectorBatch(keyGroup, vectorBatch);
};

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::addVectorBatches(
    const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
{
    stateTable->addVectorBatches(vectorBatchByKeyGroup);
}

template <typename K, typename N, typename V>
omnistream::VectorBatch* RocksdbValueState<K, N, V>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    return stateTable->getVectorBatch(keyGroup, sequenceNumber);
};

template <typename K, typename N, typename V>
std::vector<omnistream::VectorBatch*> RocksdbValueState<K, N, V>::getVectorBatches(int32_t keyGroup)
{
    return stateTable->getVectorBatches(keyGroup);
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::clear()
{
    if (stateCache->getSizeLimit() == 0) {
        deleteValue();
    } else {
        stateCache->remove(getCurrentKey(), getNamespace());
    }
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::updateByBatch(std::unordered_map<K, V>& pendingUpdates)
{
    stateTable->putByBatch(currentNamespace, pendingUpdates);
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::clearVectorBatches(int64_t currentTimestamp)
{
    return stateTable->clearVectorBatches(currentTimestamp);
}

template <typename K, typename N, typename V>
void RocksdbValueState<K, N, V>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
{
    return stateTable->clearVectorBatches(keyGroup, sequenceNumbersToDelete);
}
