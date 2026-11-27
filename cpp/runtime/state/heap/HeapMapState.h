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
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/StateDescriptor.h"
#include "StateTable.h"
#include "CopyOnWriteStateTable.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/typeutils/VectorBatchSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/VoidNamespaceSerializer.h"
#include "basictypes/java_util_HashMap.h"
#include "core/typeutils/LongSerializer.h"

// The state is a map. in the InternalKvState, the state is stored as a pointer to emhash7
template <typename K, typename N, typename UK, typename UV>
class HeapMapState : public MapState<UK, UV>, public InternalKvState<K, N, emhash7::HashMap<UK, UV>*> {
public:
    HeapMapState(
        StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable,
        TypeSerializer* keySerializer,
        TypeSerializer* valueSerializer,
        TypeSerializer* namespaceSerializer);

    ~HeapMapState() override;

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

    // Borrow the backend-owned key, so callers replacing/removing pointer
    // values can preserve its ownership without scanning the map.
    std::optional<UK> getStoredKey(const UK& userKey)
    {
        auto* userMap = stateTable->get(currentNamespace);
        if (userMap != nullptr) {
            auto it = userMap->find(userKey);
            if (it != userMap->end()) {
                return it->first;
            }
        }
        return std::nullopt;
    }

    // for DataStream used
    Object* Get(Object* userKey) override;

    void put(const UK& userKey, const UV& userValue) override;

    void updateOrCreate(const UK& key, UV defaultValue, std::function<std::optional<UV>(UV&)> transformFunc) override;

    void remove(const UK& userKey) override;

    bool contains(const UK& userKey) override;

    void update(const UK& key, const UV& value) override;

    // void updateOrCreate(const UK &key, const UV defaultValue, typename HeapMapState::ValueTransformFuncPtr
    // transformFunc) override;

    void setCurrentNamespace(N nameSpace) override;

    void clear() override
    {
        emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
        if (userMap != nullptr) {
            if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
                // Decrement refcounts for all Object* entries before the state table removes
                // the owning HashMap pointer.
                for (auto& pair : *userMap) {
                    Object* key = static_cast<Object*>(pair.first);
                    Object* value = static_cast<Object*>(pair.second);
                    if (key != nullptr) {
                        key->putRefCount();
                    }
                    if (value != nullptr) {
                        value->putRefCount();
                    }
                }
            }
        }
        stateTable->remove(currentNamespace);
    };

    static HeapMapState<K, N, UK, UV>* create(
        StateDescriptor* stateDesc,
        StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable,
        TypeSerializer* keySerializer,
        StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable);

    static HeapMapState<K, N, UK, UV>* update(
        StateDescriptor* stateDesc,
        StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable,
        HeapMapState<K, N, UK, UV>* existingState,
        StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable);

    // This gets the pointer to the actual map (a value for state), like Join's emhash<RowData*, int> with
    // currentNamespace and currentKey
    typename emhash7::HashMap<UK, UV>* entries() override;

    // for DataStream used
    java_util_Iterator* iterator() override;

    // for common scenario
    class HeapMapEntryV2;
    class HeapMapIteratorV2;
    std::unique_ptr<typename MapState<UK, UV>::IteratorV2> iteratorV2() override;

    void clearVectorBatches(int64_t currentTimestamp) override;
    void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete) override;
    uint32_t getNextSequenceNumber(int32_t keyGroup) override;
    void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch) override;
    void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup) override;
    omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber) override;
    std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup) override;

private:
    StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable;
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchStateTable = nullptr;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
    TypeSerializer* namespaceSerializer;
    N currentNamespace;
};

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::updateOrCreate(
    const UK& key, UV defaultValue, std::function<std::optional<UV>(UV&)> transformFunc)
{
    emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        // First-time initialization for namespace
        userMap = new emhash7::HashMap<UK, UV>(1);
        stateTable->put(currentNamespace, userMap);
        LOG_PRINTF("created userMap at %p\n", userMap);
    }
    userMap->updateOrCreate(key, defaultValue, transformFunc);
}

template <typename K, typename N, typename UK, typename UV>
emhash7::HashMap<UK, UV>* HeapMapState<K, N, UK, UV>::entries()
{
    LOG_PRINTF("----HeapMapState::entries\\n\\t\\t this=%p, stateTable=%p\n", this, stateTable);
    typename emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    LOG_PRINTF("----HeapMapState::entries\\n\\t\\t this=%p foundUserMap=%p\n", this, userMap);
    return userMap;
}

template <typename K, typename N, typename UK, typename UV>
java_util_Iterator* HeapMapState<K, N, UK, UV>::iterator()
{
    if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
        emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
        if (userMap == nullptr) {
            HashMap::EMPTY_ITERATOR->getRefCount();
            return HashMap::EMPTY_ITERATOR;
        }
        HashMap* MapWrapper = new HashMap(userMap, false);
        auto mapIterator = MapWrapper->iterator();
        // we should put refcount befro
        MapWrapper->putRefCount();
        return mapIterator;
    } else {
        THROW_LOGIC_EXCEPTION("type is not Object in HeapMapState::iterator()");
    }
}

template <typename K, typename N, typename UK, typename UV>
class HeapMapState<K, N, UK, UV>::HeapMapEntryV2 : public MapState<UK, UV>::MapEntryV2 {
    friend class HeapMapIteratorV2;

public:
    HeapMapEntryV2() = default;
    explicit HeapMapEntryV2(typename emhash7::HashMap<UK, UV>::Iterator iter) : currentIterator_(iter)
    {
    }

    std::optional<UK> getKey() override
    {
        return currentIterator_->first;
    }
    std::optional<UV> getValue() override
    {
        return currentIterator_->second;
    }

    void setValue(std::optional<UV> value) override
    {
        if (value.has_value()) {
            currentIterator_->second = value.value();
        } else {
            THROW_RUNTIME_ERROR("setValue() called with empty value.");
        }
    }

private:
    typename emhash7::HashMap<UK, UV>::Iterator currentIterator_;
};

template <typename K, typename N, typename UK, typename UV>
class HeapMapState<K, N, UK, UV>::HeapMapIteratorV2 : public MapState<UK, UV>::IteratorV2 {
public:
    explicit HeapMapIteratorV2(emhash7::HashMap<UK, UV>* userMap)
        : userMap_(userMap),
          currentIterator_(userMap_ != nullptr ? userMap_->begin() : typename emhash7::HashMap<UK, UV>::Iterator())
    {
    }

    bool hasNext() override
    {
        return userMap_ && currentIterator_ != userMap_->end();
    }

    HeapMapEntryV2& next() override
    {
        if (!hasNext()) {
            THROW_RUNTIME_ERROR("No more elements in the iterator.");
        }
        cacheEntry_ = HeapMapEntryV2(currentIterator_);
        ++currentIterator_;
        return cacheEntry_;
    }

private:
    emhash7::HashMap<UK, UV>* userMap_{};
    typename emhash7::HashMap<UK, UV>::Iterator currentIterator_{};
    HeapMapEntryV2 cacheEntry_{};
};

template <typename K, typename N, typename UK, typename UV>
std::unique_ptr<typename MapState<UK, UV>::IteratorV2> HeapMapState<K, N, UK, UV>::iteratorV2()
{
    auto* userMap = stateTable->get(currentNamespace);
    auto iterator = std::make_unique<HeapMapIteratorV2>(userMap);
    return iterator;
}

template <typename K, typename N, typename UK, typename UV>
std::optional<UV> HeapMapState<K, N, UK, UV>::get(const UK& userKey)
{
    if (stateTable == nullptr) {
        throw std::runtime_error("StateTable is not initialized.");
    }

    emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        return std::nullopt;
    }
    auto it = userMap->find(userKey);
    if (it == userMap->end()) {
        return std::nullopt;
    }
    return (it->second);
}

template <typename K, typename N, typename UK, typename UV>
Object* HeapMapState<K, N, UK, UV>::Get(Object* userKey)
{
    if (stateTable == nullptr) {
        throw std::runtime_error("StateTable is not initialized.");
    }

    if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
        emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
        if (userMap == nullptr) {
            return nullptr;
        }
        auto it = userMap->find(userKey);
        if (it == userMap->end()) {
            return nullptr;
        }
        auto value = static_cast<Object*>(it->second);
        if (value != nullptr) {
            value->getRefCount();
        }
        return value;
    } else {
        THROW_LOGIC_EXCEPTION("type is not Object in HeapMapState::get()");
    }
}

template <typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV>::~HeapMapState()
{
}

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::clearVectorBatches(int64_t currentTimestamp)
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

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
{
    VoidNamespace nameSpace;
    for (auto sequenceNumber : sequenceNumbersToDelete) {
        vectorBatchStateTable->remove(sequenceNumber, keyGroup, nameSpace);
    }
}

template <typename K, typename N, typename UK, typename UV>
uint32_t HeapMapState<K, N, UK, UV>::getNextSequenceNumber(int32_t keyGroup)
{
    return vectorBatchStateTable->getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
{
    VoidNamespace nameSpace;
    auto sequenceNumber = vectorBatchStateTable->getNextSequenceNumber(keyGroup);
    vectorBatchStateTable->put(sequenceNumber, keyGroup, nameSpace, vectorBatch);
    vectorBatchStateTable->addNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::addVectorBatches(
    const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
{
    VoidNamespace nameSpace;
    for (const auto& [keyGroup, vectorBatch] : vectorBatchByKeyGroup) {
        auto nextSequenceNumber = vectorBatchStateTable->getNextSequenceNumber(keyGroup);
        vectorBatchStateTable->put(nextSequenceNumber, keyGroup, nameSpace, vectorBatch);
        vectorBatchStateTable->addNextSequenceNumber(keyGroup);
    }
}

template <typename K, typename N, typename UK, typename UV>
omnistream::VectorBatch* HeapMapState<K, N, UK, UV>::getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
{
    VoidNamespace nameSpace;
    return vectorBatchStateTable->get(sequenceNumber, keyGroup, nameSpace);
}

template <typename K, typename N, typename UK, typename UV>
std::vector<omnistream::VectorBatch*> HeapMapState<K, N, UK, UV>::getVectorBatches(int32_t keyGroup)
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

template <typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV>::HeapMapState(
    StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable,
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
void HeapMapState<K, N, UK, UV>::put(const UK& userKey, const UV& userValue)
{
    LOG_PRINTF("HeapMapState::put\\n\\t\\t this=%p, stateTable=%p\n", this, stateTable);
    emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    LOG_PRINTF("stateTable %p found userMap? %p\n", stateTable, userMap);
    if (userMap == nullptr) {
        userMap = new emhash7::HashMap<UK, UV>();
        LOG_PRINTF("created userMap? %p\n", userMap);

        (*userMap)[userKey] = userValue;
        if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
            // avoid userKey object memory freed.
            static_cast<Object*>(userKey)->getRefCount();
            static_cast<Object*>(userValue)->getRefCount();
        }
        // Save this Map to stateTable
        stateTable->put(currentNamespace, userMap);
        LOG_PRINTF("userMap? %p put to stateTable %p\n", userMap, stateTable);
    } else {
        if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
            // datastream: avoid userKey object memory freed.
            auto newKey = static_cast<Object*>(userKey);
            auto newValue = static_cast<Object*>(userValue);
            auto it = userMap->find(newKey);
            newKey->getRefCount();
            newValue->getRefCount();
            if (it == userMap->end()) {
                (*static_cast<emhash7::HashMap<Object*, Object*>*>(userMap))[newKey] = newValue;
            } else {
                Object* oldKey = static_cast<Object*>(it->first);
                Object* oldValue = static_cast<Object*>(it->second);
                (*static_cast<emhash7::HashMap<Object*, Object*>*>(userMap))[oldKey] = newValue;
                oldValue->putRefCount();
                newKey->putRefCount();
            }
        } else {
            // sql
            (*userMap)[userKey] = userValue;
        }
    }
}

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::remove(const UK& userKey)
{
    emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        return;
    }

    if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
        auto it = userMap->find(userKey);
        if (it != userMap->end()) {
            Object* oldKey = static_cast<Object*>(it->first);
            Object* oldValue = static_cast<Object*>(it->second);
            userMap->erase(it);
            if (oldKey != nullptr) {
                oldKey->putRefCount();
            }
            if (oldValue != nullptr) {
                oldValue->putRefCount();
            }
        }
    } else {
        userMap->erase(userKey);
    }

    if (userMap->empty()) {
        clear();
    }
}

template <typename K, typename N, typename UK, typename UV>
bool HeapMapState<K, N, UK, UV>::contains(const UK& userKey)
{
    emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    return userMap != nullptr && (userMap->find(userKey) != userMap->end());
}

template <typename K, typename N, typename UK, typename UV>
inline void HeapMapState<K, N, UK, UV>::update(const UK& userKey, const UV& userValue)
{
    emhash7::HashMap<UK, UV>* userMap = stateTable->get(currentNamespace);
    if (userMap == nullptr) {
        throw std::runtime_error("User map is null");
    }
    auto it = userMap->find(userKey);
    if (it == userMap->end()) {
        return;
    }
    if constexpr (std::is_same_v<UK, Object*> && std::is_same_v<UV, Object*>) {
        Object* oldValue = static_cast<Object*>(it->second);
        Object* newValue = static_cast<Object*>(userValue);
        it->second = userValue;
        if (newValue != nullptr) {
            newValue->getRefCount();
        }
        if (oldValue != nullptr) {
            oldValue->putRefCount();
        }
    } else {
        it->second = userValue;
    }
}

template <typename K, typename N, typename UK, typename UV>
void HeapMapState<K, N, UK, UV>::setCurrentNamespace(N nameSpace)
{
    currentNamespace = nameSpace;
}

template <typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV>* HeapMapState<K, N, UK, UV>::create(
    StateDescriptor* stateDesc,
    StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable,
    TypeSerializer* keySerializer,
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchSideTable)
{
    auto* createdState = new HeapMapState<K, N, UK, UV>(
        stateTable, keySerializer, stateTable->getStateSerializer(), stateTable->getNamespaceSerializer());
    createdState->vectorBatchStateTable = vectorBatchSideTable;
    return createdState;
}

template <typename K, typename N, typename UK, typename UV>
HeapMapState<K, N, UK, UV>* HeapMapState<K, N, UK, UV>::update(
    StateDescriptor* stateDesc,
    StateTable<K, N, emhash7::HashMap<UK, UV>*>* stateTable,
    HeapMapState<K, N, UK, UV>* existingState,
    StateTable<uint32_t, VoidNamespace, omnistream::VectorBatch*>* vectorBatchSideTable)
{
    existingState->setNamespaceSerializer(stateTable->getNamespaceSerializer());
    existingState->setValueSerializer(stateTable->getStateSerializer());
    existingState->vectorBatchStateTable = vectorBatchSideTable;
    return existingState;
}
