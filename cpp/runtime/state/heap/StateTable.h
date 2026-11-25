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
#include <atomic>
#include <type_traits>
#include <tuple>
#include <limits>
#include <functional> // for std::hash
#include "StateMap.h"
#include "core/typeutils/TypeSerializer.h"
#include "../StateTransformationFunction.h"
#include "../internal/InternalKvState.h"
#include "../InternalKeyContext.h"
#include "../KeyGroupRange.h"
#include "../RegisteredKeyValueStateBackendMetaInfo.h"
#include "data/util/SequenceNumberHelper.h"
#include "runtime/state/StateSizeUtil.h"
#include "table/data/binary/BinaryRowData.h"
/* S is the value used in the State,
 * like RowData* for HeapValueState,
 * emhash7<RowData*, int>* for HeapMapState,
 * vector<int64_t>* for List State
 */
template <typename K, typename N, typename S>
class StateTable {
public:
    StateTable(
        InternalKeyContext<K>* keyContext,
        RegisteredKeyValueStateBackendMetaInfo* metaInfo,
        TypeSerializer* keySerializer);

    virtual ~StateTable();

    // running total of inner-container elements across all (key, namespace)
    // entries of THIS state table (only meaningful for container values: MAP/LIST/SET_LONG).
    // Maintained incrementally on the task thread at every Heap{Map,List,Value}State element-mutation
    // site. CORRECTION 13: now std::atomic because the metric-reporter thread reads it directly (the
    // data-size metric is pulled on demand from the SizeGauge supplier). The task thread is the only
    // writer; existing += / -= become atomic fetch_add/sub. Stays 0 for fixed-width VALUE state.
    std::atomic<int64_t> liveNumElements_{0};

    // CORRECTION 13: per-state widths sampled ONCE on the task thread (from the first non-empty entry)
    // and cached here so the metric-reporter thread can convert counts -> bytes WITHOUT ever touching
    // a CopyOnWriteStateMap entry (which would race a concurrent rehash's free(_pairs)). Filled lazily
    // by refreshSampledWidthsIfNeeded() at the mutation sites. Container states use
    // cachedElementWidth_/cachedContainerOverhead_/cachedKeyWidth_; fixed-width VALUE states use
    // cachedKeyWidth_/cachedValueWidth_.
    std::atomic<int64_t> cachedElementWidth_{0};
    std::atomic<int64_t> cachedContainerOverhead_{0};
    std::atomic<int64_t> cachedKeyWidth_{0};
    std::atomic<int64_t> cachedValueWidth_{0};

    // CORRECTION 13: task-thread-only width sampler. No-op once the relevant width is cached. Samples
    // ONE live entry (first non-empty key-group map) and stores the representative widths. Called from
    // the Heap*State mutation sites (synchronously on the task thread, so it never races a rehash).
    void refreshSampledWidthsIfNeeded()
    {
        if constexpr (StateSizeUtil::is_container_value<S>::value) {
            if (cachedElementWidth_.load(std::memory_order_relaxed) != 0) {
                return; // already sampled
            }
        } else {
            if (cachedValueWidth_.load(std::memory_order_relaxed) != 0) {
                return; // already sampled
            }
        }
        for (auto* stateMap : keyGroupedStateMaps) {
            if (stateMap == nullptr || stateMap->size() == 0) {
                continue;
            }
            K sampleKey{};
            S sampleValue{};
            if (stateMap->sampleFirstEntry(sampleKey, sampleValue)) {
                if constexpr (StateSizeUtil::is_container_value<S>::value) {
                    cachedElementWidth_.store(
                        StateSizeUtil::sampleElementWidth(sampleValue), std::memory_order_relaxed);
                    cachedContainerOverhead_.store(
                        StateSizeUtil::containerOverhead(sampleValue), std::memory_order_relaxed);
                    cachedKeyWidth_.store(StateSizeUtil::sizeInBytes(sampleKey), std::memory_order_relaxed);
                } else {
                    cachedKeyWidth_.store(StateSizeUtil::sizeInBytes(sampleKey), std::memory_order_relaxed);
                    cachedValueWidth_.store(StateSizeUtil::sizeInBytes(sampleValue), std::memory_order_relaxed);
                }
            }
            break;
        }
    }

    bool isEmpty()
    {
        return size() == 0;
    };

    int size();

    S get(const N& nameSpace)
    {
        return get(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    bool containsKey(const N& nameSpace)
    {
        return containsKey(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    void put(const N& nameSpace, const S& state)
    {
        put(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace, state);
    };

    void put(const K& key, int keyGroup, const N& nameSpace, const S& state)
    {
        getMapForKeyGroup(keyGroup)->put(key, nameSpace, state);
    }

    K putAndRemoveDuplicateKey(const N& nameSpace, const S& state)
    {
        return getMapForKeyGroup(keyContext->getCurrentKeyGroupIndex())
            ->putAndRemoveDuplicateKey(keyContext->getCurrentKey(), nameSpace, state);
    }

    void remove(const N& nameSpace)
    {
        remove(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    }

    void remove(const K& key, int keyGroupIndex, const N& nameSpace)
    {
        getMapForKeyGroup(keyGroupIndex)->remove(key, nameSpace);
    }

    S removeAndGetOld(const N& nameSpace)
    {
        return removeAndGetOld(keyContext->getCurrentKey(), keyContext->getCurrentKeyGroupIndex(), nameSpace);
    };

    template <typename T>
    void transform(const N& nameSpace, T value, StateTransformationFunction<S, T> transformation);

    S get(const K& key, const N& nameSpace);

    S get(const K& key, int keyGroupIndex, const N& nameSpace);

    typename InternalKvState<K, N, S>::StateIncrementalVisitor* getStateIncrementalVisitor(
        int recommendedMaxNumberOfReturnedRecords);

    RegisteredKeyValueStateBackendMetaInfo* getMetaInfo()
    {
        return metaInfo;
    }

    KeyGroupRange* getKeyGroupRange()
    {
        return keyGroupRange;
    }

    int getNumberOfKeyGroups()
    {
        return keyContext->getNumberOfKeyGroups();
    }

    void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo* newMetaInfo)
    {
        metaInfo = newMetaInfo;
    }

    void copyCurrentKey()
    {
        if constexpr (std::is_same_v<K, RowData*>) {
            auto currentKey = static_cast<BinaryRowData*>(keyContext->getCurrentKey());
            if (!currentKey) {
                INFO_RELEASE("current key is null");
                throw std::runtime_error("current key is null");
            }
            auto newKey = currentKey->copy();
            keyContext->setCurrentKey(newKey);
        } else if constexpr (std::is_pointer_v<K>) {
            NOT_IMPL_EXCEPTION;
        } else {
            return;
        }
    }

    // Not implemented
    std::vector<K>* getKeys(const N& nameSpace);

    std::vector<std::tuple<K, N>>* getKeysAndNamespace();

    // Access to maps
    std::vector<StateMap<K, N, S>*>* getState()
    {
        return &keyGroupedStateMaps;
    }

    int getKeyGroupOffset()
    {
        return keyGroupRange->getStartKeyGroup();
    }

    StateMap<K, N, S>* getMapForKeyGroup(int keyGroupIndex);

    // fixed-width VALUE state size, REPORTER-THREAD SAFE. numKeys comes from size()
    //  (sums each key-group map's _num_filled -- plain integer reads, benign race, never a _pairs
    //  deref); the per-key and per-value widths come from the task-sampled cache atomics. No
    //  CopyOnWriteStateMap entry is touched here, so this may run on the metric-reporter thread.
    int64_t fixedValueDataSize()
    {
        int64_t numKeys = static_cast<int64_t>(size());
        int64_t keyWidth = cachedKeyWidth_.load(std::memory_order_relaxed);
        int64_t valueWidth = cachedValueWidth_.load(std::memory_order_relaxed);
        return numKeys * (keyWidth + valueWidth);
    }

    // incremental container-state size, REPORTER-THREAD SAFE and with NO per-entry
    // walk. numElements is the running counter liveNumElements_ (maintained at every element-mutation
    // site); numKeys comes from size() (O(number of key-groups), benign integer reads). The element
    // width, container overhead and key width come from the task-sampled cache atomics (filled by
    // refreshSampledWidthsIfNeeded()). No CopyOnWriteStateMap entry is touched, so this may run on the
    // metric-reporter thread. Used for MAP/LIST/SET_LONG states.
    int64_t incrementalDataSize()
    {
        int64_t numKeys = static_cast<int64_t>(size());
        int64_t numElements = liveNumElements_.load(std::memory_order_relaxed);
        int64_t elementWidth = cachedElementWidth_.load(std::memory_order_relaxed);
        int64_t perContainerOverhead = cachedContainerOverhead_.load(std::memory_order_relaxed);
        int64_t keyWidth = cachedKeyWidth_.load(std::memory_order_relaxed);
        return numElements * elementWidth + numKeys * (perContainerOverhead + keyWidth);
    }

    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    }

    TypeSerializer* getStateSerializer()
    {
        return metaInfo->getStateSerializer();
    }

    TypeSerializer* getNamespaceSerializer()
    {
        return metaInfo->getNamespaceSerializer();
    }

    void deleteMaps()
    {
        for (int index = keyGroupRange->getStartKeyGroup(); index <= keyGroupRange->getEndKeyGroup(); index++) {
            int pos = indexToOffset(index);
            if (pos >= 0 && pos < static_cast<int>(keyGroupedStateMaps.size())) {
                delete keyGroupedStateMaps[pos];
                keyGroupedStateMaps[pos] = nullptr;
            }
        }
    };

    InternalKeyContext<K>* getKeyContext()
    {
        return keyContext;
    }

    uint32_t getNextSequenceNumber(int32_t keyGroup);
    void addNextSequenceNumber(int32_t keyGroup);
    void updateNextSequenceNumber(int32_t keyGroup, uint32_t nextSequenceNumber);

    class StateEntryIterator : public InternalKvState<K, N, S>::StateIncrementalVisitor {
    public:
        S nextEntries() override;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords, StateTable<K, N, S>* table);

        bool hasNext() override;

        // void remove(const K &key, const N &nameSpace, const S &state) override;

        // void update(const K &key, const N &nameSpace, const S &state, const S &newState) override;
    private:
        int recommendedMaxNumberOfReturnedRecords;
        int keyGroupIndex;
        typename InternalKvState<K, N, S>::StateIncrementalVisitor* stateIncrementalVisitor;
        StateTable<K, N, S>* table;

        void init();
    };

protected:
    // Variables
    InternalKeyContext<K>* keyContext;
    TypeSerializer* keySerializer;
    KeyGroupRange* keyGroupRange;
    std::vector<StateMap<K, N, S>*> keyGroupedStateMaps;
    RegisteredKeyValueStateBackendMetaInfo* metaInfo;
    omnistream::SequenceNumberHelper sequenceNumberHelper_{}; // only used for VectorBatch storage

    // Abstract Functions
    virtual StateMap<K, N, S>* createStateMap() = 0;

    bool containsKey(const K& key, int keyGroupIndex, const N& nameSpace)
    {
        return getMapForKeyGroup(keyGroupIndex)->containsKey(key, nameSpace);
    };

    S removeAndGetOld(const K& key, int keyGroupIndex, const N& nameSpace)
    {
        return getMapForKeyGroup(keyGroupIndex)->removeAndGetOld(key, nameSpace);
    };

    // Access to maps
    int indexToOffset(int index)
    {
        return index - getKeyGroupOffset();
    };

    virtual void initialize() = 0;
};

template <typename K, typename N, typename S>
StateTable<K, N, S>::StateTable(
    InternalKeyContext<K>* keyContext, RegisteredKeyValueStateBackendMetaInfo* metaInfo, TypeSerializer* keySerializer)
{
    this->keyContext = keyContext;
    this->metaInfo = metaInfo;
    this->keySerializer = keySerializer;
    this->keyGroupRange = keyContext->getKeyGroupRange();
    keyGroupedStateMaps = {};
    sequenceNumberHelper_ = omnistream::SequenceNumberHelper(this->keyContext->getNumberOfKeyGroups());
}

template <typename K, typename N, typename S>
StateTable<K, N, S>::~StateTable()
{
    delete metaInfo;
    for (auto stateMapPtr : keyGroupedStateMaps) {
        delete stateMapPtr;
    }
}

template <typename K, typename N, typename S>
int StateTable<K, N, S>::size()
{
    int count = 0;
    for (int i = 0; i < keyGroupedStateMaps.size(); i++) {
        if (keyGroupedStateMaps[i] != nullptr) {
            count += keyGroupedStateMaps[i]->size();
        }
    }
    return count;
}

template <typename K, typename N, typename S>
S StateTable<K, N, S>::get(const K& key, const N& nameSpace)
{
    // Key class must have a hash function
    std::hash<K> keyHash;
    int keyGroup = keyHash(key) % keyContext->getNumberOfKeyGroups();
    return get(key, keyGroup, nameSpace);
}

// Internal interactions with cowMap
template <typename K, typename N, typename S>
S StateTable<K, N, S>::get(const K& key, int keyGroupIndex, const N& nameSpace)
{
    StateMap<K, N, S>* stateMap = getMapForKeyGroup(keyGroupIndex);
    if (stateMap == nullptr) {
        if constexpr (std::is_pointer_v<S>) {
            return nullptr;
        } else {
            return std::numeric_limits<S>::max();
        }
    }
    return stateMap->get(key, nameSpace);
};

template <typename K, typename N, typename S>
std::vector<K>* StateTable<K, N, S>::getKeys(const N& nameSpace)
{
    return nullptr;
}

template <typename K, typename N, typename S>
std::vector<std::tuple<K, N>>* StateTable<K, N, S>::getKeysAndNamespace()
{
    return nullptr;
}

template <typename K, typename N, typename S>
typename InternalKvState<K, N, S>::StateIncrementalVisitor* StateTable<K, N, S>::getStateIncrementalVisitor(
    int recommendedMaxNumberOfReturnedRecords)
{
    return new typename StateTable<K, N, S>::StateEntryIterator(recommendedMaxNumberOfReturnedRecords, this);
}

template <typename K, typename N, typename S>
StateMap<K, N, S>* StateTable<K, N, S>::getMapForKeyGroup(int keyGroupIndex)
{
    const int pos = indexToOffset(keyGroupIndex);
    if (keyContext == nullptr || keyContext->getKeyGroupRange() == nullptr) {
        throw std::logic_error("Key context or key group range is null");
    }
    if (pos >= 0 && pos < keyContext->getKeyGroupRange()->getNumberOfKeyGroups()) {
        return keyGroupedStateMaps[pos];
    } else {
        throw std::logic_error("Key group is not in key group range");
    }
}

template <typename K, typename N, typename S>
StateTable<K, N, S>::StateEntryIterator::StateEntryIterator(
    int recommendedMaxNumberOfReturnedRecords, StateTable<K, N, S>* table)
{
    this->table = table;
    this->recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
    this->keyGroupIndex = 0;
    init();
}

// This function move the stateIncremental visitor to the first keyGroup that has value in it.
template <typename K, typename N, typename S>
void StateTable<K, N, S>::StateEntryIterator::init()
{
    while (keyGroupIndex < table->keyContext->getKeyGroupRange()->getNumberOfKeyGroups()) {
        StateMap<K, N, S>* stateMap = table->keyGroupedStateMaps[keyGroupIndex++];
        auto visitor = stateMap->getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
        if (visitor->hasNext()) {
            stateIncrementalVisitor = visitor;
            return;
        }
    }
}
// This function checks if next keyGroup exist and go to it
template <typename K, typename N, typename S>
bool StateTable<K, N, S>::StateEntryIterator::hasNext()
{
    while (stateIncrementalVisitor == nullptr || !stateIncrementalVisitor->hasNext()) {
        if (keyGroupIndex == table->keyContext->getKeyGroupRange()->getNumberOfKeyGroups()) {
            return false;
        }
        auto visitor = table->keyGroupedStateMaps[keyGroupIndex++]->getStateIncrementalVisitor(
            recommendedMaxNumberOfReturnedRecords);
        if (visitor->hasNext()) {
            stateIncrementalVisitor = visitor;
            break;
        }
    }
    return true;
}

template <typename K, typename N, typename S>
S StateTable<K, N, S>::StateEntryIterator::nextEntries()
{
    if (!hasNext()) {
        if constexpr (std::is_pointer_v<S>) {
            return nullptr;
        } else {
            return std::numeric_limits<S>::max();
        }
    }
    return stateIncrementalVisitor->nextEntries();
}

template <typename K, typename N, typename S>
template <typename T>
void StateTable<K, N, S>::transform(const N& nameSpace, T value, StateTransformationFunction<S, T> transformation)
{
    K key = keyContext->getCurrentKey();
    int keyGroup = keyContext->getCurrentKeyGroupIndex();
    getMapForKeyGroup(keyGroup).transform(key, nameSpace, value, transformation);
}

template <typename K, typename N, typename S>
uint32_t StateTable<K, N, S>::getNextSequenceNumber(int32_t keyGroup)
{
    return sequenceNumberHelper_.getNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename S>
void StateTable<K, N, S>::addNextSequenceNumber(int32_t keyGroup)
{
    sequenceNumberHelper_.addNextSequenceNumber(keyGroup);
}

template <typename K, typename N, typename S>
void StateTable<K, N, S>::updateNextSequenceNumber(int32_t keyGroup, uint32_t nextSequenceNumber)
{
    sequenceNumberHelper_.updateNextSequenceNumber(keyGroup, nextSequenceNumber);
}
