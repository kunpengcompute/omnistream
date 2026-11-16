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
#ifndef OMNISTREAM_HEAPSINGLESTATEITERATOR_H
#define OMNISTREAM_HEAPSINGLESTATEITERATOR_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/MapSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "table/typeutils/SortedVectorLong.h"
#include "basictypes/Object.h"
#include "StateTable.h"
#include "CopyOnWriteStateMap.h"
#include "../../../core/include/common.h"

// Type traits to detect emhash7::HashMap and std::vector pointer types
template <typename T>
struct IsEmhashMapPtr : std::false_type {};
template <typename UK, typename UV>
struct IsEmhashMapPtr<emhash7::HashMap<UK, UV>*> : std::true_type {};

template <typename T>
struct IsVectorPtr : std::false_type {};
template <typename V>
struct IsVectorPtr<std::vector<V>*> : std::true_type {};

/**
 * A SingleStateIterator that iterates over a Heap CopyOnWriteStateTable,
 * serializing each entry into byte arrays compatible with the RocksDB key format:
 *   key = [keyGroupPrefix] + [serialized key] + [serialized namespace]
 *   value = [serialized state value]
 *
 * Entries are materialized eagerly during construction and iterated in
 * key-group order (ascending) so that RocksStatesPerKeyGroupMergeIterator can
 * merge them correctly without touching live state in the async phase.
 */
template <typename K, typename N, typename S>
class HeapSingleStateIterator : public SingleStateIterator {
public:
    struct SerializedEntry {
        std::vector<int8_t> serializedKey;
        std::vector<int8_t> serializedValue;
    };

    HeapSingleStateIterator(StateTable<K, N, S>* stateTable, int kvStateId, int keyGroupPrefixBytes)
        : stateTable_(stateTable),
          kvStateId_(kvStateId),
          keyGroupPrefixBytes_(keyGroupPrefixBytes)
    {
        collectAndSerializeEntries();
        currentIndex_ = 0;
        valid_ = !entries().empty();
        refreshKeyGroup();
    }

    void next() override
    {
        if (valid_) {
            currentIndex_++;
            valid_ = (currentIndex_ < entries().size());
            refreshKeyGroup();
        }
    }

    bool isValid() const override
    {
        return valid_;
    }

    ByteView key() const override
    {
        const auto& key = entries()[currentIndex_].serializedKey;
        return ByteView::fromBuffer(key.data(), key.size());
    }

    ByteView value() const override
    {
        const auto& value = entries()[currentIndex_].serializedValue;
        return ByteView::fromBuffer(value.data(), value.size());
    }

    int keyGroup() const override
    {
        return currentKeyGroup_;
    }

    int getKvStateId() const override
    {
        return kvStateId_;
    }

    size_t getEntryCount() const override
    {
        return entries().size();
    }

    void close() override
    {
        entries_.clear();
        currentIndex_ = 0;
        currentKeyGroup_ = -1;
        valid_ = false;
    }

    /**
     * Serializes a std::vector in ListDelimitedSerializer format (consistent with RocksDB).
     * Format: [elem_1][','][elem_2][',']...[elem_N]  (no size prefix, comma-separated)
     */
    template <typename V>
    static void serializeVector(const std::vector<V>& vec, TypeSerializer* elemSer, DataOutputSerializer& out)
    {
        for (size_t i = 0; i < vec.size(); ++i) {
            if (i > 0) {
                out.writeByte(static_cast<uint8_t>(','));
            }
            if constexpr (std::is_pointer_v<V>) {
                elemSer->serialize(const_cast<V>(vec[i]), out);
            } else {
                V me = vec[i];
                elemSer->serialize(&me, out);
            }
        }
    }

private:
    StateTable<K, N, S>* stateTable_;
    int kvStateId_;
    int keyGroupPrefixBytes_;
    std::vector<SerializedEntry> entries_;
    size_t currentIndex_ = 0;
    int currentKeyGroup_ = -1;
    bool valid_ = false;

    const std::vector<SerializedEntry>& entries() const
    {
        return entries_;
    }

    std::vector<SerializedEntry>& entries()
    {
        return entries_;
    }

    void addSnapshotEntry(SerializedEntry entry)
    {
        entries_.push_back(std::move(entry));
    }

    // See RocksSingleStateIterator::refreshKeyGroup() for rationale.
    void refreshKeyGroup()
    {
        currentKeyGroup_ = -1;
        if (!valid_ || currentIndex_ >= entries().size()) {
            return;
        }
        const auto& key = entries()[currentIndex_].serializedKey;
        if (key.size() < static_cast<size_t>(keyGroupPrefixBytes_)) {
            return;
        }
        int result = 0;
        for (int i = 0; i < keyGroupPrefixBytes_; ++i) {
            result <<= 8;
            result |= static_cast<int>(static_cast<uint8_t>(key[i]));
        }
        currentKeyGroup_ = result;
    }

    void collectAndSerializeEntries()
    {
        auto* stateMaps = stateTable_->getState();
        int keyGroupOffset = stateTable_->getKeyGroupOffset();
        TypeSerializer* keySerializer = stateTable_->getKeySerializer();
        TypeSerializer* namespaceSerializer = stateTable_->getNamespaceSerializer();
        TypeSerializer* stateSerializer = stateTable_->getStateSerializer();

        for (size_t i = 0; i < stateMaps->size(); i++) {
            int keyGroup = keyGroupOffset + static_cast<int>(i);
            auto* stateMap = (*stateMaps)[i];
            if (stateMap == nullptr || stateMap->size() == 0) {
                continue;
            }

            serializeStateMap(stateMap, keyGroup, keySerializer, namespaceSerializer, stateSerializer);
        }

        // Sort by keyGroupPrefix bytes (ascending) to match MergeIterator expectation
        sortSnapshotEntries();
    }

    void sortSnapshotEntries()
    {
        auto& entries = this->entries();
        std::stable_sort(
            entries.begin(), entries.end(), [this](const SerializedEntry& a, const SerializedEntry& b) -> bool {
                for (int i = 0; i < keyGroupPrefixBytes_ && i < static_cast<int>(a.serializedKey.size()) &&
                                i < static_cast<int>(b.serializedKey.size());
                     i++) {
                    if (static_cast<uint8_t>(a.serializedKey[i]) != static_cast<uint8_t>(b.serializedKey[i])) {
                        return static_cast<uint8_t>(a.serializedKey[i]) < static_cast<uint8_t>(b.serializedKey[i]);
                    }
                }
                return false;
            });
    }

    // Snapshot entry holding raw (unserialized) copies of key, namespace, and value.
    // Taking a snapshot of all entries BEFORE serializing avoids iterator invalidation
    // if the underlying CopyOnWriteStateMap is rehashed (e.g. by a concurrent put()).
    struct RawSnapshotEntry {
        RawSnapshotEntry(const K& snapshotKey, const N& snapshotNamespace, const S& snapshotValue)
            : key(snapshotKey),
              nmspace(snapshotNamespace),
              value(snapshotValue),
              ownsRefs_(true)
        {
            retainObjectRef(key);
            retainObjectRef(nmspace);
            retainObjectRef(value);
        }

        RawSnapshotEntry(const RawSnapshotEntry&) = delete;
        RawSnapshotEntry& operator=(const RawSnapshotEntry&) = delete;

        RawSnapshotEntry(RawSnapshotEntry&& other) noexcept
            : key(other.key),
              nmspace(other.nmspace),
              value(other.value),
              ownsRefs_(other.ownsRefs_)
        {
            other.ownsRefs_ = false;
        }

        RawSnapshotEntry& operator=(RawSnapshotEntry&& other) noexcept
        {
            if (this != &other) {
                releaseRefs();
                key = other.key;
                nmspace = other.nmspace;
                value = other.value;
                ownsRefs_ = other.ownsRefs_;
                other.ownsRefs_ = false;
            }
            return *this;
        }

        ~RawSnapshotEntry()
        {
            releaseRefs();
        }

        K key;
        N nmspace;
        S value;

    private:
        template <typename T>
        static void retainObjectRef(const T& ptr)
        {
            if constexpr (std::is_same_v<std::decay_t<T>, Object*>) {
                if (ptr != nullptr) {
                    ptr->getRefCount();
                }
            }
        }

        template <typename T>
        static void releaseObjectRef(const T& ptr)
        {
            if constexpr (std::is_same_v<std::decay_t<T>, Object*>) {
                if (ptr != nullptr) {
                    ptr->putRefCount();
                }
            }
        }

        void releaseRefs()
        {
            if (!ownsRefs_) {
                return;
            }
            releaseObjectRef(key);
            releaseObjectRef(nmspace);
            releaseObjectRef(value);
            ownsRefs_ = false;
        }

        bool ownsRefs_;
    };

    void serializeStateMap(
        StateMap<K, N, S>* stateMap,
        int keyGroup,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer,
        TypeSerializer* stateSerializer)
    {
        // Use StateMap's CopyOnWriteStateMap iterator to traverse entries
        auto* cowMap = dynamic_cast<omnistream::CopyOnWriteStateMap<K, N, S>*>(stateMap);
        if (cowMap == nullptr) {
            return;
        }

        // Phase 1: snapshot all entries into a local vector to decouple from
        // the live hash-table layout. This prevents use-after-free if rehash
        // reallocates _pairs/_bitmask while we are still serializing.
        std::vector<RawSnapshotEntry> snapshot;
        snapshot.reserve(cowMap->size());
        for (auto it = cowMap->begin(); it != cowMap->end(); ++it) {
            snapshot.emplace_back(it->first, it->third, it->second);
        }

        // Phase 2: serialize from the stable local snapshot
        int mapEntryCount = 0;
        for (auto& raw : snapshot) {
            if constexpr (IsEmhashMapPtr<S>::value) {
                // MAP state: output each map entry as a separate state entry
                // Format matches RocksDB per-entry layout
                serializeMapEntriesPerEntry(
                    raw.key,
                    raw.nmspace,
                    raw.value,
                    keyGroup,
                    keySerializer,
                    namespaceSerializer,
                    stateSerializer,
                    mapEntryCount);
            } else {
                SerializedEntry entry;
                try {
                    entry.serializedKey =
                        serializeKey(keyGroup, raw.key, raw.nmspace, keySerializer, namespaceSerializer);
                    entry.serializedValue = serializeValue(raw.value, stateSerializer);
                } catch (const std::exception& e) {
                    INFO_RELEASE(
                        "Error:HeapSingleStateIterator: serializeStateMap EXCEPTION at keyGroup="
                        << keyGroup << ", entryIndex=" << mapEntryCount << ", error=" << e.what());
                    throw;
                }
                addSnapshotEntry(std::move(entry));
            }
            mapEntryCount++;
        }
    }

    std::vector<int8_t> serializeKey(
        int keyGroup,
        const K& key,
        const N& nmspace,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer)
    {
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        // Write key group prefix (same format as RocksdbStateTable::GetKeyNameSpaceSlice)
        if (keyGroupPrefixBytes_ == 1) {
            outputSerializer.writeByte(static_cast<uint32_t>(keyGroup));
        } else {
            outputSerializer.writeByte(static_cast<uint32_t>((keyGroup >> 8) & 0xFF));
            outputSerializer.writeByte(static_cast<uint32_t>(keyGroup & 0xFF));
        }

        // Serialize key
        if constexpr (std::is_pointer_v<K>) {
            keySerializer->serialize(const_cast<K>(key), outputSerializer);
        } else if constexpr (is_shared_ptr_v<K>) {
            if (!key) {
                THROW_LOGIC_EXCEPTION("Heap snapshot cannot serialize a null shared_ptr key");
            }
            keySerializer->serialize(key.get(), outputSerializer);
        } else {
            K mutableKey = key;
            keySerializer->serialize(&mutableKey, outputSerializer);
        }

        // Serialize namespace
        if constexpr (std::is_pointer_v<N>) {
            namespaceSerializer->serialize(const_cast<N>(nmspace), outputSerializer);
        } else if constexpr (is_shared_ptr_v<N>) {
            if (!nmspace) {
                THROW_LOGIC_EXCEPTION("Heap snapshot cannot serialize a null shared_ptr key");
            }
            namespaceSerializer->serialize(nmspace.get(), outputSerializer);
        } else {
            N mutableNs = nmspace;
            namespaceSerializer->serialize(&mutableNs, outputSerializer);
        }

        auto* data = outputSerializer.getData();
        size_t len = outputSerializer.length();
        std::vector<int8_t> result(len);
        for (size_t i = 0; i < len; i++) {
            result[i] = static_cast<int8_t>(data[i]);
        }
        return result;
    }

    /**
     * Serializes a MAP state by outputting each map entry as a separate state entry.
     * This matches the RocksDB per-entry format used by the restore path (writeMapEntry).
     *
     * Key format: [keyGroupPrefix][sourceKey][namespace][mapKey]
     * Value format: [null_bool][mapValue]
     */
    template <typename K2, typename N2, typename UK, typename UV>
    void serializeMapEntriesPerEntry(
        const K2& sourceKey,
        const N2& nmspace,
        const emhash7::HashMap<UK, UV>* mapPtr,
        int keyGroup,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer,
        TypeSerializer* stateSerializer,
        int entryIndex)
    {
        if (mapPtr == nullptr) {
            return;
        }
        auto* mapSer = dynamic_cast<MapSerializer*>(stateSerializer);
        if (mapSer == nullptr) {
            INFO_RELEASE(
                "Error:HeapSingleStateIterator: MAP state serializer is not MapSerializer at entry=" << entryIndex);
            throw std::runtime_error("HeapSingleStateIterator: MAP state serializer is not MapSerializer");
        }
        TypeSerializer* mapKeySer = mapSer->getKeySerializer();
        TypeSerializer* mapValSer = mapSer->getValueSerializer();

        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        for (const auto& pair : *mapPtr) {
            // Build composite key: [keyGroupPrefix][sourceKey][namespace][mapKey]
            outputSerializer.clear();
            if (keyGroupPrefixBytes_ == 1) {
                outputSerializer.writeByte(static_cast<uint32_t>(keyGroup));
            } else {
                outputSerializer.writeByte(static_cast<uint32_t>((keyGroup >> 8) & 0xFF));
                outputSerializer.writeByte(static_cast<uint32_t>(keyGroup & 0xFF));
            }
            // Serialize source key
            if constexpr (std::is_pointer_v<K2>) {
                keySerializer->serialize(const_cast<K2>(sourceKey), outputSerializer);
            } else if constexpr (is_shared_ptr_v<K2>) {
                keySerializer->serialize(sourceKey.get(), outputSerializer);
            } else {
                K2 mk = sourceKey;
                keySerializer->serialize(&mk, outputSerializer);
            }
            // Serialize namespace
            if constexpr (std::is_pointer_v<N2>) {
                namespaceSerializer->serialize(const_cast<N2>(nmspace), outputSerializer);
            } else if constexpr (is_shared_ptr_v<N2>) {
                namespaceSerializer->serialize(nmspace.get(), outputSerializer);
            } else {
                N2 mn = nmspace;
                namespaceSerializer->serialize(&mn, outputSerializer);
            }
            // Serialize map key
            if constexpr (std::is_same_v<UK, Object*>) {
                mapKeySer->serialize(const_cast<Object*>(pair.first), outputSerializer);
            } else if constexpr (std::is_pointer_v<UK>) {
                mapKeySer->serialize(const_cast<UK>(pair.first), outputSerializer);
            } else if constexpr (is_shared_ptr_v<UK>) {
                // SHARED_ROW_BK map key (e.g. StreamingJoin states): serialize the pointee row.
                if (!pair.first) {
                    THROW_LOGIC_EXCEPTION("Heap snapshot cannot serialize a null shared_ptr map key");
                }
                mapKeySer->serialize(pair.first.get(), outputSerializer);
            } else {
                UK mk = pair.first;
                mapKeySer->serialize(&mk, outputSerializer);
            }
            std::vector<int8_t> keyBytes(
                outputSerializer.getData(), outputSerializer.getData() + outputSerializer.getPosition());

            // Build value: [null_bool][mapValue]
            outputSerializer.clear();
            if constexpr (std::is_pointer_v<UV>) {
                if (pair.second == nullptr) {
                    outputSerializer.writeBoolean(true);
                } else {
                    outputSerializer.writeBoolean(false);
                    if constexpr (std::is_same_v<UV, Object*>) {
                        mapValSer->serialize(const_cast<Object*>(pair.second), outputSerializer);
                    } else {
                        mapValSer->serialize(const_cast<UV>(pair.second), outputSerializer);
                    }
                }
            } else {
                outputSerializer.writeBoolean(false);
                UV mv = pair.second;
                mapValSer->serialize(&mv, outputSerializer);
            }
            std::vector<int8_t> valueBytes(
                outputSerializer.getData(), outputSerializer.getData() + outputSerializer.getPosition());

            SerializedEntry entry;
            entry.serializedKey = std::move(keyBytes);
            entry.serializedValue = std::move(valueBytes);
            addSnapshotEntry(std::move(entry));
        }
    }

    /**
     * SP 格式 value 序列化
     * - LIST: [elem1][','][elem2][',']... (ListDelimitedSerializer)
     * - MAP: 逐条 entry（由 serializeMapEntriesPerEntry 处理，此处不处理 MAP）
     */
    std::vector<int8_t> serializeValue(const S& state, TypeSerializer* stateSerializer)
    {
        OutputBufferStatus outputBufferStatus;
        DataOutputSerializer outputSerializer;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        if constexpr (IsVectorPtr<S>::value) {
            // LIST state: serialize in ListDelimitedSerializer format (comma-separated, no size prefix).
            // This format is consistent with RocksDB's RocksDbStringAppendOperator(',') merge operator.
            //
            // 三种情况：
            // 1. stateSerializer 是 ListSerializer → 用 getElementSerializer() 逐元素序列化
            // 2. stateSerializer 是容器序列化器（如 SortedVectorLong，TopN 场景）
            //    → 直接 serialize 整个 vector（其 serialize 期望 void* 是 vector 指针）
            // 3. stateSerializer 是元素序列化器（如 LongSerializer，WindowJoin 场景
            //    dataId=BIGINT_BK）→ 用 stateSerializer 逐元素序列化
            auto* listSer = dynamic_cast<ListSerializer*>(stateSerializer);
            auto* sortedVecSer = dynamic_cast<SortedVectorLong*>(stateSerializer);
            if (listSer && state != nullptr) {
                serializeVector(*state, listSer->getElementSerializer(), outputSerializer);
            } else if (sortedVecSer != nullptr && state != nullptr) {
                // 容器序列化器：serialize 接收整个 vector 指针
                stateSerializer->serialize(const_cast<S>(state), outputSerializer);
            } else if (state != nullptr) {
                // 元素序列化器（如 LongSerializer）：逐元素序列化
                serializeVector(*state, stateSerializer, outputSerializer);
            } else {
                // state 为 nullptr，写出空 list（0 字节，与 ListDelimitedSerializer 一致）
            }
        } else if constexpr (std::is_pointer_v<S>) {
            stateSerializer->serialize(const_cast<S>(state), outputSerializer);
        } else {
            S mutableState = state;
            stateSerializer->serialize(&mutableState, outputSerializer);
        }

        auto* data = outputSerializer.getData();
        size_t len = outputSerializer.length();
        std::vector<int8_t> result(len);
        for (size_t i = 0; i < len; i++) {
            result[i] = static_cast<int8_t>(data[i]);
        }
        return result;
    }
};

#endif // OMNISTREAM_HEAPSINGLESTATEITERATOR_H
