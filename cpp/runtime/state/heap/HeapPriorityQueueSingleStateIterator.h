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

#ifndef OMNISTREAM_HEAPPRIORITYQUEUESINGLESTATEITERATOR_H
#define OMNISTREAM_HEAPPRIORITYQUEUESINGLESTATEITERATOR_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

#include "basictypes/Object.h"
#include "common.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "runtime/state/KeyGroupRangeAssignment.h"
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"
#include "runtime/state/heap/HeapPriorityQueueSet.h"
#include "core/utils/type_traits_ext.h"

/**
 * Sync-phase materialized iterator for Heap priority queue state.
 *
 * Flink's heap backend writes PRIORITY_QUEUE state through the same full snapshot
 * key/value stream as normal keyed state. Each PQ element is encoded as:
 *   key   = [key-group-prefix] + [serialized priority queue element]
 *   value = []
 *
 * The live queue is copied and serialized during construction, so async snapshot
 * writing does not touch mutable timer/PQ data structures.
 */
template <typename K, typename T, typename Comparator>
class HeapPriorityQueueSingleStateIterator : public SingleStateIterator {
public:
    static_assert(is_shared_ptr_v<T>, "T should be shared ptr.");

    HeapPriorityQueueSingleStateIterator(
        HeapPriorityQueueSet<K, T, Comparator>* priorityQueue,
        TypeSerializer* elementSerializer,
        int kvStateId,
        int keyGroupPrefixBytes,
        int totalNumberOfKeyGroups)
        : elementSerializer_(elementSerializer),
          kvStateId_(kvStateId),
          keyGroupPrefixBytes_(keyGroupPrefixBytes),
          totalNumberOfKeyGroups_(totalNumberOfKeyGroups)
    {
        collectAndSerializeEntries(priorityQueue);
        currentIndex_ = 0;
        valid_ = !entries_.empty();
        refreshKeyGroup();
        INFO_RELEASE(
            "HeapPriorityQueueSingleStateIterator: kvStateId=" << kvStateId_ << ", entryCount=" << entries_.size()
                                                               << ", valid=" << valid_);
    }

    void next() override
    {
        if (valid_) {
            currentIndex_++;
            valid_ = currentIndex_ < entries_.size();
            refreshKeyGroup();
        }
    }

    bool isValid() const override
    {
        return valid_;
    }

    ByteView key() const override
    {
        const auto& key = entries_[currentIndex_].serializedKey;
        return ByteView::fromBuffer(key.data(), key.size());
    }

    ByteView value() const override
    {
        return {};
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
        return entries_.size();
    }

    void close() override
    {
        entries_.clear();
        valid_ = false;
    }

private:
    using ElementType = typename T::element_type;

    struct SerializedEntry {
        std::vector<int8_t> serializedKey;
    };

    TypeSerializer* elementSerializer_;
    int kvStateId_;
    int keyGroupPrefixBytes_;
    int totalNumberOfKeyGroups_;
    std::vector<SerializedEntry> entries_;
    size_t currentIndex_ = 0;
    int currentKeyGroup_ = -1;
    bool valid_ = false;

    // See RocksSingleStateIterator::refreshKeyGroup() for rationale.
    void refreshKeyGroup()
    {
        currentKeyGroup_ = -1;
        if (!valid_ || currentIndex_ >= entries_.size()) {
            return;
        }
        const auto& key = entries_[currentIndex_].serializedKey;
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

    void collectAndSerializeEntries(HeapPriorityQueueSet<K, T, Comparator>* priorityQueue)
    {
        if (priorityQueue == nullptr) {
            return;
        }

        // Copy the live heap array first. This is the synchronous freeze point,
        // analogous to Flink's HeapPriorityQueueStateSnapshot.
        std::vector<T> snapshot = priorityQueue->toArray();
        entries_.reserve(snapshot.size());

        int entryIndex = 0;
        for (const auto& element : snapshot) {
            if (!element) {
                continue;
            }

            const auto& key = element->getKey();
            int keyGroup = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, totalNumberOfKeyGroups_);

            SerializedEntry entry;
            try {
                entry.serializedKey = serializeKey(keyGroup, element);
            } catch (const std::exception& e) {
                INFO_RELEASE(
                    "Error: HeapPriorityQueueSingleStateIterator: serialize EXCEPTION at keyGroup="
                    << keyGroup << ", entryIndex=" << entryIndex << ", error=" << e.what());
                throw;
            }

            entries_.push_back(std::move(entry));
            entryIndex++;
        }

        // Sort by key-group prefix so RocksStatesPerKeyGroupMergeIterator can
        // merge this PQ iterator with KV iterators correctly.
        std::sort(entries_.begin(), entries_.end(), [this](const SerializedEntry& a, const SerializedEntry& b) -> bool {
            const int len = std::min(
                {keyGroupPrefixBytes_,
                 static_cast<int>(a.serializedKey.size()),
                 static_cast<int>(b.serializedKey.size())});
            for (int i = 0; i < len; i++) {
                auto av = static_cast<uint8_t>(a.serializedKey[i]);
                auto bv = static_cast<uint8_t>(b.serializedKey[i]);
                if (av != bv) {
                    return av < bv;
                }
            }
            return false;
        });
    }

    std::vector<int8_t> serializeKey(int keyGroup, const T& element)
    {
        DataOutputSerializer outputSerializer;
        OutputBufferStatus outputBufferStatus;
        outputSerializer.setBackendBuffer(&outputBufferStatus);

        if (keyGroupPrefixBytes_ == 1) {
            outputSerializer.writeByte(static_cast<uint32_t>(keyGroup));
        } else {
            outputSerializer.writeByte(static_cast<uint32_t>((keyGroup >> 8) & 0xFF));
            outputSerializer.writeByte(static_cast<uint32_t>(keyGroup & 0xFF));
        }

        serializeElement(element, outputSerializer);

        auto* data = outputSerializer.getData();
        size_t len = outputSerializer.length();
        std::vector<int8_t> result(len);
        for (size_t i = 0; i < len; i++) {
            result[i] = static_cast<int8_t>(data[i]);
        }
        return result;
    }

    void serializeElement(const T& element, DataOutputSerializer& outputSerializer)
    {
        if (elementSerializer_ == nullptr) {
            INFO_RELEASE("Error: serializeElement Priority queue element serializer is null");
            THROW_LOGIC_EXCEPTION("Priority queue element serializer is null");
        }

        if constexpr (std::is_base_of_v<Object, ElementType>) {
            elementSerializer_->serialize(static_cast<Object*>(element.get()), outputSerializer);
        } else {
            elementSerializer_->serialize(static_cast<void*>(element.get()), outputSerializer);
        }
    }
};

#endif // OMNISTREAM_HEAPPRIORITYQUEUESINGLESTATEITERATOR_H
