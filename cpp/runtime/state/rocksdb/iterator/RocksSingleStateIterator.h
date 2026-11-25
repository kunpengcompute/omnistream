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

#ifndef OMNISTREAM_ROCKSSINGLESTATEITERATOR_H
#define OMNISTREAM_ROCKSSINGLESTATEITERATOR_H
#include <stdexcept>
#include "SingleStateIterator.h"
#include "runtime/state/RocksIteratorWrapper.h"
#include "common.h"
#include <iostream>
#include <string>

/**
 * Wraps a RocksDB iterator and assigns an id for the key/value state.
 * Returned key/value views are invalidated by next() or close().
 */
class RocksSingleStateIterator : public SingleStateIterator {
public:
    /**
     * @param iterator Underlying RocksIteratorWrapper (ownership transferred)
     * @param kvStateId Id of the K/V state to which this iterator belongs
     */
    RocksSingleStateIterator(std::unique_ptr<RocksIteratorWrapper> iterator, int kvStateId, int keyGroupPrefixBytes)
        : iterator_(std::move(iterator)),
          kvStateId_(kvStateId),
          keyGroupPrefixBytes_(keyGroupPrefixBytes)
    {
        if (!iterator_) {
            throw std::invalid_argument("RocksIteratorWrapper cannot be null");
        }
        refreshKeyGroup();
    }

    void next() override
    {
        iterator_->next();
        refreshKeyGroup();
    }

    bool isValid() const override
    {
        return iterator_ && iterator_->isValid();
    }

    ByteView key() const override
    {
        return iterator_->keyView();
    }

    ByteView value() const override
    {
        return iterator_->valueView();
    }

    int keyGroup() const override
    {
        return currentKeyGroup_;
    }

    int getKvStateId() const override
    {
        return kvStateId_;
    }

    void close() override
    {
        if (iterator_) {
            iterator_->close();
        }
    }

private:
    std::unique_ptr<RocksIteratorWrapper> iterator_;
    int kvStateId_;
    int keyGroupPrefixBytes_;
    int currentKeyGroup_ = -1;

    // Decodes the key-group big-endian prefix once per position to avoid
    // repeated memcmp in the merge iterator comparator. Duplicated across
    // iterator subclasses (HeapSingleStateIterator, HeapPriorityQueue...
    // PendingSingleStateIterator) because each accesses key bytes through
    // a different member (RocksDB slice, entries_ vector, serializedKeys_).
    void refreshKeyGroup()
    {
        currentKeyGroup_ = -1;
        if (!iterator_ || !iterator_->isValid()) {
            return;
        }
        ByteView key = iterator_->keyView();
        if (key.size() < static_cast<size_t>(keyGroupPrefixBytes_)) {
            return;
        }
        int result = 0;
        for (int i = 0; i < keyGroupPrefixBytes_; ++i) {
            result <<= 8;
            result |= static_cast<int>(key[i] & 0xFF);
        }
        currentKeyGroup_ = result;
    }
};
#endif // OMNISTREAM_ROCKSSINGLESTATEITERATOR_H
