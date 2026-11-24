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

#ifndef FLINK_TNEL_KEYVALUESTATEITERATOR_H
#define FLINK_TNEL_KEYVALUESTATEITERATOR_H

#include <vector>
#include <cstdint>
#include "core/utils/ByteView.h"

class KeyValueStateIterator {
public:
    /**
     * Hot-loop aggregation: all per-entry fields returned in a single struct
     * to avoid repeated virtual dispatch in FullSnapshotAsyncWriter's savepoint
     * KV write loop.
     */
    struct CurrentEntry {
        ByteView key;
        ByteView value;
        int keyGroup = -1;
        int kvStateId = -1;
        bool newKeyGroup = false;
        bool newKeyValueState = false;
    };

    virtual ~KeyValueStateIterator() = default;

    virtual void next() = 0;

    /** Returns the key-group for the current key. */
    virtual int keyGroup() const = 0;

    // Returned views are only valid until next(), close(), or iterator destruction.
    virtual ByteView key() const = 0;
    virtual ByteView value() const = 0;

    /** Returns the Id of the K/V state to which the current key belongs. */
    virtual int kvStateId() const = 0;

    /** Returns all hot-loop fields for the current entry in one virtual call. */
    virtual const CurrentEntry& current() const = 0;

    virtual bool isNewKeyValueState() const = 0;

    virtual bool isNewKeyGroup() const = 0;

    virtual bool isValid() const = 0;

    virtual void close() = 0;
};

#endif // FLINK_TNEL_KEYVALUESTATEITERATOR_H
