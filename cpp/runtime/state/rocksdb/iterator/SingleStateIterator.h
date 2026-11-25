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

#ifndef OMNISTREAM_SINGLESTATEITERATOR_H
#define OMNISTREAM_SINGLESTATEITERATOR_H

#include <vector>
#include <cstdint>
#include <memory>
#include "core/utils/ByteView.h"

/**
 * An interface for iterating over a single state in a RocksDB state backend.
 *
 * Extended with ByteView key/value access (zero-copy) and keyGroup() to
 * eliminate repeated byte-prefix decoding in the merge iterator comparator.
 */
class SingleStateIterator {
public:
    virtual ~SingleStateIterator() = default;

    virtual void next() = 0;
    virtual bool isValid() const = 0;
    /** Returns a ByteView valid until next(), close(), or destruction. */
    virtual ByteView key() const = 0;
    /** Returns a ByteView valid until next(), close(), or destruction. */
    virtual ByteView value() const = 0;
    /** Returns the keyGroup prefix decoded from the current key. */
    virtual int keyGroup() const = 0;
    virtual int getKvStateId() const = 0;
    virtual size_t getEntryCount() const
    {
        return 0;
    }
    virtual void close() = 0;
};

#endif // OMNISTREAM_SINGLESTATEITERATOR_H
