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

#ifndef OMNISTREAM_SAVEPOINTKVSTREAMWRITER_H
#define OMNISTREAM_SAVEPOINTKVSTREAMWRITER_H

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/utils/ByteView.h"
#include "KeyGroupRangeOffsets.h"
#include "CheckpointStateOutputStreamProxy.h"

/**
 * Writes key-group-prefixed key/value entries into a savepoint stream.
 *
 * <p>Supports two write paths:
 * <ul>
 * <li>Patchable: writes directly into the stream buffer with a BytePatch
 *     handle. The first byte of the key can later be patched with
 *     {@code FIRST_BIT_IN_BYTE_MASK} to mark end-of-key-group/state.</li>
 * <li>Buffered: when the entry is too large for the stream chunk, the
 *     key/value are copied into pending vectors and written later via
 *     {@link CheckpointStateOutputStreamProxy#writeKeyValuePair}.</li>
 * </ul>
 *
 * <p>Usage pattern: call {@code writeFirst} for the first entry, then
 * {@code writeNext} for subsequent entries, and finally {@code finish}.
 * Between calls the writer maintains a pending entry that must be finalized
 * before any operation that may flush or reallocate the stream buffer.
 */
class SavepointKvStreamWriter {
public:
    static constexpr int END_OF_KEY_GROUP_MASK = 0xffff;
    static constexpr int FIRST_BIT_IN_BYTE_MASK = 0x80;

    SavepointKvStreamWriter(CheckpointStateOutputStreamProxy& stream, KeyGroupRangeOffsets& keyGroupRangeOffsets)
        : stream_(stream),
          keyGroupRangeOffsets_(keyGroupRangeOffsets)
    {
    }

    void writeFirst(ByteView key, ByteView value, int kvStateId, int keyGroup)
    {
        keyGroupRangeOffsets_.setKeyGroupOffset(keyGroup, stream_.getPos());
        stream_.writeShort(kvStateId);
        stream_.prepareForPatchableKeyValuePair(encodedLength(key, value));
        writePending(key, value);
    }

    void writeNext(ByteView key, ByteView value, int kvStateId, int keyGroup, bool newKeyGroup, bool newKvState)
    {
        finalizePending(newKeyGroup || newKvState);
        if (newKeyGroup) {
            stream_.writeShort(END_OF_KEY_GROUP_MASK);
            keyGroupRangeOffsets_.setKeyGroupOffset(keyGroup, stream_.getPos());
            stream_.writeShort(kvStateId);
        } else if (newKvState) {
            stream_.writeShort(kvStateId);
        }
        stream_.prepareForPatchableKeyValuePair(encodedLength(key, value));
        writePending(key, value);
    }

    void finish()
    {
        if (!pending_.hasPending) {
            return;
        }
        finalizePending(true);
        stream_.writeShort(END_OF_KEY_GROUP_MASK);
    }

private:
    struct PendingEntry {
        bool hasPending = false;
        bool buffered = false;
        CheckpointStateOutputStreamProxy::BytePatch patch;
        std::vector<int8_t> key;
        std::vector<int8_t> value;
    };

    CheckpointStateOutputStreamProxy& stream_;
    KeyGroupRangeOffsets& keyGroupRangeOffsets_;
    PendingEntry pending_;

    static void copyViewToVector(std::vector<int8_t>& target, ByteView source)
    {
        target.resize(source.size());
        if (!source.empty()) {
            std::memcpy(target.data(), source.data(), source.size());
        }
    }

    static void markEndOfKeyGroupOrState(std::vector<int8_t>& key)
    {
        key[0] = static_cast<int8_t>(static_cast<uint8_t>(key[0]) | FIRST_BIT_IN_BYTE_MASK);
    }

    static size_t encodedLength(ByteView key, ByteView value)
    {
        return sizeof(int32_t) + key.size() + sizeof(int32_t) + value.size();
    }

    void writePending(ByteView key, ByteView value)
    {
        if (key.empty()) {
            throw std::runtime_error("Savepoint key/value entry key must not be empty");
        }
        CheckpointStateOutputStreamProxy::BytePatch patch;
        bool wrotePatchable = stream_.tryWritePatchableKeyValuePair(key, value, patch);
        if (wrotePatchable) {
            pending_.patch = patch;
            pending_.buffered = false;
        } else {
            copyViewToVector(pending_.key, key);
            copyViewToVector(pending_.value, value);
            pending_.buffered = true;
            pending_.patch = {};
        }
        pending_.hasPending = true;
    }

    void finalizePending(bool markEnd)
    {
        if (!pending_.hasPending) {
            return;
        }
        if (pending_.buffered) {
            if (markEnd) {
                markEndOfKeyGroupOrState(pending_.key);
            }
            stream_.writeKeyValuePair(pending_.key, pending_.value);
            pending_.key.clear();
            pending_.value.clear();
        } else if (markEnd) {
            stream_.patchByte(pending_.patch, FIRST_BIT_IN_BYTE_MASK);
        }
        if (!pending_.buffered) {
            stream_.releasePatch(pending_.patch);
        }
        pending_.hasPending = false;
        pending_.patch = {};
    }
};

#endif // OMNISTREAM_SAVEPOINTKVSTREAMWRITER_H
