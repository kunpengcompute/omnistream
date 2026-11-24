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

#ifndef OMNISTREAM_CHECKPOINTSTATEOUTPUTSTREAMPROXY_H
#define OMNISTREAM_CHECKPOINTSTATEOUTPUTSTREAMPROXY_H
#include <securec.h>
#include <algorithm>
#include <cstdint>
#include <jni.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "bridge/OmniTaskBridge.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "state/SnapshotResult.h"
#include "state/StreamStateHandle.h"
#include "state/bridge/OmniTaskBridge.h"

/**
 * Stateful savepoint output stream that combines buffering, DirectByteBuffer
 * management, and in-place byte-patching in a single class.
 *
 * Several responsibilities co-located here that would normally be split:
 * adaptive buffer growth, JNI DirectByteBuffer lifecycle, BytePatch guard
 * state machine, and big-endian KV serialisation. Splitting would introduce
 * virtual dispatch or pointer indirection on the savepoint hot path, where
 * every entry goes through writeKeyValuePair / tryWritePatchableKeyValuePair.
 */
class CheckpointStateOutputStreamProxy {
private:
    static constexpr size_t INITIAL_CHUNK_SIZE = 64 * 1024;
    static constexpr size_t MAX_CHUNK_SIZE = 4 * 1024 * 1024;
    jobject provider_;
    /** DirectByteBuffer wrapping chunk_.data() for zero-copy JNI writes.
     *  May be nullptr if DirectByteBuffer creation failed (fallback to byte[]). */
    jobject directBuffer_ = nullptr;
    std::shared_ptr<omnistream::OmniTaskBridge> bridge_;
    std::vector<int8_t> chunk_;
    size_t capacity_ = INITIAL_CHUNK_SIZE;
    size_t offset_ = 0;
    size_t pos_ = 0;
    bool patchGuardActive_ = false;
    uint64_t patchGuardGeneration_ = 0;
    size_t patchGuardOffset_ = 0;
    uint64_t flushGeneration_ = 0;

public:
    /**
     * Opaque handle for patching a previously-written byte in the pending
     * write chunk. Valid only until the next flush(), growBuffer(), or new
     * BytePatch activation.
     */
    struct BytePatch {
        size_t offset = 0;
        uint64_t flushGeneration = 0;
        bool valid = false;
    };

    CheckpointStateOutputStreamProxy(
        const std::shared_ptr<omnistream::OmniTaskBridge>& bridge,
        long checkpointId,
        CheckpointOptions* checkpointOptions)
        : bridge_(bridge),
          chunk_(INITIAL_CHUNK_SIZE)
    {
        provider_ = bridge_->AcquireSavepointOutputStream(checkpointId, checkpointOptions);
        if (!provider_) {
            throw std::runtime_error("Failed to AcquireSavepointOutputStream");
        }
        directBuffer_ = bridge_->CreateSavepointOutputDirectBuffer(chunk_.data(), capacity_);
    }

    virtual ~CheckpointStateOutputStreamProxy()
    {
        abortNoThrow();
    }

    std::shared_ptr<SnapshotResult<StreamStateHandle>> close()
    {
        flush();
        jobject provider = std::exchange(provider_, nullptr);
        if (provider == nullptr) {
            releaseDirectBuffer();
            return nullptr;
        }
        try {
            auto res = bridge_->CloseSavepointOutputStream(provider);
            releaseDirectBuffer();
            return res;
        } catch (...) {
            releaseDirectBuffer();
            throw;
        }
    }

    // Cleanup can run while another snapshot exception is unwinding and from the destructor.
    // AbortSavepointOutputStream may fail in JNI; suppress that secondary error so it neither
    // masks the original failure nor escapes the destructor and terminates the process.
    void abortNoThrow() noexcept
    {
        jobject provider = std::exchange(provider_, nullptr);
        try {
            if (provider != nullptr) {
                bridge_->AbortSavepointOutputStream(provider);
            }
        } catch (...) {
            INFO_RELEASE("Warning:CheckpointStateOutputStreamProxy::abortNoThrow failed to abort unfinalized stream");
        }
        releaseDirectBuffer();
    }

    void writeMetadata(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots, std::string keySerializer)
    {
        if (provider_ == nullptr) {
            return;
        }
        bridge_->WriteSavepointMetadata(provider_, snapshots, keySerializer);
        pos_ = bridge_->GetSavepointOutputStreamPos(provider_);
    }

    void writeOperatorMetaData(
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots)
    {
        if (provider_ == nullptr) {
            return;
        }
        bridge_->WriteOperatorMetaData(provider_, operatorStateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);
        pos_ = bridge_->GetSavepointOutputStreamPos(provider_);
    }

    void flush()
    {
        // Flushing advances flushGeneration_ and invalidates outstanding BytePatch.
        // The runtime guard rejects flush while a patchable KV is pending.
        requireNoActivePatch("flush");
        if (!provider_ || offset_ == 0) {
            return;
        }
        const size_t flushLen = offset_;
        if (directBuffer_ != nullptr) {
            (void)bridge_->WriteSavepointOutputStreamDirect(provider_, directBuffer_, flushLen);
        } else {
            bridge_->WriteSavepointOutputStream(provider_, chunk_.data(), 0, flushLen);
        }
        offset_ = 0;
        flushGeneration_++;
    }

    void writeByte(uint8_t data)
    {
        writeBytes(&data, sizeof(data));
    }

    void writeShort(int16_t data)
    {
        int8_t bytes[2];
        bytes[0] = static_cast<int8_t>((data >> 8) & 0xFF);
        bytes[1] = static_cast<int8_t>(data & 0xFF);
        writeBytes(bytes, sizeof(bytes));
    }

    void writeInt(int32_t data)
    {
        ensureBufferedCapacity(sizeof(int32_t));
        writeIntToBuffer(data);
        pos_ += sizeof(int32_t);
    }

    void writeLong(int64_t data)
    {
        int8_t bytes[8];
        bytes[0] = static_cast<int8_t>((data >> 56) & 0xFF);
        bytes[1] = static_cast<int8_t>((data >> 48) & 0xFF);
        bytes[2] = static_cast<int8_t>((data >> 40) & 0xFF);
        bytes[3] = static_cast<int8_t>((data >> 32) & 0xFF);
        bytes[4] = static_cast<int8_t>((data >> 24) & 0xFF);
        bytes[5] = static_cast<int8_t>((data >> 16) & 0xFF);
        bytes[6] = static_cast<int8_t>((data >> 8) & 0xFF);
        bytes[7] = static_cast<int8_t>(data & 0xFF);
        writeBytes(bytes, sizeof(bytes));
    }

    void writeUTF(const std::string& data)
    {
        DataOutputSerializer tmp(static_cast<int>(data.size() * 3 + 2));
        tmp.writeUTF(data);
        writeBytes(tmp.getData(), tmp.getPosition());
    }

    void writeBytes(const void* data, size_t len)
    {
        size_t ori_len = len;
        if (!provider_) {
            return;
        }
        requireNoActivePatch("writeBytes");
        const int8_t* src = (const int8_t*)data;
        while (len > 0) {
            if (offset_ == capacity_) {
                flush();
            }
            size_t size = std::min(len, capacity_ - offset_);
            (void)memcpy_s(&chunk_[offset_], capacity_ - offset_, src, size);
            len -= size;
            src += size;
            offset_ += size;
        }
        pos_ += ori_len;
    }

    void writeKeyValuePair(const std::vector<int8_t>& key, const std::vector<int8_t>& value)
    {
        writeKeyValuePair(
            ByteView::fromBuffer(key.data(), key.size()), ByteView::fromBuffer(value.data(), value.size()));
    }

    void writeKeyValuePair(ByteView key, ByteView value)
    {
        const size_t encodedLen = sizeof(int32_t) + key.size() + sizeof(int32_t) + value.size();
        if (encodedLen > capacity_) {
            writeInt(static_cast<int32_t>(key.size()));
            writeBytes(key.data(), key.size());
            writeInt(static_cast<int32_t>(value.size()));
            writeBytes(value.data(), value.size());
            return;
        }

        ensureBufferedCapacity(encodedLen);
        writeKeyValuePairToBuffer(key, value, encodedLen, nullptr);
    }

    void prepareForPatchableKeyValuePair(size_t encodedLen)
    {
        // Adaptive growth may flush and recreate the DirectByteBuffer because
        // chunk_.data() can move. Call only after the previous pending BytePatch
        // has been finalized and released; the runtime guard enforces it.
        requireNoActivePatch("prepareForPatchableKeyValuePair");
        size_t targetCapacity = capacity_;
        if (encodedLen > targetCapacity) {
            if (encodedLen <= (MAX_CHUNK_SIZE >> 1)) {
                targetCapacity = std::max(targetCapacity, roundUpPowerOfTwo(encodedLen << 1));
            } else if (encodedLen <= MAX_CHUNK_SIZE) {
                targetCapacity = std::max(targetCapacity, roundUpPowerOfTwo(encodedLen));
            }
        }
        size_t writeBytesTarget = capacity_;
        while (writeBytesTarget < MAX_CHUNK_SIZE && pos_ >= (writeBytesTarget << 1)) {
            writeBytesTarget <<= 1;
        }
        targetCapacity = std::max(targetCapacity, writeBytesTarget);
        targetCapacity = std::min(targetCapacity, MAX_CHUNK_SIZE);
        if (targetCapacity > capacity_) {
            if (!growBuffer(targetCapacity)) {
                INFO_RELEASE(
                    "Error: CheckpointStateOutputStreamProxy failed to grow buffer from "
                    << capacity_ << " to " << targetCapacity << " bytes");
            }
        }
    }

    bool tryWritePatchableKeyValuePair(ByteView key, ByteView value, BytePatch& patch)
    {
        if (key.empty()) {
            throw std::runtime_error("Patchable key/value pair key is empty");
        }
        requireNoActivePatch("tryWritePatchableKeyValuePair");
        const size_t encodedLen = sizeof(int32_t) + key.size() + sizeof(int32_t) + value.size();
        if (encodedLen > capacity_) {
            patch = {};
            return false;
        }
        ensureBufferedCapacity(encodedLen);
        writeKeyValuePairToBuffer(key, value, encodedLen, &patch);
        activatePatchGuard(patch);
        return true;
    }

    void patchByte(BytePatch patch, uint8_t mask)
    {
        requireActivePatch(patch, "patchByte");
        if (patch.flushGeneration != flushGeneration_ || patch.offset >= offset_) {
            throw std::runtime_error("Cannot patch byte after checkpoint stream buffer was flushed");
        }
        auto patched = static_cast<uint8_t>(chunk_[patch.offset]) | mask;
        chunk_[patch.offset] = static_cast<int8_t>(patched);
    }

    void releasePatch(BytePatch patch)
    {
        requireActivePatch(patch, "releasePatch");
        patchGuardActive_ = false;
        patchGuardGeneration_ = 0;
        patchGuardOffset_ = 0;
    }

    size_t getPos()
    {
        return pos_;
    }

private:
    void releaseDirectBuffer()
    {
        releaseDirectBuffer(directBuffer_);
        directBuffer_ = nullptr;
    }

    void releaseDirectBuffer(jobject directBuffer)
    {
        if (directBuffer == nullptr || !bridge_) {
            return;
        }
        try {
            bridge_->ReleaseSavepointOutputDirectBuffer(directBuffer);
        } catch (...) {
            INFO_RELEASE("Warning: ReleaseSavepointOutputDirectBuffer failed, DirectByteBuffer global ref may leak");
        }
    }

    void ensureBufferedCapacity(size_t requiredBytes)
    {
        requireNoActivePatch("ensureBufferedCapacity");
        if (offset_ + requiredBytes > capacity_) {
            flush();
        }
    }

    static size_t roundUpPowerOfTwo(size_t value)
    {
        size_t result = INITIAL_CHUNK_SIZE;
        while (result < value && result < MAX_CHUNK_SIZE) {
            result <<= 1;
        }
        return std::min(result, MAX_CHUNK_SIZE);
    }

    bool growBuffer(size_t targetCapacity)
    {
        // Growing replaces chunk_ and DirectByteBuffer, so any outstanding
        // BytePatch into the old buffer would become invalid.
        requireNoActivePatch("growBuffer");
        targetCapacity = std::min(roundUpPowerOfTwo(targetCapacity), MAX_CHUNK_SIZE);
        if (targetCapacity <= capacity_) {
            return false;
        }

        std::vector<int8_t> newChunk;
        try {
            newChunk.resize(targetCapacity);
        } catch (...) {
            INFO_RELEASE("Warning: Failed to resize savepoint output buffer to " << targetCapacity << " bytes");
            return false;
        }

        jobject newDirectBuffer = nullptr;
        if (bridge_) {
            try {
                newDirectBuffer = bridge_->CreateSavepointOutputDirectBuffer(newChunk.data(), targetCapacity);
            } catch (...) {
                INFO_RELEASE("Warning: Failed to create savepoint DirectByteBuffer, capacity=" << targetCapacity);
                return false;
            }
        }
        if (newDirectBuffer == nullptr) {
            return false;
        }

        try {
            flush();
        } catch (...) {
            releaseDirectBuffer(newDirectBuffer);
            throw;
        }

        jobject oldDirectBuffer = directBuffer_;
        directBuffer_ = nullptr;
        releaseDirectBuffer(oldDirectBuffer);
        chunk_ = std::move(newChunk);
        capacity_ = targetCapacity;
        directBuffer_ = newDirectBuffer;
        return true;
    }

    void activatePatchGuard(BytePatch patch)
    {
        if (!patch.valid) {
            throw std::runtime_error("Cannot activate savepoint BytePatch guard for invalid patch");
        }
        requireNoActivePatch("activatePatchGuard");
        patchGuardActive_ = true;
        patchGuardGeneration_ = patch.flushGeneration;
        patchGuardOffset_ = patch.offset;
    }

    void requireNoActivePatch(const char* operation) const
    {
        if (patchGuardActive_) {
            INFO_RELEASE("Error: Cannot " << operation << " while a savepoint BytePatch is pending");
            throw std::runtime_error(std::string("Cannot ") + operation + " while a savepoint BytePatch is pending");
        }
    }

    void requireActivePatch(BytePatch patch, const char* operation) const
    {
        if (!patch.valid) {
            INFO_RELEASE("Error: Cannot " << operation << " with invalid savepoint BytePatch");
            throw std::runtime_error(std::string("Cannot ") + operation + " with invalid savepoint BytePatch");
        }
        if (!patchGuardActive_) {
            INFO_RELEASE("Error: Cannot " << operation << " without active savepoint BytePatch guard");
            throw std::runtime_error(std::string("Cannot ") + operation + " without active savepoint BytePatch guard");
        }
        if (patch.flushGeneration != patchGuardGeneration_ || patch.offset != patchGuardOffset_) {
            INFO_RELEASE("Error: Cannot " << operation << " non-current savepoint BytePatch");
            throw std::runtime_error(std::string("Cannot ") + operation + " non-current savepoint BytePatch");
        }
    }

    void writeIntToBuffer(int32_t data)
    {
        int8_t bytes[4];
        bytes[0] = static_cast<int8_t>((data >> 24) & 0xFF);
        bytes[1] = static_cast<int8_t>((data >> 16) & 0xFF);
        bytes[2] = static_cast<int8_t>((data >> 8) & 0xFF);
        bytes[3] = static_cast<int8_t>(data & 0xFF);
        copyToBuffer(bytes, sizeof(bytes));
    }

    void copyToBuffer(const void* data, size_t len)
    {
        if (len == 0) {
            return;
        }
        (void)memcpy_s(&chunk_[offset_], capacity_ - offset_, data, len);
        offset_ += len;
    }

    static void writeIntBigEndian(uint8_t* dst, int32_t data)
    {
        dst[0] = static_cast<uint8_t>((data >> 24) & 0xFF);
        dst[1] = static_cast<uint8_t>((data >> 16) & 0xFF);
        dst[2] = static_cast<uint8_t>((data >> 8) & 0xFF);
        dst[3] = static_cast<uint8_t>(data & 0xFF);
    }

    void writeKeyValuePairToBuffer(ByteView key, ByteView value, size_t encodedLen, BytePatch* patch)
    {
        auto* dst = reinterpret_cast<uint8_t*>(&chunk_[offset_]);
        size_t remaining = capacity_ - offset_;
        if (patch != nullptr) {
            *patch = BytePatch{offset_ + sizeof(int32_t), flushGeneration_, true};
        }
        writeIntBigEndian(dst, static_cast<int32_t>(key.size()));
        dst += sizeof(int32_t);
        remaining -= sizeof(int32_t);
        if (!key.empty()) {
            (void)memcpy_s(dst, remaining, key.data(), key.size());
            dst += key.size();
            remaining -= key.size();
        }
        writeIntBigEndian(dst, static_cast<int32_t>(value.size()));
        dst += sizeof(int32_t);
        remaining -= sizeof(int32_t);
        if (!value.empty()) {
            (void)memcpy_s(dst, remaining, value.data(), value.size());
        }
        offset_ += encodedLen;
        pos_ += encodedLen;
    }
};
#endif
