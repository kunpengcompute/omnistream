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

#ifndef OMNISTREAM_RAWKEYEDSTATEINPUTSTREAMPROXY_H
#define OMNISTREAM_RAWKEYEDSTATEINPUTSTREAMPROXY_H

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "core/fs/FSDataInputStream.h"
#include "core/include/common.h"
#include "core/memory/DataInputView.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"

/**
 * Input counterpart of KeyedStateCheckpointOutputStream for legacy raw keyed timer restore.
 *
 * Some OmniStream serializers, such as StringValue::readString(), read directly from the
 * DataInputView backing buffer via getData()/getPosition()/setPosition().  Therefore this
 * proxy must be buffer-backed instead of a thin stream wrapper.  For remote checkpoint files,
 * the buffer is filled by opening the Java Flink FSDataInputStream through OmniTaskBridge, in
 * the same spirit as CheckpointStateOutputStreamProxy writes through OmniAdaptor.
 */
class RawKeyedStateInputStreamProxy : public DataInputView {
public:
    explicit RawKeyedStateInputStreamProxy(std::shared_ptr<FSDataInputStream> inputStream)
    {
        loadFromFsInputStream(std::move(inputStream));
    }

    RawKeyedStateInputStreamProxy(
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge,
        const std::shared_ptr<KeyGroupsStateHandle>& keyGroupsStateHandle)
        : omniTaskBridge_(std::move(omniTaskBridge))
    {
        if (keyGroupsStateHandle == nullptr) {
            INFO_RELEASE("Error: RawKeyedStateInputStreamProxy Raw keyed state handle is null.");
            THROW_LOGIC_EXCEPTION("Raw keyed state handle is null.");
        }

        auto inMemoryBytes = keyGroupsStateHandle->AsBytesIfInMemory();
        if (inMemoryBytes.has_value()) {
            data_ = std::move(inMemoryBytes.value());
            position_ = 0;
            return;
        }

        if (omniTaskBridge_ == nullptr) {
            INFO_RELEASE(
                "Error: RawKeyedStateInputStreamProxy Cannot restore raw keyed state without OmniTaskBridge "
                "for remote state handle.");
            THROW_LOGIC_EXCEPTION("Cannot restore raw keyed state without OmniTaskBridge for remote state handle.");
        }

        auto handleJson = TaskStateSnapshotSerializer::parseKeyGroupsStateHandle(keyGroupsStateHandle);
        inputStream_ = omniTaskBridge_->getSavepointInputStream(to_string(handleJson));
        if (inputStream_ == nullptr) {
            INFO_RELEASE(
                "Error: RawKeyedStateInputStreamProxy Failed to open raw keyed state input stream through "
                "OmniTaskBridge.");
            THROW_LOGIC_EXCEPTION("Failed to open raw keyed state input stream through OmniTaskBridge.");
        }

        loadFromOmniAdaptorStream();
        closeInputStream();
    }

    ~RawKeyedStateInputStreamProxy() override
    {
        closeInputStream();
    }

    void seek(int64_t offset)
    {
        if (offset < 0 || static_cast<size_t>(offset) > data_.size()) {
            INFO_RELEASE(
                "Error: seek Invalid raw keyed state seek offset: " << offset << ", dataSize=" << data_.size());
            THROW_LOGIC_EXCEPTION("Invalid raw keyed state seek offset: " << offset << ", dataSize=" << data_.size());
        }
        position_ = static_cast<size_t>(offset);
    }

    int readUnsignedByte() override
    {
        ensureAvailable(1, "readUnsignedByte");
        return data_[position_++] & 0xff;
    }

    uint8_t readByte() override
    {
        return static_cast<uint8_t>(readUnsignedByte());
    }

    int readUnsignedShort() override
    {
        ensureAvailable(2, "readUnsignedShort");
        int b1 = readUnsignedByte();
        int b2 = readUnsignedByte();
        return (b1 << 8) | b2;
    }

    int readInt() override
    {
        ensureAvailable(4, "readInt");
        uint32_t value =
            (static_cast<uint32_t>(data_[position_]) << 24) | (static_cast<uint32_t>(data_[position_ + 1]) << 16) |
            (static_cast<uint32_t>(data_[position_ + 2]) << 8) | static_cast<uint32_t>(data_[position_ + 3]);
        position_ += 4;
        return static_cast<int>(value);
    }

    int64_t readLong() override
    {
        ensureAvailable(8, "readLong");
        uint64_t value = 0;
        for (int i = 0; i < 8; ++i) {
            value = (value << 8) | static_cast<uint64_t>(data_[position_++]);
        }
        return static_cast<int64_t>(value);
    }

    double readDouble() override
    {
        INFO_RELEASE("Error: Raw keyed state timer restore does not support readDouble.");
        THROW_LOGIC_EXCEPTION("Raw keyed state timer restore does not support readDouble.");
    }

    bool readBoolean() override
    {
        return readUnsignedByte() != 0;
    }

    void readFully(uint8_t* buffer, int capacity, int offset, int length) override
    {
        if (buffer == nullptr || offset < 0 || length < 0 || offset + length > capacity) {
            INFO_RELEASE("Error: readFully Invalid readFully bounds for raw keyed state.");
            THROW_LOGIC_EXCEPTION("Invalid readFully bounds for raw keyed state.");
        }
        ensureAvailable(static_cast<size_t>(length), "readFully");
        std::copy(
            data_.begin() + static_cast<std::ptrdiff_t>(position_),
            data_.begin() + static_cast<std::ptrdiff_t>(position_ + static_cast<size_t>(length)),
            buffer + offset);
        position_ += static_cast<size_t>(length);
    }

    std::string readUTF() override
    {
        int utflen = readUnsignedShort();
        if (utflen == 0) {
            return "";
        }
        ensureAvailable(static_cast<size_t>(utflen), "readUTF");
        std::string result(reinterpret_cast<const char*>(data_.data() + position_), static_cast<size_t>(utflen));
        position_ += static_cast<size_t>(utflen);
        return result;
    }

    void* GetBuffer() override
    {
        return data_.empty() ? nullptr : data_.data();
    }

    const uint8_t* getData() override
    {
        return data_.empty() ? nullptr : data_.data();
    }

    size_t getPosition() override
    {
        return position_;
    }

    void setPosition(size_t position) override
    {
        if (position > data_.size()) {
            INFO_RELEASE(
                "Error: setPosition Invalid raw keyed state position: " << position << ", dataSize=" << data_.size());
            THROW_LOGIC_EXCEPTION("Invalid raw keyed state position: " << position << ", dataSize=" << data_.size());
        }
        position_ = position;
    }

    int remaining() const override
    {
        if (position_ >= data_.size()) {
            return 0;
        }
        const size_t remainingBytes = data_.size() - position_;
        return remainingBytes > static_cast<size_t>(std::numeric_limits<int>::max()) ? std::numeric_limits<int>::max()
                                                                                     : static_cast<int>(remainingBytes);
    }

private:
    static constexpr size_t READ_CHUNK_SIZE = 4096;

    void ensureAvailable(size_t bytes, const std::string& operation)
    {
        if (position_ > data_.size() || bytes > data_.size() - position_) {
            INFO_RELEASE(
                "Error: ensureAvailable EOF while " << operation << " raw keyed state. position=" << position_
                                                    << ", required=" << bytes << ", dataSize=" << data_.size());
            THROW_LOGIC_EXCEPTION(
                "EOF while " << operation << " raw keyed state. position=" << position_ << ", required=" << bytes
                             << ", dataSize=" << data_.size());
        }
    }

    void loadFromFsInputStream(std::shared_ptr<FSDataInputStream> inputStream)
    {
        if (inputStream == nullptr) {
            INFO_RELEASE("Error: loadFromFsInputStream Raw keyed state input stream is null.");
            THROW_LOGIC_EXCEPTION("Raw keyed state input stream is null.");
        }

        std::vector<uint8_t> chunk(READ_CHUNK_SIZE);
        while (true) {
            int read = inputStream->Read(chunk, 0, static_cast<int>(chunk.size()));
            if (read < 0) {
                break;
            }
            if (read == 0) {
                break;
            }
            data_.insert(data_.end(), chunk.begin(), chunk.begin() + read);
        }
        position_ = 0;
    }

    void loadFromOmniAdaptorStream()
    {
        std::vector<uint8_t> chunk(READ_CHUNK_SIZE);
        while (true) {
            int read = omniTaskBridge_->ReadSavepointInputStream(
                inputStream_, reinterpret_cast<int8_t*>(chunk.data()), 0, chunk.size());
            if (read < 0) {
                break;
            }
            if (read == 0) {
                break;
            }
            data_.insert(data_.end(), chunk.begin(), chunk.begin() + read);
        }
        position_ = 0;
    }

    void closeInputStream()
    {
        if (inputStream_ != nullptr && omniTaskBridge_ != nullptr) {
            omniTaskBridge_->closeSavepointInputStream(inputStream_);
            inputStream_ = nullptr;
        }
    }

    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge_;
    jobject inputStream_ = nullptr;
    std::vector<uint8_t> data_;
    size_t position_ = 0;
};

#endif // OMNISTREAM_RAWKEYEDSTATEINPUTSTREAMPROXY_H
