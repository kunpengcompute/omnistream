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

#ifndef FLINK_TNEL_BYTESTREAMSTATEHANDLE_H
#define FLINK_TNEL_BYTESTREAMSTATEHANDLE_H

#include <libboundscheck/include/securec.h>
#include <optional>
#include <cstring>
#include <sstream>
#include "core/fs/FSDataInputStream.h"
#include "runtime/state/StreamStateHandle.h"
#include "common.h"
/**
 * An input stream view on a byte array.
 */
class ByteStateHandleInputStream : public FSDataInputStream {
public:
    explicit ByteStateHandleInputStream(const std::vector<uint8_t>& data) : data_(data), index_(0)
    {
    }

    /**
     * Seek to the given offset from the start of the data.
     * The next read will begin at that location.
     *
     * @param desired the desired offset
     * @throws ios_base::failure if position is out of bounds
     */
    void Seek(std::streampos desired) override
    {
        if (desired < 0 || static_cast<size_t>(desired) > data_.size()) {
            throw std::ios_base::failure("Seek position out of bounds");
        }
        index_ = static_cast<size_t>(desired);
    }

    /**
     * Returns the current read position in the data.
     */
    std::streampos GetPos() const override
    {
        return static_cast<std::streampos>(index_);
    }

    /**
     * Reads a single byte from the data.
     *
     * @return the byte read (0–255), or -1 if end of stream
     */
    int Read() override
    {
        return index_ < data_.size() ? data_[index_++] & 0xFF : -1;
    }

    /**
     * Reads multiple bytes into a buffer.
     *
     * Note: bounds checking on the output buffer is assumed to be handled externally.
     *
     * @return number of bytes read, or -1 if end of stream
     */
    int Read(std::vector<uint8_t>& buffer, int off, int len) override
    {
        if (off < 0 || len < 0 || static_cast<size_t>(off + len) > buffer.size()) {
            throw std::out_of_range("Invalid buffer offset or length");
        }

        size_t bytesLeft = data_.size() - index_;
        if (bytesLeft == 0) return -1;

        size_t bytesToCopy = std::min(static_cast<size_t>(len), bytesLeft);
        if (bytesToCopy == 0) return 0;

        auto err = memcpy_s(buffer.data() + off, buffer.size() - off, data_.data() + index_, bytesToCopy);
        if (err != EOK) {
            throw std::runtime_error("memcpy_s failed with error code: " + std::to_string(err));
        }

        index_ += bytesToCopy;
        return static_cast<int>(bytesToCopy);
    }

private:
    std::vector<uint8_t> data_;
    size_t index_;
};
/**
 * A state handle that contains stream state in a byte array.
 */
class ByteStreamStateHandle : public StreamStateHandle {
public:
    /**
     * Creates a new ByteStreamStateHandle containing the given data.
     *
     * @param handleName A unique name used to identify and compare the state handle.
     *                   Like a filename, all ByteStreamStateHandle instances with the same name
     *                   must also have the same content.
     * @param data       The byte array containing the actual state data.
     */
    ByteStreamStateHandle(const std::string& handleName, const std::vector<uint8_t>& data)
        : handleName_(handleName),
          data_(data)
    {
        if (handleName_.empty()) {
            throw std::invalid_argument("handleName must not be empty");
        }
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        return PhysicalStateHandleID(handleName_);
    }

    /**
     * Opens an input stream to read from the byte array state.
     */
    std::shared_ptr<FSDataInputStream> OpenInputStream() const override
    {
        return std::make_shared<ByteStateHandleInputStream>(data_);
    }

    /**
     * Returns the byte array backing this state handle, if it is in memory.
     */
    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        return data_;
    }

    /**
     * Returns the raw data.
     */
    const std::vector<uint8_t>& GetData() const
    {
        return data_;
    }

    /**
     * Returns the unique handle name.
     */
    const std::string& GetHandleName() const
    {
        return handleName_;
    }

    /**
     * Discards the state. No-op for in-memory data.
     */
    void DiscardState() override
    {
        // no-op
    }

    /**
     * Returns the size of the state in bytes.
     */
    long GetStateSize() const override
    {
        return data_.size();
    }

    /**
     * Equality is based on handle name.
     */
    bool operator==(const StreamStateHandle& other) const
    {
        auto pOther = dynamic_cast<const ByteStreamStateHandle*>(&other);
        if (!pOther) return false;
        return handleName_ == pOther->handleName_;
    }

    size_t hashCode() const
    {
        size_t hashMultiplier = 31;
        return std::hash<std::string>{}(handleName_)*hashMultiplier;
    }

    /**
     * Returns a debug string representation.
     */
    std::string ToString() const override
    {
        nlohmann::json j;
        j["stateHandleName"] = "ByteStreamStateHandle";
        j["data"] = Base64_encode(data_);
        j["handleName"] = handleName_;
        j["streamStateHandleID"] = nlohmann::json::parse(GetStreamStateHandleID().ToString());
        j["stateSize"] = data_.size();
        return j.dump();
    }

private:
    std::string handleName_;
    std::vector<uint8_t> data_;
};

#endif // FLINK_TNEL_BYTESTREAMSTATEHANDLE_H
