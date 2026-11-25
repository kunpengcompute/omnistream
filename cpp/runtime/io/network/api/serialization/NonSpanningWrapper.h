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

#ifndef FLINK_TNEL_NONSPANNINGWRAPPER_H
#define FLINK_TNEL_NONSPANNINGWRAPPER_H

#include <cstdint>
#include <cstdlib>
#include <string>
#include "DeserializationResult.h"
#include "core/io/IOReadableWritable.h"
#include "core/utils/ByteBuffer.h"
#include "memory/MemorySegment.h"
#include "buffer/Buffer.h"
#include "buffer/NetworkBuffer.h"
#include "buffer/OriginalNetworkBufferRecycler.h"
#include "core/include/common.h"
#include "core/utils/utils.h"

namespace omnistream::datastream {
class NonSpanningWrapper : public DataInputView {
public:
    NonSpanningWrapper();
    ~NonSpanningWrapper() override = default;

    inline bool hasRemaining() const;
    inline int remaining() const;

    inline bool hasCompleteLength() const;
    inline bool canReadRecord(int recordLength) const;
    inline DeserializationResult& readInto(IOReadableWritable& target);

    inline void clear();

    inline void transferTo(ByteBuffer& dst);
    inline void initializeFromMemoryBuffer(const uint8_t* buffer, int limit);
    inline void InitializeFromMemoryBuffer(const uint8_t* buffer, int position, int limit);
    inline int copyContentTo(uint8_t* dst);

    inline uint8_t readByte() override;
    inline int readInt() override;

    inline void readFully(uint8_t* buffer, int capacity, int offset, int length) override;

    inline int readUnsignedByte() override;

    inline int64_t readLong() override;
    inline void* GetBuffer() override
    {
        return nullptr;
    }

    inline std::string readUTF() override;

    inline int readUnsignedShort() override;

    inline bool readBoolean() override;

    inline double readDouble() override;

    const uint8_t* getData() override
    {
        return data_;
    }

    size_t getPosition() override
    {
        return position_;
    }

    void setPosition(size_t position) override
    {
        position_ = position;
    }

    Buffer* GetUnconsumedSegment()
    {
        LOG("NonSpanningWrapper GetUnconsumedSegment position: " << position_ << ", length: " << length_
                                                                 << ", remaining: " << remaining());
        if (!hasRemaining() || remaining() <= 0) {
            return nullptr;
        }
        uint8_t* data = new uint8_t[remaining()];
        MemorySegment* memorySegment = new MemorySegment(data, remaining());
        memorySegment->put(0, data_, position_, remaining());
        ::datastream::NetworkBuffer* networkBuffer = new ::datastream::NetworkBuffer(
            memorySegment,
            remaining(),
            0,
            std::make_shared<OriginalNetworkBufferRecycler>(),
            ObjectBufferDataType::DATA_BUFFER,
            true);
        return networkBuffer;
    }

private:
    const std::string BROKEN_SERIALIZATION_ERROR_MESSAGE = "Serializer consumed more bytes than the record had. ";

    const uint8_t* data_;
    size_t length_;
    size_t position_;

    // opt member variable
    std::vector<uint8_t> byteArr;
    std::vector<char> charArr;
};

inline bool NonSpanningWrapper::hasRemaining() const
{
    return position_ < length_;
}

inline int NonSpanningWrapper::remaining() const
{
    return static_cast<int>(length_ - position_);
}

inline uint8_t NonSpanningWrapper::readByte()
{
    if (unlikely(length_ == 0)) {
        THROW_LOGIC_EXCEPTION("NullptrException");
    }
    if (!hasRemaining()) {
        THROW_LOGIC_EXCEPTION("data_ has no remaining bytes to read");
    }

    return data_[position_++];
}

inline int NonSpanningWrapper::readInt()
{
    // big endian
    if (unlikely(position_ + sizeof(uint32_t) > length_)) {
        // Handle error or throw exception
        THROW_LOGIC_EXCEPTION("EOFException");
    }
    uint32_t value = (static_cast<uint32_t>(data_[position_]) << 24) |
                     (static_cast<uint32_t>(data_[position_ + 1]) << 16) |
                     (static_cast<uint32_t>(data_[position_ + 2]) << 8) | static_cast<uint32_t>(data_[position_ + 3]);

    position_ += sizeof(uint32_t);

    // LOG("position_ " + std::to_string(position_) + " address " + std::to_string(reinterpret_cast<long>(data_)))
    return static_cast<int>(value);
}

inline bool NonSpanningWrapper::hasCompleteLength() const
{
    return remaining() >= LENGTH_BYTES;
}

inline bool NonSpanningWrapper::canReadRecord(int recordLength) const
{
    return recordLength <= remaining();
}

inline DeserializationResult& NonSpanningWrapper::readInto(IOReadableWritable& target)
{
    LOG(">>>>>>>>>");

    target.read(*this);
    int remaining = this->remaining();
    if (unlikely(remaining < 0)) {
        std::string errorMsg = BROKEN_SERIALIZATION_ERROR_MESSAGE + " Remaining =  " + std::to_string(remaining);
        THROW_LOGIC_EXCEPTION(errorMsg);
    }

    if (remaining == 0) {
        return DeserializationResult_LAST_RECORD_FROM_BUFFER;
    } else {
        return DeserializationResult_INTERMEDIATE_RECORD_FROM_BUFFER;
    }
}

inline void NonSpanningWrapper::transferTo(ByteBuffer& dst)
{
    //

    int dstRemaining = dst.remaining();
    if (dstRemaining < remaining()) {
        THROW_LOGIC_EXCEPTION("Buffer overflow");
    }
    dst.putBytes(this->data_ + position_, remaining());

    clear();
}

inline void NonSpanningWrapper::clear()
{
    this->position_ = 0;
    data_ = nullptr;
    this->length_ = 0;
}

inline void NonSpanningWrapper::initializeFromMemoryBuffer(const uint8_t* buffer, int limit)
{
    data_ = buffer;
    position_ = 0;
    length_ = static_cast<size_t>(limit);

#ifdef DEBUG
    LOG("   NonSpanningWrapper::initializeFromMemoryBuffer: buffer  " + std::to_string(reinterpret_cast<long>(data_)) +
        " length_ " + std::to_string(length_));
#endif
}

inline void NonSpanningWrapper::InitializeFromMemoryBuffer(const uint8_t* buffer, int position, int limit)
{
    data_ = buffer;
    position_ = position;
    length_ = limit;
}

inline int NonSpanningWrapper::copyContentTo(uint8_t* dst)
{
    int numBytesChunk = remaining();
    auto* src = data_ + position_;
#ifdef DEBUG
    LOG("   buffer  " << reinterpret_cast<long>(dst) << " numBytesChunk " << numBytesChunk << " data_ " << data_
                      << " position_: " << position_ << " length_: " << length_);
#endif
    if (numBytesChunk) {
        auto ret = memcpy_s(dst, numBytesChunk, src, numBytesChunk);
        if (unlikely(ret != EOK)) {
            throw std::runtime_error("memcpy_s failed");
        }
    }
    return numBytesChunk;
}

inline int NonSpanningWrapper::readUnsignedByte()
{
    return static_cast<int>(data_[position_++]);
}

inline int64_t NonSpanningWrapper::readLong()
{
    const size_t required_size = sizeof(uint64_t);
    if (unlikely(length_ < required_size || position_ > length_ - required_size)) {
        THROW_LOGIC_EXCEPTION("EOFException");
    }
    auto ret = static_cast<int64_t>(
        (static_cast<uint64_t>(data_[position_]) << 56) | (static_cast<uint64_t>(data_[position_ + 1]) << 48) |
        (static_cast<uint64_t>(data_[position_ + 2]) << 40) | (static_cast<uint64_t>(data_[position_ + 3]) << 32) |
        (static_cast<uint64_t>(data_[position_ + 4]) << 24) | (static_cast<uint64_t>(data_[position_ + 5]) << 16) |
        (static_cast<uint64_t>(data_[position_ + 6]) << 8) | static_cast<uint64_t>(data_[position_ + 7]));
    position_ += 8;
    return ret;
}

inline void NonSpanningWrapper::readFully(uint8_t* buffer, int capacity, int offset, int length)
{
    size_t ulength = static_cast<size_t>(length);
    std::copy(data_ + position_, data_ + position_ + ulength, buffer + offset);
    position_ += ulength;
}

inline std::string NonSpanningWrapper::readUTF()
{
    auto utflen = readUnsignedShort();
    byteArr.clear();
    byteArr.resize(utflen);
    charArr.clear();
    charArr.resize(utflen);

    int c;
    int char2;
    int char3;
    int count = 0;
    int chararrCount = 0;

    readFully(byteArr.data(), utflen, 0, utflen);

    while (count < utflen) {
        c = (int)byteArr[count] & 0xff;
        if (c > 127) {
            break;
        }
        count++;
        charArr[chararrCount++] = (char)c;
    }

    while (count < utflen) {
        c = (int)byteArr[count] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                /* 0xxxxxxx */
                count++;
                charArr[chararrCount++] = (char)c;
                break;
            case 12:
            case 13:
                /* 110x xxxx 10xx xxxx */
                count += 2;
                if (unlikely(count > utflen)) {
                    THROW_LOGIC_EXCEPTION("malformed input: partial character at end");
                }
                char2 = (int)byteArr[count - 1];
                if (unlikely((char2 & 0xC0) != 0x80)) {
                    THROW_LOGIC_EXCEPTION("malformed input around byte " + count);
                }
                charArr[chararrCount++] = (char)(((c & 0x1F) << 6) | (char2 & 0x3F));
                break;
            case 14:
                /* 1110 xxxx 10xx xxxx 10xx xxxx */
                count += 3;
                if (unlikely(count > utflen)) {
                    THROW_LOGIC_EXCEPTION("malformed input: partial character at end");
                }
                char2 = (int)byteArr[count - 2];
                char3 = (int)byteArr[count - 1];
                if (unlikely(((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))) {
                    THROW_LOGIC_EXCEPTION("malformed input around byte " + (count - 1));
                }
                charArr[chararrCount++] = (char)(((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                break;
            default:
                /* 10xx xxxx, 1111 xxxx */
                THROW_LOGIC_EXCEPTION("malformed input around byte " + count);
        }
    }

    return std::string(charArr.data(), charArr.size());
}

inline int NonSpanningWrapper::readUnsignedShort()
{
    // big endian
    if (unlikely(position_ + sizeof(uint16_t) > length_)) {
        // Handle error or throw exception
        THROW_LOGIC_EXCEPTION("readUnsignedShort EOFException");
    }
    int value =
        (((static_cast<uint16_t>(data_[position_]) & 0xFF00) >> 8) | (static_cast<uint16_t>(data_[position_]) << 8)) &
        0xffff;
    position_ += sizeof(uint16_t);

    return value;
}

inline bool NonSpanningWrapper::readBoolean()
{
    return readByte() == 1;
}

inline double NonSpanningWrapper::readDouble()
{
    return Double::doubleToLongBits(readLong());
}
} // namespace omnistream::datastream

#endif // FLINK_TNEL_NONSPANNINGWRAPPER_H
