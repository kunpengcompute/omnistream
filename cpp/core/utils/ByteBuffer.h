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

#ifndef FLINK_TNEL_BYTEBUFFER_H
#define FLINK_TNEL_BYTEBUFFER_H

#include <vector>
#include <cstddef>
#include <cstdint>
#include <memory>
#include "../include/common.h"
#include <libboundscheck/include/securec.h>

// Inline required for performance
class ByteBuffer {
public:
    explicit ByteBuffer(int capacity) : position_(0), limit_(capacity), mark_(-1), capacity_(capacity)
    {
        data_ = new uint8_t[capacity_];
    }
    ByteBuffer() : data_(nullptr), position_(0), limit_(0), mark_(-1), capacity_(0)
    {
    }

    ByteBuffer(uint8_t* data, int capacity) : position_(0), limit_(capacity), mark_(-1), capacity_(capacity)
    {
        data_ = data;
        reusedData = true;
    }
    // Capacity and limit operations
    inline int capacity() const;
    inline int position() const;
    inline int remaining() const;
    inline bool hasRemaining() const;
    inline void setPosition(int position);
    inline void setLimit(int limit);
    inline void clear();

    //  Primitive data type operations,  position_ updated
    inline void putBytes(const void* src, int len);
    inline void putInt(int value);
    inline void putLong(int64_t value);
    inline void putByte(uint8_t value);

    inline int64_t getLong();
    // int ByteBuffer::getIntBigEndian();
    inline uint8_t getByte();
    inline void getBytes(uint8_t* dst, int len);
    inline int limit();
    inline void flip();

    inline int getIntBigEndian();

    // Data read, position_ not updated
    inline int getIntBigEndian(int index);
    inline uint8_t getInt();
    inline uint8_t* getValue();

    // only used in datastream
    inline ByteBuffer* wrapOpt(uint8_t* data, int position, int length);
    static inline ByteBuffer* wrap(uint8_t* data, int length);
    static inline ByteBuffer* wrap(uint8_t* data, int position, int length);
    static inline std::shared_ptr<ByteBuffer> wrap2(uint8_t* data_, int length);

    inline int getSize()
    {
        return limit_;
    }

    static inline void showInternalInfo(ByteBuffer* buffer);
    inline int getIntFromValue();

    virtual ~ByteBuffer()
    {
        if (!reusedData) {
            delete[] data_;
        }
    }

protected:
    uint8_t* data_;
    int position_;
    int limit_;
    int mark_;
    int capacity_;
    bool reusedData = false;
};

// Capacity and limit operations
inline int ByteBuffer::capacity() const
{
    return capacity_;
}

inline int ByteBuffer::remaining() const
{
    int rem = limit_ - position_;
    return rem > 0 ? rem : 0;
}

inline bool ByteBuffer::hasRemaining() const
{
    return position_ < limit_;
}

inline int ByteBuffer::position() const
{
    return position_;
}

inline void ByteBuffer::clear()
{
    position_ = 0;
    limit_ = capacity_;
    mark_ = -1;
}

//  Primitive data type operations,  position_ updated
inline void ByteBuffer::putBytes(const void* src, int len)
{
    if (position_ + len > capacity()) {
        // Handle buffer_ overflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer overflow");
    }

    // Use memcpy for efficient bulk copy
    memcpy_s(data_ + position_, len, src, len);
    position_ += len;
}

inline void ByteBuffer::putInt(int value)
{
    // to avoid clean code
    auto curValue = static_cast<unsigned int>(value);
    uint8_t bytes[4];
    for (int i = 3; i >= 0; --i) {
        bytes[i] = static_cast<uint8_t>(curValue & 0xFF);
        curValue >>= 8;
    }
    putBytes(bytes, 4);
}

inline void ByteBuffer::putLong(int64_t value)
{
    auto curValue = static_cast<unsigned long>(value);
    uint8_t bytes[8];
    for (int i = 7; i >= 0; --i) {
        bytes[i] = static_cast<uint8_t>(curValue & 0xFF);
        curValue >>= 8;
    }
    putBytes(bytes, 8);
}

inline void ByteBuffer::putByte(uint8_t value)
{
    putBytes(&value, 1);
}

inline void ByteBuffer::flip()
{
    position_ = 0;
    mark_ = -1;
}

inline int64_t ByteBuffer::getLong()
{
    const int LEN = 8;
    if (position_ + LEN > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned long value = 0;
    for (int i = 0; i < LEN; ++i) {
        value <<= 8;
        value |= static_cast<uint8_t>((data_)[position_ + i]);
    }
    position_ += LEN;
    return static_cast<int64_t>(value);
}

inline uint8_t ByteBuffer::getInt()
{
    const int INT_SIZE = 4;
    if (position_ + INT_SIZE > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned int result = 0;
    for (int i = 0; i < INT_SIZE; ++i) {
        result <<= 8; //
        result |= static_cast<uint8_t>((data_)[position_ + i]);
    }

    position_ += INT_SIZE;
    return result;
}

inline uint8_t ByteBuffer::getByte()
{
    if (position_ + 1 > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }
    return (data_)[position_++];
}

inline void ByteBuffer::getBytes(uint8_t* dst, int len)
{
    if (position_ + len > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }
    memcpy_s(dst, len, data_ + position_, len);
    position_ += len;
}
inline int ByteBuffer::limit()
{
    return limit_;
}

inline int ByteBuffer::getIntBigEndian()
{
    const int INT_LENGTH = 4; // explicitly set the same size with java
    if (position_ + INT_LENGTH > limit_) {
        // Handle underflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned int value = 0;
    for (int i = 0; i < INT_LENGTH; ++i) {
        value <<= 8;
        value |= (data_)[position_ + i];
    }
    position_ += INT_LENGTH;
    return static_cast<int>(value);
}

// Data read, position_ not updated
inline int ByteBuffer::getIntBigEndian(int index)
{
    const int INT_LENGTH = 4; // explicitly set the same size with java
    if (index + INT_LENGTH > limit_) {
        // Handle underflow (e.g., throw an exception)
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }

    unsigned int value = 0;
    for (int i = 0; i < INT_LENGTH; ++i) {
        value <<= 8;
        value |= (data_)[index + i];
    }
    return static_cast<int>(value);
}

inline ByteBuffer* ByteBuffer::wrap(uint8_t* data, int length)
{
    return wrap(data, 0, length);
}

inline std::shared_ptr<ByteBuffer> ByteBuffer::wrap2(uint8_t* data_, int length)
{
    std::shared_ptr<ByteBuffer> ret = std::make_shared<ByteBuffer>();
    ret->data_ = data_;
    ret->capacity_ = length;
    ret->limit_ = length;
    ret->position_ = 0;
    ret->reusedData = true;
    return ret;
}

inline ByteBuffer* ByteBuffer::wrap(uint8_t* data, int position, int length)
{
    auto* ret = new ByteBuffer();
    ret->position_ = position;
    ret->data_ = data;
    ret->limit_ = length;
    ret->reusedData = true;
    return ret;
}

inline ByteBuffer* ByteBuffer::wrapOpt(uint8_t* data, int position, int length)
{
    this->data_ = data;
    this->position_ = position;
    this->limit_ = length;
    this->capacity_ = length;
    this->reusedData = true;
    return this;
}

inline void ByteBuffer::setPosition(int position)
{
    position_ = position;
}

inline void ByteBuffer::setLimit(int limit)
{
    limit_ = limit;
}
inline uint8_t* ByteBuffer::getValue()
{
    if (!data_) {
        return nullptr;
    }
    return data_;
}

inline void ByteBuffer::showInternalInfo(ByteBuffer* buffer)
{
    LOG("buffer   " << &buffer << " position_    " << buffer->position_ << " limit_    " << buffer->limit_
                    << " capacity_    " << buffer->capacity_ << " data_    "
                    << std::string(reinterpret_cast<const char*>(buffer->data_), buffer->limit_));
}

inline int ByteBuffer::getIntFromValue()
{
    const int INT_LENGTH = 4;
    if (position_ + INT_LENGTH > limit_) {
        THROW_LOGIC_EXCEPTION("ByteBuffer underflow");
    }
    unsigned int result = 0;
    uint8_t* value = getValue();
    for (int i = 0; i < INT_LENGTH; ++i) {
        result <<= 8;
        result |= (value[position_ + i]);
    }
    position_ += INT_LENGTH;
    return result;
}

#endif // FLINK_TNEL_BYTEBUFFER_H
