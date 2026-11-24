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

#pragma once
#include <securec.h>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utils/ByteBuffer.h>

#include "include/outputbuffer.h"
#include "include/common.h"
#include "basictypes/Double.h"

class DataOutputSerializer {
public:
    DataOutputSerializer() = default;
    explicit DataOutputSerializer(int size);

    ~DataOutputSerializer()
    {
        if (data_ != nullptr && (this->outputBufferStatus == nullptr || this->outputBufferStatus->ownership == 1)) {
            delete[] data_;
        }
    }

    inline void setBackendBuffer(uint8_t* address, int32_t capacity);
    inline void setBackendBuffer(OutputBufferStatus* outputBufferStatus);
    inline void clear();

    inline void setPosition(int position);
    inline void setPositionUnsafe(int position);
    inline void writeIntUnsafe(uint32_t value, int pos);
    inline int length() const;
    inline void writeLong(int64_t value);

    inline void writeRecordTimestamp(uint64_t value);

    inline void writeByte(uint32_t var);

    inline void writeUTF(std::string str);

    inline void write(uint32_t var1);

    inline void writeShort(uint16_t value);

    inline void write(std::vector<uint8_t>& var);

    // todo: need to use int32_t
    inline void writeInt(uint32_t value);

    inline void writeBoolean(bool var);

    inline void writeDouble(double value);

    inline void write(uint8_t* buffer, int bufferSize, int dstOffset, int length);
    inline void expandDataBuffer(int requiredSize);

    inline ByteBuffer* wrapAsByteBuffer();

    inline uint8_t* getData();
    inline int getPosition();

    std::vector<uint8_t>* getCopyOfBuffer()
    {
        return new std::vector<uint8_t>(data_, data_ + position_);
    }

private:
    static inline void copyEncoded(uint8_t* destination, const void* source, size_t size);
    uint8_t* data_ = nullptr;
    int position_ = 0;
    int capacity_ = 0;
    OutputBufferStatus* outputBufferStatus = nullptr;
    ByteBuffer reusedWrapper;
};

inline void DataOutputSerializer::copyEncoded(uint8_t* destination, const void* source, size_t size)
{
    const errno_t errorCode = memcpy_s(destination, size, source, size);
    if (errorCode != EOK) {
        const std::string errorMessage =
            "Error: DataOutputSerializer::copyEncoded -> memcpy_s failed size=" + std::to_string(size) +
            " errorCode=" + std::to_string(errorCode);
        INFO_RELEASE(errorMessage << " destination=" << static_cast<const void*>(destination) << " source=" << source);
        throw std::runtime_error("memcpy_s failed");
    }
}

inline DataOutputSerializer::DataOutputSerializer(int size)
{
    this->data_ = new uint8_t[size];
}

inline ByteBuffer* DataOutputSerializer::wrapAsByteBuffer()
{
    ByteBuffer* byteBuffer = reusedWrapper.wrapOpt(data_, 0, position_);
    return byteBuffer;
    // return ByteBuffer::wrap(data_, 0, position_);
}

inline uint8_t* DataOutputSerializer::getData()
{
    return data_;
}

inline void DataOutputSerializer::setBackendBuffer(uint8_t* address, int32_t capacity)
{
    data_ = address;
    capacity_ = capacity;
    position_ = 0;
}

inline void DataOutputSerializer::setBackendBuffer(OutputBufferStatus* outputBufferStatus_)
{
    data_ = reinterpret_cast<uint8_t*>(outputBufferStatus_->outputBuffer_);
    capacity_ = outputBufferStatus_->capacity_;
    position_ = 0;
    this->outputBufferStatus = outputBufferStatus_;
}

inline void DataOutputSerializer::clear()
{
    position_ = 0;
}

inline void DataOutputSerializer::setPosition(int position)
{
    position_ = position;
}

inline void DataOutputSerializer::setPositionUnsafe(int position)
{
    position_ = position;
}

// flink buffer should be big endian. assume native byte order is little endian
inline void DataOutputSerializer::writeIntUnsafe(uint32_t value, int pos)
{
    const uint32_t encoded = __builtin_bswap32(value);
    copyEncoded(data_ + pos, &encoded, sizeof(encoded));
}

inline int DataOutputSerializer::length() const
{
    return position_;
}

inline void DataOutputSerializer::writeByte(uint32_t var)
{
    expandDataBuffer(1);
    *(data_ + position_++) = var & 0xff;
}

inline void DataOutputSerializer::write(uint32_t value)
{
    writeByte(value);
}

inline void DataOutputSerializer::writeShort(uint16_t value)
{
    expandDataBuffer(2);
    if (data_ == nullptr) {
        throw std::runtime_error("Data buffer is null");
    }
    const uint16_t encoded = __builtin_bswap16(value);
    copyEncoded(data_ + position_, &encoded, sizeof(encoded));
    position_ += 2;
}

inline void DataOutputSerializer::write(std::vector<uint8_t>& var)
{
    write(var.data(), var.size(), 0, var.size());
}

inline void DataOutputSerializer::writeInt(uint32_t value)
{
    expandDataBuffer(4);
    if (data_ == nullptr) {
        throw std::runtime_error("Data buffer is null");
    }
    const uint32_t encoded = __builtin_bswap32(value);
    copyEncoded(data_ + position_, &encoded, sizeof(encoded));
    position_ += 4;
}

inline void DataOutputSerializer::writeLong(int64_t value)
{
    expandDataBuffer(8);
    if (data_ == nullptr) {
        throw std::runtime_error("Data buffer is null");
    }
    const uint64_t encoded = __builtin_bswap64(static_cast<uint64_t>(value));
    copyEncoded(data_ + position_, &encoded, sizeof(encoded));
    position_ += 8;
}

/**
 * Write timestamp in a record element
 *  The timestamp is written in big endian format,  while the rest of the record element is written in little endian
 * format  Thus, we need this specific function
 */
inline void DataOutputSerializer::writeRecordTimestamp(uint64_t value)
{
    expandDataBuffer(8);
    if (data_ == nullptr) {
        throw std::runtime_error("Data buffer is null");
    }
    const uint64_t encoded = __builtin_bswap64(value);
    copyEncoded(data_ + position_, &encoded, sizeof(encoded));
    position_ += 8;
}

inline void DataOutputSerializer::write(uint8_t* buffer, int bufferSize, int dstOffset, int length)
{
    expandDataBuffer(length);
    std::copy(buffer + dstOffset, buffer + dstOffset + length, data_ + position_);
    position_ += length;
}

inline void DataOutputSerializer::expandDataBuffer(int requiredSize)
{
    if (unlikely(position_ + requiredSize > capacity_)) {
        LOG("******************output buffer is full, expand the buffer****************************************");
        capacity_ = (position_ + requiredSize) * 4;
        uint8_t* newData = new uint8_t[capacity_];
        // when data_ is nullptr, memcpy_s return not EOK, will cause exception
        if (likely(data_ != nullptr)) {
            std::copy(data_, data_ + position_, newData);
        }
        if (outputBufferStatus != nullptr && outputBufferStatus->ownership == 0) {
            STD_LOG("data_ is owned by java, should not delete " << reinterpret_cast<uintptr_t>(data_));
        } else {
            delete[] data_;
        }
        data_ = newData;

        if (outputBufferStatus != nullptr) {
            this->outputBufferStatus->ownership = 1;
            this->outputBufferStatus->outputBuffer_ = reinterpret_cast<uintptr_t>(data_);
            this->outputBufferStatus->capacity_ = capacity_;
        }
    }
}

inline int DataOutputSerializer::getPosition()
{
    return position_;
}

inline void DataOutputSerializer::writeUTF(std::string str)
{
    int strlen = str.length();
    int utflen = 0;
    int c;
    char* strChar = str.data();

    /* use charAt instead of copying String to char array */
    for (int i = 0; i < strlen; i++) {
        c = strChar[i];
        if ((c >= 0x0001) && (c <= 0x007F)) {
            utflen++;
        } else if (c > 0x07FF) {
            utflen += 3;
        } else {
            utflen += 2;
        }
    }
    if (utflen > 65535) {
        THROW_RUNTIME_ERROR("Encoded string is too long: " + utflen);
    }
    expandDataBuffer(utflen + 2);
    int count = position_;

    *(data_ + count++) = (utflen >> 8) & 0xff;
    *(data_ + count++) = utflen & 0xff;
    int i;
    for (i = 0; i < strlen; i++) {
        c = strChar[i];
        if (!((c >= 0x0001) && (c <= 0x007F))) {
            break;
        }
        *(data_ + count++) = c;
    }

    for (; i < strlen; i++) {
        c = strChar[i];
        if ((c >= 0x0001) && (c <= 0x007F)) {
            *(data_ + count++) = c;
        } else if (c > 0x07FF) {
            *(data_ + count++) = (0xE0 | ((c >> 12) & 0x0F));
            *(data_ + count++) = (0x80 | ((c >> 6) & 0x3F));
            *(data_ + count++) = (0x80 | (c & 0x3F));
        } else {
            *(data_ + count++) = (0xC0 | ((c >> 6) & 0x1F));
            *(data_ + count++) = (0x80 | (c & 0x3F));
        }
    }
    position_ = count;
}

inline void DataOutputSerializer::writeDouble(double value)
{
    writeLong(Double::doubleToLongBits(value));
}

inline void DataOutputSerializer::writeBoolean(bool var)
{
    write(var ? 1 : 0);
}
