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

#ifndef FLINK_TNEL_DATAINPUTDESERIALIZER_H
#define FLINK_TNEL_DATAINPUTDESERIALIZER_H

#include <cstdint>
#include <vector>
#include "DataInputView.h"
#include "basictypes/Double.h"
#include "include/common.h"

class DataInputDeserializer : public DataInputView {
public:
    explicit DataInputDeserializer(const uint8_t* buffer = nullptr, int anEnd = 0, int position = 0)
        : buffer_(buffer),
          end_(anEnd),
          position_(position) {};

    ~DataInputDeserializer() override = default;

    void setBuffer(const uint8_t* buffer, int bufferLength, int start, int len);

    int readUnsignedByte() override;

    uint8_t readByte() override;

    int64_t readLong() override;

    int readInt() override;

    std::string readUTF() override;

    int readUnsignedShort() override;

    bool readBoolean() override;

    void readFully(uint8_t* b, int size, int off, int len) override;

    void* GetBuffer() override
    {
        return const_cast<void*>(static_cast<const void*>(buffer_));
    }

    int Available()
    {
        if (position_ < end_) {
            return end_ - position_;
        } else {
            return 0;
        }
    }

    int remaining() const override
    {
        return position_ < end_ ? end_ - position_ : 0;
    }
    double readDouble() override;

    const uint8_t* getData() override
    {
        return buffer_;
    }

    size_t getPosition() override
    {
        return position_;
    }

    void setPosition(size_t position) override
    {
        position_ = position;
    }

private:
    const uint8_t* buffer_;
    int end_ = 0;
    int position_ = 0;

    // opt member variable
    std::vector<uint8_t> byteArr;
    std::vector<char> charArr;
};

inline void DataInputDeserializer::setBuffer(const uint8_t* buffer, int bufferLength, int start, int len)
{
    LOG("DataInputDeserializer::setBuffer " << reinterpret_cast<long>(buffer) << " bufferLength " << bufferLength
                                            << " start " << start << " len " << len);

    if (start < 0 || len < 0 || start + len > bufferLength) {
        THROW_LOGIC_EXCEPTION("Invalid bounds.");
    }

    buffer_ = buffer;
    position_ = start;
    end_ = start + len;
}

inline uint8_t DataInputDeserializer::readByte()
{
    // debug
    LOG("DataInputDeserializer::readByte " << reinterpret_cast<long>(buffer_) << " position_ " << position_ << " end_ "
                                           << end_);
    if (unlikely(position_ < 0 || position_ > end_)) {
        THROW_LOGIC_EXCEPTION("readByte EOFException.");
    }
    return buffer_[position_++];
}

inline int64_t DataInputDeserializer::readLong()
{
    // big endian
    if (unlikely(position_ < 0 || position_ + sizeof(int64_t) > static_cast<size_t>(end_))) {
        // Handle error or throw exception
        THROW_LOGIC_EXCEPTION("EOFException, position_: " << position_ << ", end_: " << end_);
    }
    // big endian
    auto ret = static_cast<int64_t>(
        (static_cast<uint64_t>(buffer_[position_]) << 56) | (static_cast<uint64_t>(buffer_[position_ + 1]) << 48) |
        (static_cast<uint64_t>(buffer_[position_ + 2]) << 40) | (static_cast<uint64_t>(buffer_[position_ + 3]) << 32) |
        (static_cast<uint64_t>(buffer_[position_ + 4]) << 24) | (static_cast<uint64_t>(buffer_[position_ + 5]) << 16) |
        (static_cast<uint64_t>(buffer_[position_ + 6]) << 8) | static_cast<uint64_t>(buffer_[position_ + 7]));
    position_ += 8;
    return ret;
}

inline int DataInputDeserializer::readInt()
{
    // big endian
    if (unlikely(position_ < 0 || position_ + static_cast<int>(sizeof(uint32_t)) > end_)) {
        // Handle error or throw exception
        THROW_LOGIC_EXCEPTION("readInt EOFException");
    }
    uint32_t value =
        (static_cast<uint32_t>(buffer_[position_]) << 24) | (static_cast<uint32_t>(buffer_[position_ + 1]) << 16) |
        (static_cast<uint32_t>(buffer_[position_ + 2]) << 8) | static_cast<uint32_t>(buffer_[position_ + 3]);

    position_ += sizeof(uint32_t);

    LOG("position_ " << position_ << " address " << reinterpret_cast<long>(buffer_) << " value  " << value);
    return static_cast<int>(value);
}

inline void DataInputDeserializer::readFully(uint8_t* b, int size, int off, int len)
{
    if (likely(len >= 0 && off <= size && position_ <= end_ - len)) {
        std::copy(buffer_ + position_, buffer_ + position_ + len, b + off);
        position_ += len;
    } else {
        THROW_LOGIC_EXCEPTION("EOFException, position: " << position_ << ", end_: " << end_ << ", len: " << len);
    }
}

inline int DataInputDeserializer::readUnsignedByte()
{
    if (likely(position_ < end_)) {
        return (buffer_[position_++] & 0xff);
    } else {
        THROW_LOGIC_EXCEPTION("EOFException");
    }
}

inline std::string DataInputDeserializer::readUTF()
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

inline int DataInputDeserializer::readUnsignedShort()
{
    if (likely(position_ < end_ - 1)) {
        int f1 = position_++;
        return ((buffer_[f1] & 0xff) << 8) | (buffer_[position_++] & 0xff);
    } else {
        THROW_LOGIC_EXCEPTION("readUnsignedShort EOFException, end_: " << end_);
    }
}

inline bool DataInputDeserializer::readBoolean()
{
    if (likely(position_ < end_)) {
        return buffer_[position_++] != 0;
    } else {
        THROW_LOGIC_EXCEPTION("readBoolean EOFException, endl_: " << end_);
    }
}

inline double DataInputDeserializer::readDouble()
{
    return Double::doubleToLongBits(readLong());
}

#endif // FLINK_TNEL_DATAINPUTDESERIALIZER_H
