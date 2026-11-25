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

#ifndef FLINK_TNEL_SPANNINGWRAPPER_H
#define FLINK_TNEL_SPANNINGWRAPPER_H

#include <vector>
#include <memory>
#include "core/utils/ByteBuffer.h"
#include "core/memory/DataInputDeserializer.h"
#include "NonSpanningWrapper.h"
#include "core/utils/utils.h"
#include "core/include/common.h"

namespace omnistream::datastream {
class SpanningWrapper {
public:
    ByteBuffer* lengthBuffer_;

    SpanningWrapper();
    ~SpanningWrapper();
    inline bool hasFullRecord() const;
    inline int getNumGatheredBytes() const;
    inline void clear();

    inline DataInputView& getInputView();

    inline void transferLeftOverTo(NonSpanningWrapper& nonSpanningWrapper);
    inline void transferFrom(NonSpanningWrapper& partial, int nextRecordLength);
    inline void addNextChunkFromMemoryBuffer(const uint8_t* buffer, int numBytes);

    Buffer* GetUnconsumedSegment()
    {
        LOG("SpanningWrapper GetUnconsumedSegment position: " << lengthBuffer_->position());
        if (lengthBuffer_->position() > 0) {
            uint8_t* data = new uint8_t[lengthBuffer_->position()];
            MemorySegment* memorySegment = new MemorySegment(data, lengthBuffer_->position());
            memorySegment->put(0, lengthBuffer_->getValue(), 0, lengthBuffer_->position());
            ::datastream::NetworkBuffer* networkBuffer = new ::datastream::NetworkBuffer(
                memorySegment,
                lengthBuffer_->position(),
                0,
                std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER,
                true);
            return networkBuffer;
        } else if (recordLength_ == -1) {
            return nullptr;
        } else {
            return CopyDataBuffer();
        }
    }

    ::datastream::NetworkBuffer* CopyDataBuffer()
    {
        int leftOverSize = leftOverLimit_ - leftOverStart_;
        int unconsumedSize = LENGTH_BYTES + accumulatedRecordBytes_ + leftOverSize;
        auto serializer = std::make_shared<DataOutputSerializer>(unconsumedSize);
        serializer->writeInt(recordLength_);
        serializer->write(buffer_.data(), accumulatedRecordBytes_, 0, accumulatedRecordBytes_);
        if (leftOverData_ != nullptr) {
            serializer->write(const_cast<uint8_t*>(leftOverData_), 0, leftOverStart_, leftOverSize);
        }
        uint8_t* data = new uint8_t[unconsumedSize];
        MemorySegment* memorySegment = new MemorySegment(data, unconsumedSize);
        memorySegment->put(0, serializer->getData(), 0, unconsumedSize);
        ::datastream::NetworkBuffer* networkBuffer = new ::datastream::NetworkBuffer(
            memorySegment,
            unconsumedSize,
            0,
            std::make_shared<OriginalNetworkBufferRecycler>(),
            ObjectBufferDataType::DATA_BUFFER,
            true);
        return networkBuffer;
    }

private:
    std::vector<uint8_t> buffer_; // internal buff to stick data, has the ownership

    int recordLength_;
    int accumulatedRecordBytes_;

    DataInputDeserializer* serializationReadBuffer_;

    const uint8_t* leftOverData_; // ref to input buffer_ (flink MemSegment), no ownership
    int leftOverStart_;
    int leftOverLimit_;

    inline bool isReadingLength() const;
    inline void updateLength(int length);
    inline int readLength(const uint8_t* buffer, int remaining);
    inline void ensureBufferCapacity(int minLength);
    inline void copyIntoBuffer(const uint8_t* buffer, int offset, int length);
};

inline bool SpanningWrapper::hasFullRecord() const
{
    bool result = recordLength_ >= 0 && accumulatedRecordBytes_ >= recordLength_;
    // LOG("hasFullRecord is " << result)
#ifdef DEBUG
    if (result) PRINT_HEX(const_cast<uint8_t*>(buffer_.data()), 0, recordLength_);
#endif
    return result;
}

inline DataInputView& SpanningWrapper::getInputView()
{
    return *serializationReadBuffer_;
}

inline void SpanningWrapper::transferLeftOverTo(NonSpanningWrapper& nonSpanningWrapper)
{
    nonSpanningWrapper.clear();

    if (leftOverData_ != nullptr) {
        nonSpanningWrapper.initializeFromMemoryBuffer(leftOverData_ + leftOverStart_, leftOverLimit_ - leftOverStart_);
    }
    clear();
}

inline void SpanningWrapper::clear()
{
    recordLength_ = -1;
    accumulatedRecordBytes_ = 0;

    lengthBuffer_->clear();

    leftOverData_ = nullptr;
    leftOverStart_ = 0;
    leftOverLimit_ = 0;
}

// the function will be called after nonspanningwtrapper has read all full record. now the left is not full record
// length copy all nonspaccingwrapper remainings to spaandingwtriper buff
inline void SpanningWrapper::transferFrom(NonSpanningWrapper& partial, int nextRecordLength)
{
    // LOG("nextRecordLength :" << nextRecordLength)
    updateLength(nextRecordLength);
    accumulatedRecordBytes_ = partial.copyContentTo(buffer_.data());
    // LOG("after partial.copyContentTo : accumulatedRecordBytes_ is " << accumulatedRecordBytes_)
    partial.clear();
}

// numBytes total number of bytes of buffer
inline void SpanningWrapper::addNextChunkFromMemoryBuffer(const uint8_t* buffer, int numBytes)
{
    // 1. if the record length is not completed, get it
    // LOG("isReadingLength()  " << isReadingLength())
#ifdef DEBUG
    ByteBuffer::showInternalInfo(lengthBuffer_);
#endif
    int numBytesRead = isReadingLength() ? readLength(buffer, numBytes) : 0;
    int offset = numBytesRead;
    int remainNumBytes = numBytes - numBytesRead;
    // LOG("numBytesRead ()  " << numBytesRead  << " numBytes   " << numBytes  <<   " remainNumBytes  " <<
    // remainNumBytes  )
    if (remainNumBytes == 0) {
        return;
    }

    // 2. now the record length is ready, get the whole record
    // LOG("recordLength_  " << recordLength_   << " accumulatedRecordBytes_    " << accumulatedRecordBytes_  )
    int toCopy = std::min(recordLength_ - accumulatedRecordBytes_, remainNumBytes);
    // LOG("offset  " << offset   << " toCopy    " << toCopy  )
    if (toCopy > 0) {
        copyIntoBuffer(buffer, offset, toCopy);
    }

    // 3. if there is leftover data in buffer_, ref the left one through leftover
    if (remainNumBytes > toCopy) {
        leftOverData_ = buffer;
        leftOverStart_ = offset + toCopy;
        leftOverLimit_ = numBytes;
    }
}

// private member functions
inline bool SpanningWrapper::isReadingLength() const
{
    return lengthBuffer_->position() > 0;
}

inline void SpanningWrapper::updateLength(int length)
{
    lengthBuffer_->clear();
    recordLength_ = length;
    ensureBufferCapacity(length);
}

inline void SpanningWrapper::ensureBufferCapacity(int minLength)
{
    if (static_cast<size_t>(minLength) > buffer_.capacity()) {
        int newCapacity_ = std::max(minLength, static_cast<int>(buffer_.capacity() * 2));
        buffer_.reserve(newCapacity_);
    }
}

inline int SpanningWrapper::readLength(const uint8_t* buffer, int remaining)
{
    // LOG("lengthBuffer_->remaining(): "  << lengthBuffer_->remaining()  << " remaining:  "<< remaining)
    int bytesToRead = std::min(lengthBuffer_->remaining(), remaining);
    // LOG(" bytesToRead:  "<< bytesToRead)
    lengthBuffer_->putBytes(buffer, bytesToRead);
    // LOG("After put bytes lengthBuffer_->remaining(): "  << lengthBuffer_->remaining())
    if (!lengthBuffer_->hasRemaining()) {
        updateLength(lengthBuffer_->getIntBigEndian(0));
    }
    return bytesToRead;
}

inline void SpanningWrapper::copyIntoBuffer(const uint8_t* buffer, int offset, int length)
{
    auto ret = memcpy_s(buffer_.data() + accumulatedRecordBytes_, length, buffer + offset, length);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    accumulatedRecordBytes_ += length;
    if (hasFullRecord()) {
        // LOG("buffer_" << reinterpret_cast <long> (buffer_.data()) << " buffer size " << buffer_.size())
        // LOG("accumulatedRecordBytes_ " << accumulatedRecordBytes_  << " recordLength_ " << recordLength_)
        serializationReadBuffer_->setBuffer(buffer_.data(), accumulatedRecordBytes_, 0, recordLength_);
    }
}

inline int SpanningWrapper::getNumGatheredBytes() const
{
    return accumulatedRecordBytes_ + (recordLength_ >= 0 ? LENGTH_BYTES : lengthBuffer_->position());
}
} // namespace omnistream::datastream

#endif
