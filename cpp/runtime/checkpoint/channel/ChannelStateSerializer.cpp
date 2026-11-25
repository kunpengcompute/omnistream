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

#include <cstring>
#include "runtime/buffer/MemoryBufferBuilder.h"
#include "runtime/buffer/ObjectBufferBuilder.h"
#include "runtime/checkpoint/channel/ChannelStateSerializer.h"

#include "core/memory/MemorySegment.h"

namespace omnistream {

constexpr int32_t MAX_REASONABLE_CHANNEL_STATE_CHUNK = 64 * 1024 * 1024;

inline int32_t DecodeIntBE(const uint8_t* b)
{
    return (static_cast<int32_t>(b[0]) << 24) | (static_cast<int32_t>(b[1]) << 16) | (static_cast<int32_t>(b[2]) << 8) |
           static_cast<int32_t>(b[3]);
}

inline int32_t DecodeIntLE(const uint8_t* b)
{
    return (static_cast<int32_t>(b[3]) << 24) | (static_cast<int32_t>(b[2]) << 16) | (static_cast<int32_t>(b[1]) << 8) |
           static_cast<int32_t>(b[0]);
}

inline bool IsPlausibleChannelStateLength(int32_t v)
{
    return v >= 0 && v <= MAX_REASONABLE_CHANNEL_STATE_CHUNK;
}

inline void ReadFully(std::ifstream& stream, uint8_t* dst, size_t len)
{
    stream.read(reinterpret_cast<char*>(dst), static_cast<std::streamsize>(len));
    if (stream.gcount() != static_cast<std::streamsize>(len)) {
        throw std::runtime_error("Failed to read full integer from file stream");
    }
}

inline void ReadFully(std::shared_ptr<ByteStateHandleInputStream>& stream, uint8_t* dst, size_t len)
{
    std::vector<uint8_t> tmp(len);
    int bytesRead = stream->Read(tmp, 0, static_cast<int>(len));
    if (bytesRead != static_cast<int>(len)) {
        throw std::runtime_error("Failed to read full integer from byte stream");
    }
    std::memcpy(dst, tmp.data(), len);
}

inline int32_t ReadIntBE(std::ifstream& stream)
{
    uint8_t b[4];
    ReadFully(stream, b, sizeof(b));
    return DecodeIntBE(b);
}

inline int32_t ReadIntBE(std::shared_ptr<ByteStateHandleInputStream>& stream)
{
    uint8_t b[4];
    ReadFully(stream, b, sizeof(b));
    return DecodeIntBE(b);
}

inline int32_t ReadLengthCompat(std::ifstream& stream)
{
    uint8_t b[4];
    ReadFully(stream, b, sizeof(b));

    int32_t be = DecodeIntBE(b);
    int32_t le = DecodeIntLE(b);

    if (IsPlausibleChannelStateLength(be) && !IsPlausibleChannelStateLength(le)) {
        return be;
    }
    if (!IsPlausibleChannelStateLength(be) && IsPlausibleChannelStateLength(le)) {
        LOG("WARN: detected legacy little-endian channel-state length from file stream: " << le);
        return le;
    }

    // 两边都合理时，优先 Flink/Java 标准大端
    return be;
}

inline int32_t ReadLengthCompat(std::shared_ptr<ByteStateHandleInputStream>& stream)
{
    uint8_t b[4];
    ReadFully(stream, b, sizeof(b));

    int32_t be = DecodeIntBE(b);
    int32_t le = DecodeIntLE(b);

    if (IsPlausibleChannelStateLength(be) && !IsPlausibleChannelStateLength(le)) {
        return be;
    }
    if (!IsPlausibleChannelStateLength(be) && IsPlausibleChannelStateLength(le)) {
        LOG("WARN: detected legacy little-endian channel-state length from byte stream: " << le);
        return le;
    }

    // 两边都合理时，优先 Flink/Java 标准大端
    return be;
}

void ChannelStateSerializerImpl::ReadHeader(std::ifstream& stream)
{
    int version = ReadIntBE(stream);
    if (version != 0) {
        LOG("Unsupported version: " << std::to_string(version));
        throw std::invalid_argument("Unsupported version: " + std::to_string(version));
    }
}

void ChannelStateSerializerImpl::ReadHeader2(std::shared_ptr<ByteStateHandleInputStream>& stream)
{
    int version = ReadIntBE(stream);
    if (version != 0) {
        LOG("Unsupported version: " << std::to_string(version));
        throw std::runtime_error("Unsupported version: " + std::to_string(version));
    }
}

int ChannelStateSerializerImpl::ReadLength(std::ifstream& stream)
{
    int length = ReadLengthCompat(stream);
    if (length < 0) {
        LOG("ERROR: Negative state size: " << std::to_string(length));
        throw std::invalid_argument("Negative state size");
    }
    return length;
}

int ChannelStateSerializerImpl::ReadLength2(std::shared_ptr<ByteStateHandleInputStream>& stream)
{
    int length = ReadLengthCompat(stream);
    if (length < 0) {
        LOG("ERROR: Negative state size: " << std::to_string(length));
        throw std::invalid_argument("Negative state size");
    }
    return length;
}

// void ChannelStateSerializerImpl::ReadHeader(std::ifstream &stream)
//{
//     int version;
//     stream.read(reinterpret_cast<char*>(&version), sizeof(version));
//     if (version != 0) {
//         LOG("Unsupported version: " << std::to_string(version));
//         throw std::invalid_argument("Unsupported version: " + std::to_string(version));
//     }
// }
// int readInt(std::shared_ptr<ByteStateHandleInputStream> &stream) {
//     std::vector<uint8_t> buffer(4);
//     int bytesRead = stream->Read(buffer, 0, 4);
//     if (bytesRead != 4) {
//         throw std::runtime_error("Failed to read full integer from stream");
//     }
//     int value = (buffer[3] << 24) | (buffer[2] << 16) | (buffer[1] << 8) | buffer[0];
//     return value;
// }
//
// void ChannelStateSerializerImpl::ReadHeader2(std::shared_ptr<ByteStateHandleInputStream> &stream)
//{
//     int version = readInt(stream);
//     if (version != 0) {
//         LOG("Unsupported version: " << std::to_string(version));
//         throw std::runtime_error("Unsupported version: " + std::to_string(version));
//     }
// }
//
// int ChannelStateSerializerImpl::ReadLength(std::ifstream &stream)
//{
//     int length;
//     stream.read(reinterpret_cast<char*>(&length), sizeof(length));
//     if (length < 0) {
//         LOG("ERROR: Negative state size: " <<  std::to_string(length));
//         throw std::invalid_argument("Negative state size");
//     }
//     return length;
// }
//
// int ChannelStateSerializerImpl::ReadLength2(std::shared_ptr<ByteStateHandleInputStream> &stream)
//{
//     int length = readInt(stream);
//     if (length < 0) {
//         LOG("ERROR: Negative state size: " <<  std::to_string(length));
//         throw std::invalid_argument("Negative state size");
//     }
//     return length;
// }

int ChannelStateSerializerImpl::ReadData(
    std::ifstream& stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes)
{
    if (!buffer) {
        throw std::invalid_argument("ChannelStateByteBuffer is null");
    }
    if (bytes < 0) {
        throw std::invalid_argument("Negative bytes to read");
    }
    return buffer->writeBytes(stream, bytes);
}

int ChannelStateSerializerImpl::ReadData2(
    std::shared_ptr<ByteStateHandleInputStream>& stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes)
{
    if (!stream) {
        throw std::invalid_argument("ByteStateHandleInputStream is null");
    }
    if (!buffer) {
        throw std::invalid_argument("ChannelStateByteBuffer is null");
    }
    if (bytes < 0) {
        throw std::invalid_argument("Negative bytes to read");
    }
    return buffer->writeBytes2(stream, bytes);
}

std::vector<char> ChannelStateSerializerImpl::ExtractAndMerge(
    const std::vector<char>& bytes, const std::vector<long>& offsets)
{
    std::vector<char> mergedData;
    //    DataInputStream lengthReadingStream(bytes);
    //
    //    long prevOffset = 0;
    //    for (long offset : offsets) {
    //        lengthReadingStream.skipBytes(static_cast<int>(offset - prevOffset));
    //        int dataWithLengthOffset = static_cast<int>(offset) + sizeof(int);
    //        mergedData.insert(mergedData.end(), bytes.begin() + dataWithLengthOffset, bytes.begin() +
    //        dataWithLengthOffset + lengthReadingStream.readInt()); prevOffset = dataWithLengthOffset;
    //    }
    //
    return mergedData;
}

std::shared_ptr<ChannelStateByteBuffer> ChannelStateByteBuffer::wrap(BufferBuilder* bufferBuilder)
{
    return std::make_shared<ChannelStateByteBufferImpl>(bufferBuilder);
}

std::shared_ptr<ChannelStateByteBuffer> ChannelStateByteBuffer::wrap(Buffer* buffer)
{
    return std::make_shared<ChannelStateByteBufferImpl2>(buffer);
}

ChannelStateByteBufferImpl::ChannelStateByteBufferImpl(BufferBuilder* builder) : bufferBuilder_(builder), buf_(1024)
{
}

bool ChannelStateByteBufferImpl::isWritable() const
{
    return !bufferBuilder_->isFull();
}

void ChannelStateByteBufferImpl::close()
{
    if (bufferBuilder_) {
        bufferBuilder_->close();
    }
}

int ChannelStateByteBufferImpl::writeBytes(std::ifstream& input, int bytesToRead) //
{
    if (bytesToRead <= 0) {
        return 0;
    }
    auto memoryBuilder = dynamic_cast<omnistream::datastream::MemoryBufferBuilder*>(bufferBuilder_);
    auto objectBuilder = dynamic_cast<omnistream::ObjectBufferBuilder*>(bufferBuilder_);
    if (memoryBuilder) {
        // MemoryBuffer can read 1024 bytes in a single loop, read all data through multiple loops
        int toRead = getToRead(bytesToRead);
        if (toRead <= 0) {
            return 0;
        }

        input.read(reinterpret_cast<char*>(buf_.data()), toRead);
        int readBytes = static_cast<int>(input.gcount());
        if (readBytes != toRead) {
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from file stream");
        }
        return memoryBuilder->appendRawBytes(buf_.data(), readBytes);
    }
    if (objectBuilder) {
        // ObjectBuffer must read all bytes in a single loop
        std::vector<uint8_t> tmpBuf(bytesToRead);

        input.read(reinterpret_cast<char*>(tmpBuf.data()), bytesToRead);
        int readBytes = static_cast<int>(input.gcount());
        if (readBytes != bytesToRead) {
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from file stream");
        }
        return objectBuilder->appendSerializedObjectSegment(tmpBuf.data(), readBytes);
    }
    throw std::runtime_error(
        "ChannelStateByteBufferImpl only supports MemoryBufferBuilder and ObjectBufferBuilder for byte channel-state "
        "restore");
}

int ChannelStateByteBufferImpl::writeBytes2(std::shared_ptr<ByteStateHandleInputStream>& input, int bytesToRead) //
{
    if (bytesToRead <= 0) {
        return 0;
    }

    if (!input) {
        INFO_RELEASE("Exception: ByteStateHandleInputStream is null.");
        throw std::invalid_argument("ByteStateHandleInputStream is null");
    }

    if (!bufferBuilder_) {
        INFO_RELEASE("Exception: ChannelStateByteBufferImpl buffer builder is null.");
        throw std::invalid_argument("ChannelStateByteBufferImpl buffer builder is null");
    }

    auto memoryBuilder = dynamic_cast<omnistream::datastream::MemoryBufferBuilder*>(bufferBuilder_);
    auto objectBuilder = dynamic_cast<omnistream::ObjectBufferBuilder*>(bufferBuilder_);
    if (memoryBuilder) {
        // MemoryBuffer can read 1024 bytes in a single loop, read all data through multiple loops
        int toRead = getToRead(bytesToRead);
        if (toRead <= 0) {
            return 0;
        }

        int readBytes = input->Read(buf_, 0, toRead);
        if (readBytes != toRead) {
            INFO_RELEASE("Exception: Unexpected EOF while reading channel-state data from byte stream.");
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from byte stream");
        }
        return memoryBuilder->appendRawBytes(buf_.data(), readBytes);
    }
    if (objectBuilder) {
        // ObjectBuffer must read all bytes in a single loop
        std::vector<uint8_t> tmpBuf(bytesToRead);

        int readBytes = input->Read(tmpBuf, 0, bytesToRead);
        if (readBytes != bytesToRead) {
            INFO_RELEASE("Exception: Unexpected EOF while reading channel-state data from byte stream.");
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from byte stream");
        }
        return objectBuilder->appendSerializedObjectSegment(tmpBuf.data(), readBytes);
    }
    INFO_RELEASE(
        "Exception: ChannelStateByteBufferImpl only supports MemoryBufferBuilder for byte channel-state restore.");
    throw std::runtime_error(
        "ChannelStateByteBufferImpl only supports MemoryBufferBuilder for byte channel-state restore");
}

int ChannelStateByteBufferImpl::getToRead(int bytesToRead) const
{
    int writable = bufferBuilder_->getWritableBytes();
    return std::min({bytesToRead, static_cast<int>(buf_.size()), writable});
}

bool ChannelStateByteBufferImpl2::isWritable() const
{
    return buffer_ && buffer_->GetSize() < buffer_->GetMaxCapacity();
}

void ChannelStateByteBufferImpl2::close()
{
    if (buffer_) {
        buffer_->RecycleBuffer();
    }
}

int ChannelStateByteBufferImpl2::writeBytes(std::ifstream& input, int bytesToRead) //
{
    if (!buffer_) {
        throw std::invalid_argument("Buffer is null");
    }

    auto memorySegment = dynamic_cast<MemorySegment*>(buffer_->GetSegment());
    if (memorySegment) {
        int writable = buffer_->GetMaxCapacity() - buffer_->GetSize();
        int toRead = std::min(bytesToRead, writable);
        if (toRead <= 0) {
            return 0;
        }

        std::vector<uint8_t> tmp(toRead);
        input.read(reinterpret_cast<char*>(tmp.data()), toRead);
        int readBytes = static_cast<int>(input.gcount());
        if (readBytes != toRead) {
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from file stream");
        }

        int writeOffset = buffer_->GetOffset() + buffer_->GetSize();
        memorySegment->put(writeOffset, tmp.data(), 0, readBytes);
        buffer_->SetSize(buffer_->GetSize() + readBytes);
        return readBytes;
    }
    auto objectSegment = dynamic_cast<ObjectSegment*>(buffer_->GetSegment());
    if (objectSegment) {
        // 注意：这里不能按 object writable 数量截断 bytesToRead
        // 必须读取完整 ObjectSegment payload
        std::vector<uint8_t> tmp(bytesToRead);
        input.read(reinterpret_cast<char*>(tmp.data()), bytesToRead);
        int readBytes = static_cast<int>(input.gcount());
        if (readBytes != bytesToRead) {
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from file stream");
        }

        auto result = ObjectSegmentChannelStateSerde::AppendSerializedObjectSegment(
            tmp.data(),
            bytesToRead,
            objectSegment,
            buffer_->GetOffset() + buffer_->GetSize(),
            buffer_->GetMaxCapacity() - buffer_->GetSize());

        buffer_->SetSize(buffer_->GetSize() + result.elementsWritten);
        return result.bytesConsumed;
    }
    throw std::runtime_error(
        "ChannelStateByteBufferImpl2 only supports MemorySegment and ObjectSegment Buffer for byte channel-state "
        "restore");
}

int ChannelStateByteBufferImpl2::writeBytes2(std::shared_ptr<ByteStateHandleInputStream>& input, int bytesToRead)
{
    if (!input) {
        INFO_RELEASE("Exception: ChannelStateByteBufferImpl2::writeBytes2 ByteStateHandleInputStream is null.");
        throw std::invalid_argument("ByteStateHandleInputStream is null");
    }
    if (!buffer_) {
        INFO_RELEASE("Exception: ChannelStateByteBufferImpl2::writeBytes2 Buffer is null.");
        throw std::invalid_argument("Buffer is null");
    }

    auto memorySegment = dynamic_cast<MemorySegment*>(buffer_->GetSegment());
    if (memorySegment) {
        int writable = buffer_->GetMaxCapacity() - buffer_->GetSize();
        int toRead = std::min(bytesToRead, writable);
        if (toRead <= 0) {
            return 0;
        }

        std::vector<uint8_t> tmp(toRead);
        int readBytes = input->Read(tmp, 0, toRead);
        if (readBytes != toRead) {
            INFO_RELEASE("Exception: Unexpected EOF while reading channel-state data from byte stream.");
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from byte stream");
        }

        int writeOffset = buffer_->GetOffset() + buffer_->GetSize();
        memorySegment->put(writeOffset, tmp.data(), 0, readBytes);
        buffer_->SetSize(buffer_->GetSize() + readBytes);
        return readBytes;
    }

    auto objectSegment = dynamic_cast<ObjectSegment*>(buffer_->GetSegment());
    if (objectSegment) {
        // 注意：这里不能按 object writable 数量截断 bytesToRead
        // 必须读取完整 ObjectSegment payload
        std::vector<uint8_t> tmp(bytesToRead);
        int readBytes = input->Read(tmp, 0, bytesToRead);
        if (readBytes != bytesToRead) {
            INFO_RELEASE("Exception: Unexpected EOF while reading channel-state data from byte stream.");
            throw std::ios_base::failure("Unexpected EOF while reading channel-state data from byte stream");
        }

        auto result = ObjectSegmentChannelStateSerde::AppendSerializedObjectSegment(
            tmp.data(),
            bytesToRead,
            objectSegment,
            buffer_->GetOffset() + buffer_->GetSize(),
            buffer_->GetMaxCapacity() - buffer_->GetSize());

        buffer_->SetSize(buffer_->GetSize() + result.elementsWritten);
        return result.bytesConsumed;
    }
    INFO_RELEASE(
        "Exception: ChannelStateByteBufferImpl2 only supports MemorySegment and ObjectSegment Buffer for byte "
        "channel-state restore.");
    throw std::runtime_error(
        "ChannelStateByteBufferImpl2 only supports MemorySegment and ObjectSegment Buffer for byte channel-state "
        "restore");
}
} // namespace omnistream
