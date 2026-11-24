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

#ifndef OMNISTREAM_CHANNEL_STATE_SERIALIZER_H
#define OMNISTREAM_CHANNEL_STATE_SERIALIZER_H

#include "runtime/watermark/WatermarkStatus.h"
#include <sstream>
#include <stdexcept>
#include <vector>
#include <cstdint>
#include <iostream>
#include <limits>
#include <libboundscheck/include/securec.h>
#include "core/memory/MemorySegment.h"
#include "include/basictypes/java_io_InputStream.h"
#include "runtime/buffer/BufferBuilder.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "runtime/buffer/VectorBatchBuffer.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "table/utils/VectorBatchSerializationUtils.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/api/watermark/Watermark.h"

namespace omnistream {
class ChannelStateByteBuffer;
class ChannelStateSerializer {
public:
    virtual ~ChannelStateSerializer() = default;

    virtual void WriteHeader(char* dataStream) = 0;
    virtual void WriteData(char* dataStream, Buffer* buffer, int64_t& oldOffset) = 0;

    virtual std::vector<char> SerializeData(Buffer* buffer)
    {
        (void)buffer;
        throw std::runtime_error("SerializeData is not implemented by this ChannelStateSerializer");
    }

    virtual int64_t GetHeaderLength() const = 0;

    virtual void ReadHeader(std::ifstream& stream) = 0;
    virtual void ReadHeader2(std::shared_ptr<ByteStateHandleInputStream>& stream) = 0;

    virtual int ReadLength(std::ifstream& stream) = 0;
    virtual int ReadLength2(std::shared_ptr<ByteStateHandleInputStream>& stream) = 0;

    virtual int ReadData(std::ifstream& stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes) = 0;
    virtual int ReadData2(
        std::shared_ptr<ByteStateHandleInputStream>& stream,
        std::shared_ptr<ChannelStateByteBuffer> buffer,
        int bytes) = 0;

    virtual std::vector<char> ExtractAndMerge(const std::vector<char>& bytes, const std::vector<long>& offsets) = 0;

    virtual int getSize(Buffer* buffers) = 0;
};

class ChannelStateSerializerImpl : public ChannelStateSerializer {
public:
    void WriteHeader(char* dataStream) override
    {
        uint8_t header[4];
        (void)memset_s(header, 4, 0, 4);
        offset.store(0);
        int64_t oldOffset = offset.fetch_add(sizeof(header));
        memcpy_s(dataStream + oldOffset, memSize - oldOffset, reinterpret_cast<const char*>(header), sizeof(header));
    }

    void WriteData(char* dataStream, Buffer* buffers, int64_t& oldOffset) override
    {
        int32_t size = getSize(buffers);
        uint8_t lenBytes[4];
        (void)memset_s(lenBytes, sizeof(lenBytes), 0, sizeof(lenBytes));
        lenBytes[0] = static_cast<uint8_t>((size >> 24) & 0xFF);
        lenBytes[1] = static_cast<uint8_t>((size >> 16) & 0xFF);
        lenBytes[2] = static_cast<uint8_t>((size >> 8) & 0xFF);
        lenBytes[3] = static_cast<uint8_t>(size & 0xFF);

        auto segment = buffers->GetSegment();
        if (segment->isObjectSegment()) {
            oldOffset = offset.fetch_add(sizeof(lenBytes));
            int64_t newOffset = oldOffset;
            memcpy_s(
                dataStream + newOffset, memSize - newOffset, reinterpret_cast<const char*>(lenBytes), sizeof(lenBytes));
            newOffset = offset.fetch_add(size);
            auto rawBytes = GetRawBytes(buffers);
            if (rawBytes.first != nullptr) {
                memcpy_s(dataStream + newOffset, memSize - newOffset, rawBytes.first, rawBytes.second);
            } else {
                auto objectSegment = static_cast<ObjectSegment*>(segment);
                auto serializedData =
                    serializeObjectSegment(objectSegment, size, buffers->GetOffset(), buffers->GetSize());
                memcpy_s(
                    dataStream + newOffset,
                    memSize - newOffset,
                    reinterpret_cast<const char*>(serializedData.data()),
                    size);
            }
        } else {
            auto memorySegment = dynamic_cast<MemorySegment*>(segment);
            if (memorySegment == nullptr) {
                throw std::runtime_error("ChannelStateSerializerImpl::WriteData requires MemorySegment-backed buffer");
            }
            oldOffset = offset.fetch_add(sizeof(lenBytes));
            int64_t newOffset = oldOffset;
            memcpy_s(
                dataStream + newOffset, memSize - newOffset, reinterpret_cast<const char*>(lenBytes), sizeof(lenBytes));
            newOffset = offset.fetch_add(size);
            auto memsegoff = buffers->GetOffset();
            memcpy_s(
                dataStream + newOffset,
                memSize - newOffset,
                reinterpret_cast<const char*>(memorySegment->getData()) + memsegoff,
                size);
        }
    }

    std::vector<char> SerializeData(Buffer* buffers) override
    {
        if (buffers == nullptr) {
            throw std::invalid_argument("ChannelStateSerializerImpl::SerializeData requires non-null buffer");
        }

        int32_t size = getSize(buffers);
        if (size < 0) {
            throw std::runtime_error("ChannelStateSerializerImpl::SerializeData got negative size");
        }

        uint8_t lenBytes[4];
        (void)memset_s(lenBytes, sizeof(lenBytes), 0, sizeof(lenBytes));
        lenBytes[0] = static_cast<uint8_t>((size >> 24) & 0xFF);
        lenBytes[1] = static_cast<uint8_t>((size >> 16) & 0xFF);
        lenBytes[2] = static_cast<uint8_t>((size >> 8) & 0xFF);
        lenBytes[3] = static_cast<uint8_t>(size & 0xFF);

        std::vector<char> serialized(sizeof(lenBytes) + static_cast<size_t>(size));
        errno_t ret =
            memcpy_s(serialized.data(), serialized.size(), reinterpret_cast<const char*>(lenBytes), sizeof(lenBytes));
        if (ret != EOK) {
            throw std::runtime_error("Failed to serialize channel-state length");
        }

        if (size == 0) {
            return serialized;
        }

        char* payload = serialized.data() + sizeof(lenBytes);
        size_t payloadCapacity = serialized.size() - sizeof(lenBytes);
        auto segment = buffers->GetSegment();
        if (segment->isObjectSegment()) {
            auto rawBytes = GetRawBytes(buffers);
            if (rawBytes.first != nullptr) {
                ret = memcpy_s(payload, payloadCapacity, rawBytes.first, rawBytes.second);
            } else {
                auto objectSegment = static_cast<ObjectSegment*>(segment);
                auto serializedData =
                    serializeObjectSegment(objectSegment, size, buffers->GetOffset(), buffers->GetSize());
                ret = memcpy_s(
                    payload,
                    payloadCapacity,
                    reinterpret_cast<const char*>(serializedData.data()),
                    serializedData.size());
            }
        } else {
            auto memorySegment = dynamic_cast<MemorySegment*>(segment);
            if (memorySegment == nullptr) {
                throw std::runtime_error(
                    "ChannelStateSerializerImpl::SerializeData requires MemorySegment-backed buffer");
            }
            auto memsegoff = buffers->GetOffset();
            ret = memcpy_s(
                payload, payloadCapacity, reinterpret_cast<const char*>(memorySegment->getData()) + memsegoff, size);
        }
        if (ret != EOK) {
            throw std::runtime_error("Failed to serialize channel-state payload");
        }
        return serialized;
    }

    std::vector<uint8_t> serializeObjectSegment(
        ObjectSegment* objectSegment, int32_t totalSize, int offset, size_t elementNum)
    {
        std::vector<uint8_t> data(totalSize);
        uint8_t* buffer = data.data();
        memcpy_s(buffer, totalSize, &elementNum, sizeof(int32_t));
        buffer += sizeof(int32_t);
        for (int32_t i = offset; i < elementNum + offset; i++) {
            StreamElement* element = objectSegment->getObject(i);
            if (element == nullptr) {
                int8_t tag = static_cast<int8_t>(StreamElementTag::TAG_UNKNOWN);
                memcpy_s(buffer, totalSize - (buffer - data.data()), &tag, sizeof(int8_t));
                buffer += sizeof(int8_t);
                continue;
            }
            StreamElementTag tag = element->getTag();
            if (tag == StreamElementTag::TAG_WATERMARK) {
                int8_t tagByte = static_cast<int8_t>(StreamElementTag::TAG_WATERMARK);
                memcpy_s(buffer, totalSize - (buffer - data.data()), &tagByte, sizeof(int8_t));
                buffer += sizeof(int8_t);
                auto* watermark = static_cast<Watermark*>(element);
                int64_t timestamp = watermark->getTimestamp();
                memcpy_s(buffer, totalSize - (buffer - data.data()), &timestamp, sizeof(int64_t));
                buffer += sizeof(int64_t);
            } else if (tag == StreamElementTag::TAG_STREAM_STATUS) {
                int8_t tagByte = static_cast<int8_t>(tag);
                int32_t status = static_cast<WatermarkStatus*>(element)->GetStatus();
                memcpy_s(buffer, totalSize - (buffer - data.data()), &tagByte, sizeof(tagByte));
                buffer += sizeof(tagByte);
                memcpy_s(buffer, totalSize - (buffer - data.data()), &status, sizeof(status));
                buffer += sizeof(status);
            } else if (
                tag == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP || tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
                tag == StreamElementTag::VECTOR_BATCH) {
                auto* record = dynamic_cast<StreamRecord*>(element);
                if (record != nullptr && record->hasExternalRow()) {
                    INFO_RELEASE("Warn: serializeObjectSegment hasExternalRow, which is not supported.");
                    throw std::invalid_argument("serializeObjectSegment hasExternalRow, which is not supported.");
                }
                int8_t tagByte = static_cast<int8_t>(tag);
                memcpy_s(buffer, totalSize - (buffer - data.data()), &tagByte, sizeof(int8_t));
                buffer += sizeof(int8_t);
                if (tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP) {
                    long timeStamp = record->getTimestamp();
                    memcpy_s(buffer, totalSize - (buffer - data.data()), &timeStamp, sizeof(long));
                    buffer += sizeof(long);
                }
                auto* vectorBatch = static_cast<VectorBatch*>(element->getValue());
                int32_t batchSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
                memcpy_s(buffer, totalSize - (buffer - data.data()), &batchSize, sizeof(int32_t));
                buffer += sizeof(int32_t);
                int32_t vectorCount = vectorBatch->GetVectorCount();
                memcpy_s(buffer, totalSize - (buffer - data.data()), &vectorCount, sizeof(int32_t));
                buffer += sizeof(int32_t);
                int32_t rowCnt = vectorBatch->GetRowCount();
                memcpy_s(buffer, totalSize - (buffer - data.data()), &rowCnt, sizeof(int32_t));
                buffer += sizeof(int32_t);
                VectorBatchSerializationUtils::serializeTimestampAndRowKinds(vectorBatch, buffer, batchSize);
                for (int32_t idx = 0; idx < vectorCount; idx++) {
                    BaseVector* colVector = vectorBatch->Get(idx);
                    DataTypeId dataType = colVector->GetTypeId();
                    Encoding encoding = colVector->GetEncoding();
                    if (encoding == OMNI_FLAT) {
                        if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                            VectorBatchSerializationUtils::serializeCharVector(colVector, buffer, batchSize);
                        } else {
                            VectorBatchSerializationUtils::serializePrimitiveVector(colVector, buffer, batchSize);
                        }
                    } else if (encoding == OMNI_DICTIONARY) {
                        if (dataType == OMNI_CHAR || dataType == OMNI_VARCHAR) {
                            VectorBatchSerializationUtils::serializeStringDictionaryContainerVector(
                                colVector, buffer, batchSize);
                        } else {
                            throw std::runtime_error("Unsupported encoding and data type combination");
                        }
                    } else {
                        throw std::runtime_error("Unsupported encoding");
                    }
                }
            } else if (
                tag == StreamElementTag::TAG_RECORD_ATTRIBUTES || tag == StreamElementTag::TAG_LATENCY_MARKER ||
                tag == StreamElementTag::TAG_INTERNAL_WATERMARK) {
                ERROR_RELEASE(
                    "ObjectSegment channel-state serialization does not support StreamElement tag "
                    << static_cast<int>(tag));
                throw std::runtime_error(
                    "ObjectSegment channel-state serialization does not support StreamElement tag " +
                    std::to_string(static_cast<int>(tag)));
            } else if (tag == StreamElementTag::TAG_UNKNOWN) {
                int8_t tagByte = static_cast<int8_t>(tag);
                memcpy_s(buffer, totalSize - (buffer - data.data()), &tagByte, sizeof(int8_t));
                buffer += sizeof(int8_t);
            } else {
                ERROR_RELEASE(
                    "ObjectSegment channel-state serialization encountered unknown StreamElement tag "
                    << static_cast<int>(tag));
                throw std::runtime_error(
                    "ObjectSegment channel-state serialization encountered unknown StreamElement tag " +
                    std::to_string(static_cast<int>(tag)));
            }
        }
        return data;
    }

    int32_t calculateObjectSegmentSerializedSize(ObjectSegment* objectSegment, size_t elementNum, int offset)
    {
        int32_t totalSize = sizeof(int32_t);
        for (int32_t i = offset; i < elementNum + offset; i++) {
            StreamElement* element = objectSegment->getObject(i);
            if (element == nullptr) {
                totalSize += sizeof(int8_t);
                continue;
            }
            StreamElementTag tag = element->getTag();
            if (tag == StreamElementTag::TAG_WATERMARK) {
                totalSize += sizeof(int8_t) + sizeof(int64_t);
            } else if (tag == StreamElementTag::TAG_STREAM_STATUS) {
                totalSize += sizeof(int8_t) + sizeof(int32_t);
            } else if (
                tag == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP || tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
                tag == StreamElementTag::VECTOR_BATCH) {
                if (auto* record = dynamic_cast<StreamRecord*>(element);
                    record != nullptr && record->hasExternalRow()) {
                    INFO_RELEASE("Warn: serializeObjectSegment hasExternalRow, which is not supported.");
                    throw std::invalid_argument("serializeObjectSegment hasExternalRow, which is not supported.");
                }
                if (tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP) {
                    totalSize += sizeof(long);
                }
                auto* vectorBatch = static_cast<VectorBatch*>(element->getValue());
                totalSize += VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(vectorBatch);
            } else if (
                tag == StreamElementTag::TAG_RECORD_ATTRIBUTES || tag == StreamElementTag::TAG_LATENCY_MARKER ||
                tag == StreamElementTag::TAG_INTERNAL_WATERMARK) {
                ERROR_RELEASE(
                    "ObjectSegment channel-state serialization does not support StreamElement tag "
                    << static_cast<int>(tag));
                throw std::runtime_error(
                    "ObjectSegment channel-state serialization does not support StreamElement tag " +
                    std::to_string(static_cast<int>(tag)));
            } else if (tag == StreamElementTag::TAG_UNKNOWN) {
                totalSize += sizeof(int8_t);
            } else {
                ERROR_RELEASE(
                    "ObjectSegment channel-state serialization encountered unknown StreamElement tag "
                    << static_cast<int>(tag));
                throw std::runtime_error(
                    "ObjectSegment channel-state serialization encountered unknown StreamElement tag " +
                    std::to_string(static_cast<int>(tag)));
            }
        }
        return totalSize;
    }

    int getSize(Buffer* buffers) override
    {
        auto segment = buffers->GetSegment();
        if (segment->isObjectSegment()) {
            auto rawBytes = GetRawBytes(buffers);
            if (rawBytes.first != nullptr) {
                if (rawBytes.second > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
                    throw std::overflow_error("Raw network buffer is too large for channel-state serialization");
                }
                return static_cast<int32_t>(rawBytes.second);
            }
            auto objectSegment = static_cast<ObjectSegment*>(segment);
            size_t elementNum = buffers->GetSize();
            int offset = buffers->GetOffset();
            return calculateObjectSegmentSerializedSize(objectSegment, elementNum, offset);
        }
        return buffers->GetSize();
    }

    int64_t GetHeaderLength() const override
    {
        return sizeof(int32_t);
    }

    void ReadHeader(std::ifstream& stream) override;
    void ReadHeader2(std::shared_ptr<ByteStateHandleInputStream>& stream) override;

    int ReadLength(std::ifstream& stream) override;
    int ReadLength2(std::shared_ptr<ByteStateHandleInputStream>& stream) override;

    int ReadData(std::ifstream& stream, std::shared_ptr<ChannelStateByteBuffer> buffer, int bytes) override;
    int ReadData2(
        std::shared_ptr<ByteStateHandleInputStream>& stream,
        std::shared_ptr<ChannelStateByteBuffer> buffer,
        int bytes) override;

    std::vector<char> ExtractAndMerge(const std::vector<char>& bytes, const std::vector<long>& offsets) override;
    std::atomic<int64_t> offset{0};
    size_t memSize = 128 * 1024 * 1024;

private:
    static std::pair<uint8_t*, size_t> GetRawBytes(Buffer* buffer)
    {
        auto* objectBuffer = dynamic_cast<ObjectBuffer*>(buffer);
        if (objectBuffer == nullptr) {
            return {nullptr, 0};
        }
        auto rawBytes = objectBuffer->GetBytes();
        if (rawBytes.first == nullptr || rawBytes.second == 0) {
            return {nullptr, 0};
        }
        return rawBytes;
    }
};

class ChannelStateByteBuffer {
public:
    virtual ~ChannelStateByteBuffer() = default;

    virtual bool isWritable() const = 0;

    virtual void close() = 0;

    virtual int writeBytes(std::ifstream& input, int bytesToRead) = 0;
    virtual int writeBytes2(std::shared_ptr<ByteStateHandleInputStream>& input, int bytesToRead) = 0;

    static std::shared_ptr<ChannelStateByteBuffer> wrap(BufferBuilder* bufferBuilder);

    static std::shared_ptr<ChannelStateByteBuffer> wrap(Buffer* buffer);
};

class ChannelStateByteBufferImpl : public ChannelStateByteBuffer {
public:
    explicit ChannelStateByteBufferImpl(BufferBuilder* builder);

    bool isWritable() const override;

    void close() override;

    int writeBytes(std::ifstream& input, int bytesToRead) override;
    int writeBytes2(std::shared_ptr<ByteStateHandleInputStream>& input, int bytesToRead) override;

private:
    BufferBuilder* bufferBuilder_;
    std::vector<uint8_t> buf_;

    int getToRead(int bytesToRead) const;
};

class ChannelStateByteBufferImpl2 : public ChannelStateByteBuffer {
public:
    explicit ChannelStateByteBufferImpl2(Buffer* buffer) : buffer_(buffer)
    {
    }

    bool isWritable() const override;

    void close() override;

    int writeBytes(std::ifstream& input, int bytesToRead) override;
    int writeBytes2(std::shared_ptr<ByteStateHandleInputStream>& input, int bytesToRead) override;

private:
    Buffer* buffer_;
};
} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_SERIALIZER_H
