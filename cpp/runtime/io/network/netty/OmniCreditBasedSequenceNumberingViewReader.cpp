/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/streamrecord/StreamElementSerializer.h"
#include "typeutils/BinaryRowDataSerializer.h"

#include "buffer/ReadOnlySlicedVectorBatchBuffer.h"
#include "runtime/buffer/ReadOnlySlicedNetworkBuffer.h"

namespace omnistream {
OmniCreditBasedSequenceNumberingViewReader::OmniCreditBasedSequenceNumberingViewReader(
    ResultPartitionIDPOD partitionId, int subPartitionIndex, long outputBufferStatus, bool downStreamIsNative)
    : outputBufferStatus(reinterpret_cast<OutputBufferStatus*>(outputBufferStatus))
{
    this->downStreamIsNative = downStreamIsNative;
    nettyBufferPool = std::make_unique<NettyBufferPool>(bufferPoolSize, bufferSize);
    LOG_TRACE("create OmniCreditBasedSequenceNumberingViewReader " << reinterpret_cast<long>(this));
}

OmniCreditBasedSequenceNumberingViewReader::~OmniCreditBasedSequenceNumberingViewReader()
{
    stopped_.store(true, std::memory_order_release);
    if (subpartitionView) {
        subpartitionView->releaseAllResources();
        subpartitionView.reset();
    }
    if (networkBufferPendingRecycling.empty()) {
        return;
    }
    GErrorLog(
        "When OmniCreditBasedSequenceNumberingViewReader is destroyed, "
        "there are still " +
        std::to_string(networkBufferPendingRecycling.size()) + " network buffers not recycled");
    for (auto it = networkBufferPendingRecycling.begin(); it != networkBufferPendingRecycling.end();) {
        it->second->RecycleBuffer();
        delete it->second; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
        it = networkBufferPendingRecycling.erase(it);
    }
};

void OmniCreditBasedSequenceNumberingViewReader::notifyDataAvailable()
{
    LOG_TRACE("notifyDataAvailable is invoked queue size by outputflusher");
    this->getNextBufferInternal();
}

void OmniCreditBasedSequenceNumberingViewReader::requestSubpartitionView(
    std::shared_ptr<ResultPartitionManager> resultPartitionManager,
    ResultPartitionIDPOD partitionId,
    int subPartitionId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    this->subpartitionView = resultPartitionManager->createSubpartitionView(partitionId, subPartitionId, this);
    if (!this->subpartitionView) {
        LOG_TRACE("subpartitionView is null.........................");
        throw std::runtime_error("Subpartition view is null");
    }
}

int OmniCreditBasedSequenceNumberingViewReader::getAvailabilityAndBacklog()
{
    auto queueSize = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        queueSize = static_cast<int>(this->serializedBatchQueue.size());
    }
    LOG_TRACE(
        "OmniCreditBasedSequenceNumberingViewReaderN getAvailabilityAndBacklog "
        "queue size :"
        << queueSize);
    return queueSize;
}

void OmniCreditBasedSequenceNumberingViewReader::getNextBufferInternal()
{
    if (stopped_.load(std::memory_order_acquire)) {
        return;
    }
    if (!this->subpartitionView) {
        THROW_LOGIC_EXCEPTION("must be a bug , at this phase subpartitionView should not be null");
    }

    std::lock_guard<std::recursive_mutex> lock(fetchingDataMutex);
    BufferAndBacklog* bufferAndLog = this->subpartitionView->getNextBuffer();
    while (bufferAndLog) {
        Buffer* buffer = bufferAndLog->getBuffer();
        if (auto vectorBatchBuffer = dynamic_cast<VectorBatchBuffer*>(buffer)) {
            if (vectorBatchBuffer->GetSize() > 0) {
                // serialize data
                SerializeBufferAndBacklog(vectorBatchBuffer);
            } else {
                LOG("buffer size is 0, so i need to return " << std::this_thread::get_id());
                break;
            }
            // recycle buffer
            vectorBatchBuffer->RecycleBuffer();
            delete vectorBatchBuffer;
        } else if (auto nBuffer = dynamic_cast<::datastream::ReadOnlySlicedNetworkBuffer*>(buffer)) {
            uint8_t* memorySegmentAddress = nBuffer->getMemorySegment()->getAll();
            int memorySegmentOffset = nBuffer->GetMemorySegmentOffset();
            uint8_t* readableAddress = memorySegmentAddress + memorySegmentOffset;
            int datasSize = nBuffer->GetSize();
            int bufferType = nBuffer->isBuffer() ? 1 : 2;
            SerializedBatchInfo serializedBatchInfo = {readableAddress, datasSize, -1, bufferType};
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            auto serializedBatchInfoPtr = std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
            std::lock_guard<std::recursive_mutex> maplock(recycleNetworkBufferMutex);
            networkBufferPendingRecycling.insert({reinterpret_cast<int64_t>(readableAddress), nBuffer});
            serializedBatchQueue.push(serializedBatchInfoPtr);
        } else {
            THROW_RUNTIME_ERROR("Unknown buffer type in getNextBufferInternal");
        }

        delete bufferAndLog;
        bufferAndLog = this->subpartitionView->getNextBuffer();
    }
}

int OmniCreditBasedSequenceNumberingViewReader::getNextBuffer()
{
    int readElementNumber = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (!this->serializedBatchQueue.empty()) {
            size_t dataSize = this->serializedBatchQueue.size();
            readElementNumber = dataSize > 10 ? 10 : dataSize;
            uintptr_t dataResultContainer = this->outputBufferStatus->outputBuffer_;
            unsigned int position = 0;
            for (int i = 0; i < readElementNumber; i++) {
                std::shared_ptr<SerializedBatchInfo> serializedBatchInfo = this->serializedBatchQueue.front();
                this->serializedBatchQueue.pop();
                long bufferAddress = 0;
                int bufferLength = 0;
                if (serializedBatchInfo->event != -1) {
                    bufferAddress = -1;
                    bufferLength = serializedBatchInfo->event;
                    INFO_RELEASE(
                        ">>>OmniCreditBasedSequenceNumberingViewReader pop an event from queue type"
                        << serializedBatchInfo->event << "from subpartitionView for " << reinterpret_cast<long>(this));
                } else {
                    bufferAddress = reinterpret_cast<long>(serializedBatchInfo->buffer);
                    bufferLength = serializedBatchInfo->size;
                }
                LOG("bufferAddress: " << bufferAddress << " bufferLength: " << bufferLength);
                *reinterpret_cast<uint64_t*>(dataResultContainer + position) = bufferAddress;
                position += 8;
                *reinterpret_cast<uint32_t*>(dataResultContainer + position) = bufferLength;
                position += 4;
                *reinterpret_cast<uint32_t*>(dataResultContainer + position) = serializedBatchInfo->bufferType;
                position += 4;
            }
        }
    }

    this->outputBufferStatus->numberElement = static_cast<int32_t>(readElementNumber);
    return readElementNumber;
}

void OmniCreditBasedSequenceNumberingViewReader::DoSerializeVectorBatch(
    VectorBatch* element, int vectorSize, std::shared_ptr<NettyBufferInfo>& bufferInfo)
{
    if (!bufferInfo) {
        INFO_RELEASE("buffer info in DoSerializeVectorBatch is null");
        throw std::runtime_error("buffer info in DoSerializeVectorBatch is null");
    }
    VectorBatchSerializationUtils::serializeVectorBatch(element, vectorSize, bufferInfo->GetAddress());
    bufferInfo->SetWrittenBytes(vectorSize);
    bufferInfo->IncrementElementNum();
}

bool OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatch(
    VectorBatch* element, std::shared_ptr<NettyBufferInfo>& bufferInfo)
{
    int vectorSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(element);
    if (!bufferInfo) {
        INFO_RELEASE("buffer info in SerializeVectorBatch is null");
        throw std::runtime_error("buffer info in SerializeVectorBatch is null");
    }
    auto expectedBufferSize = vectorSize + NettyBufferInfo::elementNumBytes;
    if (expectedBufferSize > bufferSize) {
        // send regular buffer to queue first
        AddNettyBufferInfoToQueue(bufferInfo);
        // allocate a new buffer
        auto bigBufferInfo = RequestNettyBuffer(expectedBufferSize);
        DoSerializeVectorBatch(element, vectorSize, bigBufferInfo);
        AddNettyBufferInfoToQueue(bigBufferInfo);
        return true;
    } else {
        if (bufferInfo->Useable(vectorSize)) {
            DoSerializeVectorBatch(element, vectorSize, bufferInfo);
            return true;
        } else {
            // send data in buffer to queue
            AddNettyBufferInfoToQueue(bufferInfo);
            return false;
        }
    }
}

bool OmniCreditBasedSequenceNumberingViewReader::DoSerializeWaterMark(
    long timestamp, std::shared_ptr<NettyBufferInfo> bufferInfo)
{
    LOG("START TO SERIALIZE WATERMARK <<< " << timestamp);
    int dataSize = sizeof(int8_t) + sizeof(long);
    if (dataSize > bufferSize - bufferInfo->elementNumBytes) {
        // send regular buffer to queue first
        AddNettyBufferInfoToQueue(bufferInfo);
        // allocate a new buffer
        auto bigBufferInfo = RequestNettyBuffer(dataSize);
        if (!bufferInfo) {
            INFO_RELEASE("buffer info in DoSerializeWaterMark is null");
            throw std::runtime_error("buffer info in DoSerializeWaterMark is null");
        }
        VectorBatchSerializationUtils::SerializWatermark(timestamp, dataSize, bufferInfo->GetAddress());
        if (!bigBufferInfo) {
            INFO_RELEASE("big buffer info in DoSerializeWaterMark is null");
            throw std::runtime_error("big buffer info in DoSerializeWaterMark is null");
        }
        bigBufferInfo->SetWrittenBytes(dataSize);
        bigBufferInfo->IncrementElementNum();
        AddNettyBufferInfoToQueue(bigBufferInfo);
        return true;
    } else {
        if (bufferInfo->Useable(dataSize)) {
            VectorBatchSerializationUtils::SerializWatermark(timestamp, dataSize, bufferInfo->GetAddress());
            bufferInfo->SetWrittenBytes(dataSize);
            bufferInfo->IncrementElementNum();
            return true;
        } else {
            // send data in buffer to queue
            AddNettyBufferInfoToQueue(bufferInfo);
            return false;
        }
    }
}

void OmniCreditBasedSequenceNumberingViewReader::AddNettyBufferInfoToQueue(std::shared_ptr<NettyBufferInfo>& bufferInfo)
{
    if (bufferInfo->GetWrittenBytes() > 0) {
        VectorBatchSerializationUtils::SerializElementNum(
            bufferInfo->GetElementNum(), bufferInfo->GetOriginalAddress());
        bufferInfo->MarkElementNumWritten();
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        SerializedBatchInfo serializedBatchInfo = {bufferInfo->GetOriginalAddress(), bufferInfo->GetWrittenBytes()};
        auto serializedBatchInfoPtr = std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
        serializedBatchQueue.push(serializedBatchInfoPtr);
        bufferInfo = nullptr;
    }
}

std::shared_ptr<NettyBufferInfo> OmniCreditBasedSequenceNumberingViewReader::RequestNettyBuffer(int expectedBufferSize)
{
    int count = 0;
    std::shared_ptr<NettyBufferInfo> bufferInfo = nullptr;
    do {
        if (expectedBufferSize > bufferSize) {
            bufferInfo = nettyBufferPool->RequestBigBuffer(expectedBufferSize);
        } else {
            bufferInfo = nettyBufferPool->RequestBuffer();
        }
        if (!bufferInfo) {
            count++;
            if (count % noBufferPrintCount == 0) {
                std::lock_guard<std::recursive_mutex> queueLock(queueMutex);
                INFO_RELEASE(
                    "NO BUFFER AVAILABLE, the thread has been waiting for "
                    << requestNextBufferWaitingTime * noBufferPrintCount << " ms, serializedBatchQueue.size(): "
                    << serializedBatchQueue.size() << " pointer = " << reinterpret_cast<long>(this));
                count = 0;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(requestNextBufferWaitingTime));
        }
    } while (!bufferInfo);
    return bufferInfo;
}

void OmniCreditBasedSequenceNumberingViewReader::RecycleNettyBuffer(long address)
{
    nettyBufferPool->RecycleBuffer(address);
}

void OmniCreditBasedSequenceNumberingViewReader::SerializeBufferAndBacklog(VectorBatchBuffer* vectorBatchBuffer)
{
    if (vectorBatchBuffer->isBuffer()) {
        if (downStreamIsNative) {
            SerializeVectorBatchBuffer(vectorBatchBuffer);
        } else {
            SerializeVectorBatchBufferToRowData(vectorBatchBuffer);
        }
    } else {
        SerializeEvent(vectorBatchBuffer);
    }
}

void OmniCreditBasedSequenceNumberingViewReader::SerializeEvent(VectorBatchBuffer* vectorBatchBuffer)
{
    int evenType = vectorBatchBuffer->EventType();
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    SerializedBatchInfo serializedBatchInfo = {nullptr, 0, evenType};
    auto serializedBatchInfoPtr = std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
    serializedBatchQueue.push(serializedBatchInfoPtr);
    INFO_RELEASE(
        ">>>OmniCreditBasedSequenceNumberingViewReader push an event to queue type: "
        << evenType << "from subpartitionView for " << reinterpret_cast<long>(this));
}

void OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatchBuffer(VectorBatchBuffer* vectorBatchBuffer)
{
    ObjectSegment* objectSegment = vectorBatchBuffer->GetObjectSegment();
    int vectorBatchSize = vectorBatchBuffer->GetSize();
    auto offset = vectorBatchBuffer->GetOffset();
    auto bufferInfo = RequestNettyBuffer(bufferSize);
    for (int i = offset; i < vectorBatchSize + offset; i++) {
        StreamElement* streamElement = objectSegment->getObject(i);
        if (dynamic_cast<StreamRecord*>(streamElement)) {
            StreamRecord* streamRecord = static_cast<StreamRecord*>(streamElement);
            // Handle StreamRecord
            // process streamRecord
            VectorBatch* element = static_cast<VectorBatch*>(streamRecord->getValue());
            while (!SerializeVectorBatch(element, bufferInfo)) {
                // it means buffer is not enough
                bufferInfo = RequestNettyBuffer(bufferSize);
            }
            if (!bufferInfo) {
                bufferInfo = RequestNettyBuffer(bufferSize);
            }
            delete element;
            delete streamRecord;
        } else if (dynamic_cast<Watermark*>(streamElement)) {
            Watermark* watermark = static_cast<Watermark*>(streamElement);
            // Handle Watermark
            long timestamp = watermark->getTimestamp();
            while (!DoSerializeWaterMark(timestamp, bufferInfo)) {
                // it means buffer is not enough
                bufferInfo = RequestNettyBuffer(bufferSize);
            }
            delete watermark;
        } else {
            THROW_RUNTIME_ERROR("Unsupported stream element type");
        }
    }
    if (bufferInfo) {
        AddNettyBufferInfoToQueue(bufferInfo);
    }

    if (bufferInfo) {
        RecycleNettyBuffer(reinterpret_cast<long>(bufferInfo->GetOriginalAddress()));
    }
}

void OmniCreditBasedSequenceNumberingViewReader::RecycleNetworkBuffer(int64_t address)
{
    std::lock_guard<std::recursive_mutex> lock(recycleNetworkBufferMutex);
    auto it = networkBufferPendingRecycling.find(address);
    if (it != networkBufferPendingRecycling.end()) {
        it->second->RecycleBuffer();
        delete it->second; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
        networkBufferPendingRecycling.erase(it);
    }
}

void OmniCreditBasedSequenceNumberingViewReader::ResumeConsumption()
{
    this->subpartitionView->resumeConsumption();
}
void OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatchBufferToRowData(VectorBatchBuffer* buffer)
{
    auto enqueue = [this](StreamElement* element, omnistream::datastream::StreamElementSerializer& serializer) {
        DataOutputSerializer output;
        output.writeInt(0);
        serializer.serialize(element, output);
        output.writeIntUnsafe(output.length() - 4, 0);
        auto networkBuffer = RequestNettyBuffer(output.length());
        std::memcpy(networkBuffer->GetOriginalAddress(), output.getData(), output.length());
        auto info = std::make_shared<SerializedBatchInfo>(
            SerializedBatchInfo{networkBuffer->GetOriginalAddress(), output.length()});
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        serializedBatchQueue.push(info);
    };
    auto* segment = buffer->GetObjectSegment();
    for (int i = buffer->GetOffset(); i < buffer->GetOffset() + buffer->GetSize(); ++i) {
        std::unique_ptr<StreamElement> element(segment->getObject(i));
        if (auto* record = dynamic_cast<StreamRecord*>(element.get())) {
            std::unique_ptr<VectorBatch> batch(static_cast<VectorBatch*>(record->getValue()));
            omnistream::datastream::StreamElementSerializer serializer(
                new BinaryRowDataSerializer(batch->GetVectorCount()));
            for (int row = 0; row < batch->GetRowCount(); ++row) {
                std::unique_ptr<RowData> rowData(batch->extractRowData(row));
                StreamRecord rowRecord;
                rowRecord.setValue(rowData.get());
                if (batch->getTimestamps() != nullptr) {
                    rowRecord.setTimestamp(batch->getTimestamp(row));
                }
                enqueue(&rowRecord, serializer);
            }
        } else if (dynamic_cast<Watermark*>(element.get())) {
            omnistream::datastream::StreamElementSerializer serializer(nullptr);
            enqueue(element.get(), serializer);
        } else {
            THROW_RUNTIME_ERROR("Unsupported stream element in remote row conversion");
        }
    }
}

} // namespace omnistream
