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

#include "OmniLocalChannelReader.h"
#include <cstring>
#include <unordered_set>
#include "runtime/streamrecord/StreamRecord.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "streaming/runtime/streamrecord/StreamElementSerializer.h"

namespace omnistream {
OmniLocalChannelReader::OmniLocalChannelReader(
    ResultPartitionIDPOD partitionId, int subPartitionIndex, long outputBufferStatus, std::string taskNameWithSubtask)
    : partitionId_(partitionId),
      subPartitionIndex_(subPartitionIndex),
      memorySegmentInfo(reinterpret_cast<MemorySegmentInfo*>(outputBufferStatus)),
      taskNameWithSubtask_(std::move(taskNameWithSubtask))
{
}

OmniLocalChannelReader::~OmniLocalChannelReader()
{
    if (subpartitionView) {
        subpartitionView->releaseAllResources();
        subpartitionView.reset();
    }
    INFO_RELEASE(
        "When OmniLocalChannelReader is destroyed, "
        "there are still " +
        std::to_string(pendingRecyclingBufferMap.size()) + " network buffers not recycled");
    for (auto it = pendingRecyclingBufferMap.begin(); it != pendingRecyclingBufferMap.end();) {
        it->second->RecycleBuffer();
        delete it->second; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
        it = pendingRecyclingBufferMap.erase(it);
    }
    std::unordered_set<PendingSqlBuffer*> releasedBatches;
    for (const auto& entry : pendingSqlRows) {
        delete[] reinterpret_cast<uint8_t*>(entry.first);
        const auto& batch = entry.second;
        if (releasedBatches.insert(batch.get()).second) {
            batch->buffer->RecycleBuffer();
            delete batch->buffer;
            delete[] batch->container;
        }
    }
    pendingSqlRows.clear();
}

void OmniLocalChannelReader::requestSubpartitionView(
    std::shared_ptr<ResultPartitionManager> resultPartitionManager,
    ResultPartitionIDPOD partitionId,
    int subPartitionId)
{
    std::lock_guard<std::recursive_mutex> lock(createViewMutex);
    this->subpartitionView = resultPartitionManager->createSubpartitionView(partitionId, subPartitionId, this);
    if (!this->subpartitionView) {
        INFO_RELEASE("local reader subpartitionView is null.........................");
        throw std::runtime_error("Subpartition view is null");
    }
}

void OmniLocalChannelReader::notifyDataAvailable()
{
    LOG("OmniLocalChannelReader notifyDataAvailable invoke!");
    std::unique_lock<std::recursive_mutex> lock(dataAvailableMutex);
    dataAvailable = true;
    dataAvailableCondition.notify_one();
}

bool OmniLocalChannelReader::checkIfDataAvailable()
{
    std::unique_lock<std::recursive_mutex> lock(dataAvailableMutex);
    if (isStopped.load()) {
        INFO_RELEASE("OmniLocalChannelReader is stopped, no data available.");
        return false;
    }
    dataAvailableCondition.wait(lock, [this] {
        bool wait = !dataAvailable.load() && !isStopped.load();
        if (wait) {
            LOG("************* JNI INVOCATION FOR " << taskNameWithSubtask_
                                                    << " checkIfDataAvailable in OmniLocalChannelReader IS WAITING");
        }
        return !wait;
    });
    dataAvailable = false;
    return true;
}

uint8_t OmniLocalChannelReader::getNextBuffer()
{
    if (!this->subpartitionView) {
        LOG("must be a bug , at this phase subpartitionView should not be "
            "null");
        throw std::runtime_error("Subpartition view is null--------------------------");
    }

    std::lock_guard<std::recursive_mutex> lock(fetchingDataMutex);
    BufferAndBacklog* bufferAndLog = this->subpartitionView->getNextBuffer();

    if (bufferAndLog) {
        Buffer* buffer = bufferAndLog->getBuffer();
        if (buffer->GetSize() > 0) {
            if (auto vBuffer = dynamic_cast<VectorBatchBuffer*>(buffer)) {
                WrapBufferInfoIntoBinaryRowDataInfo(vBuffer, bufferAndLog);
                delete bufferAndLog;
                return 1;
            } else if (auto nBuffer = dynamic_cast<::datastream::ReadOnlySlicedNetworkBuffer*>(buffer)) {
                WrapBufferInfoIntoMemorySegmentInfo(nBuffer, bufferAndLog);
                delete bufferAndLog;
                return 1;
            } else {
                INFO_RELEASE("Unknown buffer type!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                buffer->RecycleBuffer();
                delete buffer;
                delete bufferAndLog;
                throw std::logic_error("Unknown buffer type in OmniLocalChannelReader");
            }
        } else {
            LOG("buffer size is 0, so i need to return " << std::this_thread::get_id());
            buffer->RecycleBuffer();
            delete buffer;
            delete bufferAndLog;
            return 0;
        }
    } else {
        return 0;
    }
}

void OmniLocalChannelReader::WrapBufferInfoIntoMemorySegmentInfo(
    ::datastream::ReadOnlySlicedNetworkBuffer* nBuffer, BufferAndBacklog* bufferAndLog)
{
    uint8_t* memorySegmentAddress = nBuffer->getMemorySegment()->getAll();
    int offset = nBuffer->GetMemorySegmentOffset();
    int numBytes = nBuffer->GetSize();
    int sequenceNumber = bufferAndLog->getSequenceNumber();
    int backlog = bufferAndLog->getBuffersInBacklog();
    int nextDataType = 0;
    if (bufferAndLog->getNextDataType() == ObjectBufferDataType::DATA_BUFFER) {
        nextDataType = 1;
    } else if (bufferAndLog->getNextDataType() == ObjectBufferDataType::EVENT_BUFFER) {
        nextDataType = 2;
    }
    int currentDataType = nBuffer->isBuffer() ? 1 : 2;
    memorySegmentInfo->memorySegmentAddress = reinterpret_cast<uint64_t>(memorySegmentAddress);
    memorySegmentInfo->readIndex = offset;
    memorySegmentInfo->length = numBytes;
    memorySegmentInfo->currentDataType = currentDataType;
    memorySegmentInfo->backlog = backlog;
    memorySegmentInfo->nextDataType = nextDataType;
    memorySegmentInfo->sequenceNumber = sequenceNumber;
    std::lock_guard<std::recursive_mutex> lock(recycleBufferMutex);
    pendingRecyclingBufferMap.emplace(memorySegmentInfo->memorySegmentAddress, nBuffer);
    LOG("memorySegmentAddress is " << memorySegmentInfo->memorySegmentAddress << " offset is "
                                   << memorySegmentInfo->readIndex << " numBytes is " << memorySegmentInfo->length
                                   << " currentDataType is " << memorySegmentInfo->currentDataType << " backlog is "
                                   << memorySegmentInfo->backlog << " nextDataType is "
                                   << memorySegmentInfo->nextDataType << " sequenceNumber is "
                                   << memorySegmentInfo->sequenceNumber);
}

void OmniLocalChannelReader::recycleMemorySegment(long memorySegmentAddress)
{
    uint64_t address = static_cast<uint64_t>(memorySegmentAddress);
    std::lock_guard<std::recursive_mutex> lock(recycleBufferMutex);
    auto sqlRow = pendingSqlRows.find(address);
    if (sqlRow != pendingSqlRows.end()) {
        auto batch = sqlRow->second;
        pendingSqlRows.erase(sqlRow);
        delete[] reinterpret_cast<uint8_t*>(address);
        if (--batch->remainingRows == 0) {
            batch->buffer->RecycleBuffer();
            delete batch->buffer;
            delete[] batch->container;
        }
        return;
    }
    auto it = pendingRecyclingBufferMap.find(address);
    if (it != pendingRecyclingBufferMap.end()) {
        it->second->RecycleBuffer();
        delete it->second; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
        pendingRecyclingBufferMap.erase(it);
    }
}

void OmniLocalChannelReader::Close()
{
    std::unique_lock<std::recursive_mutex> lock(dataAvailableMutex);
    isStopped = true;
    dataAvailableCondition.notify_one();
}

void OmniLocalChannelReader::ReleaseAllResources()
{
    Close();
    recycleMemorySegment(0);
    if (subpartitionView) {
        subpartitionView->releaseAllResources();
        subpartitionView = nullptr;
    }
}

void OmniLocalChannelReader::ResumeConsumption()
{
    subpartitionView->resumeConsumption();
}

int OmniLocalChannelReader::calculateTotalRows(ObjectSegment* objectSegment, int offset, int vbNum)
{
    int totalRow = 0;
    for (int i = offset; i < vbNum + offset; i++) {
        StreamElement* streamElement = objectSegment->getObject(i);
        if (dynamic_cast<StreamRecord*>(streamElement)) {
            auto* streamRecord = dynamic_cast<StreamRecord*>(streamElement);
            auto* element = static_cast<VectorBatch*>(streamRecord->getValue());
            totalRow += element->GetRowCount();
        } else if (dynamic_cast<Watermark*>(streamElement)) {
            totalRow += 1;
        }
    }
    return totalRow;
}

void OmniLocalChannelReader::setRowDataToPtr(
    RowData* binaryRowData,
    uint8_t* dataResultContainer,
    unsigned int& position,
    int vectorBatchCol,
    VectorBatch* element,
    int index)
{
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(vectorBatchCol);
    omnistream::datastream::StreamElementSerializer streamElementSerializer(binaryRowDataSerializer);

    DataOutputSerializer* valueOutputSerializer = new DataOutputSerializer(4);

    OutputBufferStatus* valueOutputBufferStatus = new OutputBufferStatus();
    valueOutputSerializer->setBackendBuffer(valueOutputBufferStatus);
    StreamRecord streamRecord;
    streamRecord.setValue(binaryRowData);
    if (element->getTimestamps() != nullptr) {
        streamRecord.setTimestamp(element->getTimestamp(index));
    }
    valueOutputSerializer->setPosition(4);
    streamElementSerializer.serialize(&streamRecord, *valueOutputSerializer);
    valueOutputSerializer->writeIntUnsafe(valueOutputSerializer->length() - 4, 0);
    *reinterpret_cast<uint32_t*>(dataResultContainer + position) = valueOutputSerializer->length();
    position += 4;
    *reinterpret_cast<uint64_t*>(dataResultContainer + position) =
        reinterpret_cast<uint64_t>(valueOutputSerializer->getData());
    position += 8;

    INFO_RELEASE(
        " length is " << valueOutputSerializer->length() << " data ptr is "
                      << reinterpret_cast<uint64_t>(valueOutputSerializer->getData()));

    valueOutputBufferStatus->ownership = 0;
    delete valueOutputSerializer;
    delete valueOutputBufferStatus;
}

void OmniLocalChannelReader::WrapBufferInfoIntoBinaryRowDataInfo(
    omnistream::VectorBatchBuffer* vBuffer, BufferAndBacklog* bufferAndLog)
{
    int vbNum = vBuffer->GetSize();
    int32_t vectorBatchCol = 0;
    int offset = vBuffer->GetOffset();
    ObjectSegment* objectSegment = vBuffer->GetObjectSegment();

    // Calc total row
    int totalRow = calculateTotalRows(objectSegment, offset, vbNum);
    int totalSize = totalRow * 12; // length(int32_t) + ptr(uint64_t)
    uint8_t* dataResultContainer = new uint8_t[totalSize];
    INFO_RELEASE(
        "totalSize is " << totalSize << " dataResultContainer ptr is "
                        << reinterpret_cast<uint64_t>(dataResultContainer));
    memorySegmentInfo->memorySegmentAddress = reinterpret_cast<uint64_t>(dataResultContainer);

    unsigned int position = 0;
    // Encapsulated BinaryRowData Information
    for (int i = offset; i < vbNum + offset; i++) {
        StreamElement* streamElement = objectSegment->getObject(i);
        if (dynamic_cast<StreamRecord*>(streamElement)) {
            auto* streamRecord = dynamic_cast<StreamRecord*>(streamElement);
            auto* element = static_cast<VectorBatch*>(streamRecord->getValue());
            auto vectorBatchRow = element->GetRowCount();
            vectorBatchCol = element->GetVectorCount();

            for (int j = 0; j < vectorBatchRow; j++) {
                auto binaryRowData = element->extractRowData(j);
                // serializer binaryRowData
                setRowDataToPtr(binaryRowData, dataResultContainer, position, vectorBatchCol, element, j);
            }
        } else if (dynamic_cast<Watermark*>(streamElement)) {
            auto* watermark = dynamic_cast<Watermark*>(streamElement);

            omnistream::datastream::StreamElementSerializer streamElementSerializer(nullptr);
            auto* valueOutputSerializer = new DataOutputSerializer(4);
            auto* valueOutputBufferStatus = new OutputBufferStatus();
            valueOutputSerializer->setBackendBuffer(valueOutputBufferStatus);
            valueOutputSerializer->setPosition(4);
            streamElementSerializer.serialize(watermark, *valueOutputSerializer);
            valueOutputSerializer->writeIntUnsafe(valueOutputSerializer->length() - 4, 0);
            *reinterpret_cast<uint32_t*>(dataResultContainer + position) = valueOutputSerializer->length();
            position += 4;
            *reinterpret_cast<uint64_t*>(dataResultContainer + position) =
                reinterpret_cast<uint64_t>(valueOutputSerializer->getData());
            position += 8;
            valueOutputBufferStatus->ownership = 0;
            delete valueOutputSerializer;
            delete valueOutputBufferStatus;
            // Draining the segment takes ownership of the element and ~ObjectSegment frees only
            // the pointer array, so the watermark is ours. broadcastEmit allocates one per
            // channel per watermark; at 96 bytes and ~2% slab utilisation they are cheap to
            // leak in bytes but pin whole slabs.
            delete watermark;
        }
    }

    // get other msg
    int sequenceNumber = bufferAndLog->getSequenceNumber();
    int backlog = bufferAndLog->getBuffersInBacklog();
    int nextDataType = 0;
    if (bufferAndLog->getNextDataType() == ObjectBufferDataType::DATA_BUFFER) {
        nextDataType = 1;
    } else if (bufferAndLog->getNextDataType() == ObjectBufferDataType::EVENT_BUFFER) {
        nextDataType = 2;
    }
    int currentDataType = 1;
    memorySegmentInfo->readIndex = offset;
    memorySegmentInfo->length = totalRow;
    memorySegmentInfo->currentDataType = currentDataType;
    memorySegmentInfo->backlog = backlog;
    memorySegmentInfo->nextDataType = nextDataType;
    memorySegmentInfo->sequenceNumber = sequenceNumber;
    std::lock_guard<std::recursive_mutex> lock(recycleBufferMutex);
    if (totalRow == 0) {
        vBuffer->RecycleBuffer();
        delete vBuffer;
        delete[] dataResultContainer;
        memorySegmentInfo->memorySegmentAddress = static_cast<uint64_t>(-1);
        return;
    }
    auto batch = std::make_shared<PendingSqlBuffer>(
        PendingSqlBuffer{vBuffer, dataResultContainer, static_cast<size_t>(totalRow)});
    for (int i = 0; i < totalRow; ++i) {
        uint64_t rowAddress;
        std::memcpy(&rowAddress, dataResultContainer + i * 12 + 4, sizeof(rowAddress));
        pendingSqlRows.emplace(rowAddress, batch);
    }
}
} // namespace omnistream
