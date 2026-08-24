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

#include "runtime/watermark/WatermarkStatus.h"
#include "RemoteInputChannel.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "common.h"
#include "buffer/ReadOnlySlicedVectorBatchBuffer.h"
#include "runtime/buffer/NetworkBuffer.h"
#include "runtime/buffer/VectorBatchBuffer.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include <buffer/ReadOnlySlicedNetworkBuffer.h>
#include "core/include/omni_const.h"
#include <exception>

namespace omnistream {
namespace {
void RecycleCheckpointBuffers(std::vector<Buffer*>& buffers) noexcept
{
    std::vector<Buffer*> buffersToRecycle;
    buffersToRecycle.swap(buffers);
    for (Buffer* buffer : buffersToRecycle) {
        if (buffer == nullptr) {
            continue;
        }
        try {
            buffer->RecycleBuffer();
        } catch (const std::exception& e) {
            INFO_RELEASE("ERROR: Failed to recycle an inflight checkpoint buffer: " << e.what());
        } catch (...) {
            INFO_RELEASE("ERROR: Failed to recycle an inflight checkpoint buffer due to an unknown exception");
        }
    }
}

void RetainVectorBatchBufferForCheckpoint(VectorBatchBuffer* source, std::vector<Buffer*>& target)
{
    if (source == nullptr || source->GetObjectSegment() == nullptr) {
        return;
    }

    Buffer* retainedBuffer = source->RetainBuffer();
    if (retainedBuffer == nullptr) {
        return;
    }
    try {
        target.push_back(retainedBuffer);
    } catch (const std::exception& e) {
        INFO_RELEASE("ERROR: Failed to retain a VectorBatchBuffer for checkpoint: " << e.what());
        retainedBuffer->RecycleBuffer();
        throw;
    } catch (...) {
        INFO_RELEASE("ERROR: Failed to retain a VectorBatchBuffer for checkpoint due to an unknown exception");
        retainedBuffer->RecycleBuffer();
        throw;
    }
}
} // namespace

RemoteInputChannel::RemoteInputChannel(
    std::shared_ptr<SingleInputGate> inputGate,
    int channelIndex,
    ResultPartitionIDPOD partitionId,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    int initialBackoff,
    int maxBackoff,
    int networkBuffersPerChannel,
    std::shared_ptr<Counter> numBytesIn,
    std::shared_ptr<Counter> numBuffersIn,
    std::shared_ptr<ChannelStateWriter> stateWriter)
    : LocalInputChannel(
          inputGate,
          channelIndex,
          partitionId,
          partitionManager,
          initialBackoff,
          maxBackoff,
          numBytesIn,
          numBuffersIn,
          stateWriter),
      initialCredit(networkBuffersPerChannel)
{
}

void RemoteInputChannel::requestSubpartition(int subpartitionIndex)
{
    // remote version, no need to implement
}

void RemoteInputChannel::notifyRemoteDataAvailableForVectorBatch(
    long bufferAddress,
    int bufferLength,
    int sequenceNumber,
    const std::shared_ptr<OriginalNetworkBufferRecycler>& originalNetworkBufferRecycle)
{
    std::unique_lock<std::recursive_mutex> lock(queueMutex);
    if (isReleased()) {
        lock.unlock();
        if (bufferAddress != -1 && originalNetworkBufferRecycle != nullptr) {
            originalNetworkBufferRecycle->recycle(bufferAddress);
        }
        return;
    }
    taskType = 1;
    if (bufferAddress == -1) {
        // event
        int eventType = bufferLength;
        LOG("remote got an event data:::: event type: " << eventType);
        INFO_RELEASE("remote got an event data:::: event type: " << eventType);
        auto eventData = new VectorBatchBuffer(eventType);
        if (eventData != nullptr) {
            this->dataQueue.push(eventData);
        }
    } else {
        uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferAddress);
        int32_t vertorBatchNum = 0;
        memcpy_s(&vertorBatchNum, sizeof(int32_t), buffer, sizeof(int32_t));
        // do data deserialization
        std::shared_ptr<ObjectSegment> objectSegment = this->DoDataDeserializationResult(buffer);
        objectSegment->setData(reinterpret_cast<uint8_t*>(bufferAddress), static_cast<size_t>(bufferLength));
        auto vectorBatchBuffer = new VectorBatchBuffer(objectSegment, originalNetworkBufferRecycle);
        auto readOnlyVectorBatchBuffer = new ReadOnlySlicedVectorBatchBuffer(vectorBatchBuffer, 0, vertorBatchNum);

        if (isNeedExpansion && (sequenceNumber > lastSequenceNumber)) {
            isNeedExpansion = false;
        }
        if (vectorBatchBuffer != nullptr) {
            vectorBatchBuffer->SetSize(objectSegment->getSize());
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            this->dataQueue.push(readOnlyVectorBatchBuffer);
            LOG("remote got an buffer  " << readOnlyVectorBatchBuffer->ToDebugString(true));
            if (isNeedPersistence_ || isNeedExpansion) {
                if (readOnlyVectorBatchBuffer != nullptr && readOnlyVectorBatchBuffer->GetObjectSegment() != nullptr) {
                    channelStatePersister->MaybePersist(readOnlyVectorBatchBuffer);
                }
            }
        }
        auto vectorBatchCount = readOnlyVectorBatchBuffer->GetSize();
        insize += vectorBatchCount;
        if (!isNeedExpansion) {
            lastSequenceNumber = sequenceNumber;
        }
    }
    lock.unlock();
    this->notifyDataAvailable();
}

std::optional<BufferAndAvailability> RemoteInputChannel::getNextBuffer()
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    if (isReleased()) {
        return std::nullopt;
    }
    if (this->dataQueue.size() == 0) {
        return std::nullopt;
    }

    auto buffer = this->dataQueue.front();
    this->dataQueue.pop();
    datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
        dynamic_cast<datastream::ReadOnlySlicedNetworkBuffer*>(buffer);
    ObjectBufferDataType dataType = ObjectBufferDataType::NONE;
    outsize += buffer->GetSize();
    int backlogSize = static_cast<int>(this->dataQueue.size());
    if (backlogSize > 0) {
        dataType = ObjectBufferDataType::DATA_BUFFER;
    }
    if (readOnlyBuffer != nullptr && readOnlyBuffer->GetBufferType() == 3) {
        dataType = ObjectBufferDataType::NONE;
        readOnlyBuffer->SetBufferType(1);
    }
    // std::shared_ptr<ObjectBuffer> data = std::shared_ptr<ObjectBuffer>(vectorBatchBuffer);

    return BufferAndAvailability{buffer, dataType, backlogSize, expectSequenceNumber++};
}

int RemoteInputChannel::unsynchronizedGetNumberOfQueuedBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    return static_cast<int>(dataQueue.size());
}

int RemoteInputChannel::unsynchronizedGetSizeOfQueuedBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::queue<Buffer*> tmpQueue = dataQueue;
    int num = tmpQueue.size();
    int size = 0;
    for (int i = 0; i < num; i++) {
        Buffer* buffer = tmpQueue.front();
        size += buffer->GetSize();
        tmpQueue.pop();
    }
    return size;
}

bool RemoteInputChannel::IsNeedPersistence()
{
    // outsize is the cumulative amount delivered from this channel. Once the
    // data present when the barrier arrived has been consumed, the next
    // checkpoint must not persist buffers after that barrier.
    return startSize_ == 0 || outsize < startSize_;
}

std::shared_ptr<ObjectSegment> RemoteInputChannel::DoDataDeserializationResult(uint8_t*& buffer)
{
    LOG("----DoDataDeserializationResult start 1:: " << buffer);
    int32_t elementNum;
    memcpy_s(&elementNum, sizeof(int32_t), buffer, sizeof(int32_t));
    buffer += sizeof(int32_t);
    std::shared_ptr<ObjectSegment> objectSegment = std::make_shared<ObjectSegment>(elementNum);
    LOG("----DoDataDeserializationResult start 2:: " << buffer);
    for (int32_t i = 0; i < elementNum; i++) {
        int8_t dataType;
        memcpy_s(&dataType, sizeof(int8_t), buffer, sizeof(int8_t));
        buffer += sizeof(int8_t);
        LOG("----DoDataDeserializationResult start 3:: " << buffer);
        StreamElementTag tagType = static_cast<StreamElementTag>(dataType);
        switch (tagType) {
            case StreamElementTag::TAG_STREAM_STATUS: {
                int32_t status;
                memcpy_s(&status, sizeof(status), buffer, sizeof(status));
                buffer += sizeof(status);
                objectSegment->putObject(i, new WatermarkStatus(status));
                break;
            }
            case StreamElementTag::TAG_WATERMARK: {
                long timestamp = VectorBatchDeserializationUtils::derializeWatermark(buffer);
                LOG("RemoteInputChannel::DoDataDeserializationResult:: deserialize watermark :: " << timestamp);
                Watermark* watermark = new Watermark(timestamp);
                objectSegment->putObject(i, watermark);
                break;
            }
            case StreamElementTag::VECTOR_BATCH: {
                LOG("RemoteInputChannel::DoDataDeserializationResult:: deserialize VECTOR_BATCH ");
                VectorBatch* vb = VectorBatchDeserializationUtils::deserializeVectorBatch(buffer);
                StreamRecord* streamRecord = new StreamRecord(vb);
                objectSegment->putObject(i, streamRecord);
                break;
            }
            case StreamElementTag::BINARY_ROW_DATA: {
                LOG("RemoteInputChannel::DoDataDeserializationResult:: deserialize BinaryRowData " );
                BinaryRowData* row = BinaryRowDataSerializationUtils::deserializeBinaryRowData(buffer); 
                StreamRecord* streamRecord = new StreamRecord(row);
                streamRecord->setValueType(RecordValueType::BINARY_ROW_DATA);
                objectSegment->putObject(i, streamRecord);
                break;
            }
            default: {
                LOG("Warning: cannt deserialize the records" );
                break;
            }
        }
    }
    return objectSegment;
    // no need to implement
}

void RemoteInputChannel::notifyRemoteDataAvailableForNetworkBuffer(
    long bufferAddress,
    int bufferLength,
    int readIndex,
    int sequenceNumber,
    std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler,
    bool isBuffer,
    int bufferType)
{
    if (bufferLength > IO_SIZE_512M) {
        INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
        originalNetworkBufferRecycler->recycle(bufferAddress);
        return;
    }
    int type = bufferType;
    if (bufferType > 1) {
        isBlockedByCheckpoint_.store(true);
        type = 1;
    }
    LOG("notifyRemoteDataAvailableForDataStream bufferAddress: " << bufferAddress << " bufferLength: " << bufferLength
                                                                 << " sequenceNumber: " << sequenceNumber);
    MemorySegment* memorySegment = new MemorySegment(reinterpret_cast<uint8_t*>(bufferAddress), bufferLength, this);
    datastream::NetworkBuffer* networkBuffer = new datastream::NetworkBuffer(
        memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler, type, true);
    datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
        new datastream::ReadOnlySlicedNetworkBuffer(networkBuffer, readIndex, bufferLength);

    std::unique_lock<std::recursive_mutex> lock(queueMutex);
    bool wasEmpty = this->dataQueue.empty();
    if (bufferType == 3) {
        readOnlyBuffer->SetBufferType(bufferType);
    }
    if (readOnlyBuffer != nullptr) {
        insize += bufferLength;
        this->dataQueue.push(readOnlyBuffer);
        if (isNeedExpansion && (sequenceNumber > lastSequenceNumber)) {
            isNeedExpansion = false;
        }
        if ((isNeedPersistence_ && (readOnlyBuffer->isBuffer())) || (isNeedExpansion && (readOnlyBuffer->isBuffer()))) {
            uint8_t* newBufferAddress = new uint8_t[bufferLength];
            MemorySegment* newMemorySegment = new MemorySegment(newBufferAddress, bufferLength);
            newMemorySegment->put(0, reinterpret_cast<uint8_t*>(bufferAddress), readIndex, bufferLength);
            ::datastream::NetworkBuffer* newNetworkBuffer = new ::datastream::NetworkBuffer(
                newMemorySegment,
                bufferLength,
                0,
                std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER,
                true);

            inflightBuffers_.push_back(newNetworkBuffer);
        }
        if (!readOnlyBuffer->isBuffer()) {
            std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
            if (event->GetEventClassName() == "CheckpointBarrier") {
                startSize_ = insize;
                if (isNeedPersistence_ && sequenceNumber > lastSequenceNumber + 1) {
                    isNeedExpansion = true;
                    lastSequenceNumber = sequenceNumber;
                }
                isNeedPersistence_ = false;
            }
        }
        if (!isNeedExpansion) {
            lastSequenceNumber = sequenceNumber;
        }
    }
    if (!isBuffer) {
        INFO_RELEASE(
            "REMOTE_EVENT_ENQUEUE gate=" << getChannelInfo().getGateIdx() << " channel=" << getChannelIndex()
                                         << " sequence=" << sequenceNumber << " wasEmpty=" << wasEmpty
                                         << " queueSize=" << dataQueue.size());
    }
    lock.unlock();

    if (wasEmpty) {
        this->notifyDataAvailable();
    }
}

void RemoteInputChannel::SetRemoteDataFetcherBridge(std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge)
{
    this->remoteDataFetcherBridge = remoteDataFetcherBridge;
}

void RemoteInputChannel::resumeConsumption()
{
    if (this->remoteDataFetcherBridge == nullptr) {
        LOG("RemoteInputChannel::resumeConsumption: remoteDataFetcherBridge is null");
        return;
    }
    int gateIndex = this->getChannelInfo().getGateIdx();
    int channelIndex = this->getChannelInfo().getInputChannelIdx();
    // Clear the hint before the bridge call so a newly arriving blocking event is not
    // overwritten after the producer has been resumed.
    isBlockedByCheckpoint_.store(false);
    this->remoteDataFetcherBridge->InvokeJavaRemoteDataFetcherResumeConsumption(gateIndex, channelIndex);
}

void RemoteInputChannel::TimeOutResumeConsumption()
{
    if (this->remoteDataFetcherBridge == nullptr) {
        LOG("RemoteInputChannel::resumeConsumption: remoteDataFetcherBridge is null");
        return;
    }
    int gateIndex = this->getChannelInfo().getGateIdx();
    int channelIndex = this->getChannelInfo().getInputChannelIdx();
    if (isBlockedByCheckpoint_.exchange(false)) {
        this->remoteDataFetcherBridge->InvokeJavaRemoteDataFetcherResumeConsumption(gateIndex, channelIndex);
    }
}

void RemoteInputChannel::releaseAllResources()
{
    LocalInputChannel::releaseAllResources();

    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    RecycleCheckpointBuffers(inflightBuffers_);
    while (!dataQueue.empty()) {
        Buffer* buffer = dataQueue.front();
        dataQueue.pop();
        if (buffer == nullptr) {
            continue;
        }
        try {
            buffer->RecycleBuffer();
        } catch (const std::exception& e) {
            INFO_RELEASE("ERROR: Failed to recycle a queued remote input buffer: " << e.what());
        } catch (...) {
            INFO_RELEASE("ERROR: Failed to recycle a queued remote input buffer due to an unknown exception");
        }
        delete buffer;
    }
}

void RemoteInputChannel::CheckpointStarted(
    const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    if (barrier.GetId() < lastBarrierId_) {
        LOG("Barrier id is too small");
        return;
    } else if (barrier.GetId() > lastBarrierId_) {
        ResetLastBarrier();
    }
    RecycleCheckpointBuffers(inflightBuffers_);
    if (channelStatePersister == nullptr) {
        SetChannelStateWriter(channelStateWriter);
    }
    std::vector<Buffer*> knownBuffers;
    try {
        if (IsNeedPersistence()) {
            if (taskType == 1) {
                knownBuffers = GetInflightVectorBatchBuffersUnsafe(barrier.GetId());
            } else {
                knownBuffers = GetInflightBuffersUnsafe(barrier.GetId());
            }
        }
        channelStatePersister->StartPersisting(barrier.GetId(), knownBuffers);
        // Ownership is transferred to ChannelStateWriter.
        knownBuffers.clear();
    } catch (const std::exception& e) {
        INFO_RELEASE("ERROR: Failed to start persisting remote input channel state: " << e.what());
        RecycleCheckpointBuffers(knownBuffers);
        throw;
    } catch (...) {
        INFO_RELEASE("ERROR: Failed to start persisting remote input channel state due to an unknown exception");
        RecycleCheckpointBuffers(knownBuffers);
        throw;
    }
}

void RemoteInputChannel::CheckpointStopped(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    RecycleCheckpointBuffers(inflightBuffers_);
    if (lastBarrierId_ == checkpointId) {
        ResetLastBarrier();
    }
    startSize_ = 0;
    if (channelStatePersister != nullptr) {
        channelStatePersister->StopPersisting(checkpointId);
    }
}

void RemoteInputChannel::AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    if (channelStatePersister == nullptr) {
        RecycleCheckpointBuffers(inflightBuffers_);
        THROW_RUNTIME_ERROR("RemoteInputChannel::AddInputData called without a channel state persister");
    }

    try {
        if (taskType == 1) {
            // SQL 已通过 MaybePersist 逐个转交。
            // 理论上这里应为空；防止异常残留引用被直接 clear 后泄漏。
            RecycleCheckpointBuffers(inflightBuffers_);
            return;
        }
        channelStatePersister->AddInputData(inflightBuffers_, checkpointId, info);
        // Ownership is transferred to ChannelStateWriter. It recycles each buffer after writing or when the request
        // exits exceptionally.
        inflightBuffers_.clear();
    } catch (const std::exception& e) {
        INFO_RELEASE("ERROR: Failed to add remote inflight buffers to channel state: " << e.what());
        RecycleCheckpointBuffers(inflightBuffers_);
        throw;
    } catch (...) {
        INFO_RELEASE("ERROR: Failed to add remote inflight buffers to channel state due to an unknown exception");
        RecycleCheckpointBuffers(inflightBuffers_);
        throw;
    }
}

std::vector<Buffer*> RemoteInputChannel::GetInflightVectorBatchBuffersUnsafe(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::vector<Buffer*> inflightBuffers;
    try {
        std::queue<Buffer*> tmpQueue = dataQueue;
        while (!tmpQueue.empty()) {
            Buffer* buffer = tmpQueue.front();
            VectorBatchBuffer* vectorBatchBuffer = dynamic_cast<VectorBatchBuffer*>(buffer);
            datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
                dynamic_cast<datastream::ReadOnlySlicedNetworkBuffer*>(buffer);
            if (vectorBatchBuffer == nullptr && readOnlyBuffer == nullptr) {
                tmpQueue.pop();
                continue;
            }
            if (vectorBatchBuffer != nullptr) {
                RetainVectorBatchBufferForCheckpoint(vectorBatchBuffer, inflightBuffers);
                tmpQueue.pop();
                continue;
            }
            if (startSize_ != 0) {
                std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
                if (event->GetEventClassName() == "CheckpointBarrier") {
                    isNeedPersistence_ = false;
                    break;
                }
            }
            tmpQueue.pop();
        }
    } catch (const std::exception& e) {
        INFO_RELEASE("ERROR: Failed to collect inflight VectorBatchBuffers for checkpoint: " << e.what());
        // These retained references have not been returned to ChannelStatePersister.
        RecycleCheckpointBuffers(inflightBuffers);
        throw;
    } catch (...) {
        INFO_RELEASE("ERROR: Failed to collect inflight VectorBatchBuffers for checkpoint due to an unknown exception");
        // These retained references have not been returned to ChannelStatePersister.
        RecycleCheckpointBuffers(inflightBuffers);
        throw;
    }
    LOG("RemoteInputChannel get inflight buffers success, buffer num:" << inflightBuffers.size()
                                                                       << ", checkpointId: " << checkpointId);
    return inflightBuffers;
}

std::vector<Buffer*> RemoteInputChannel::GetInflightBuffersUnsafe(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::vector<Buffer*> inflightBuffers;
    std::queue<Buffer*> tmpQueue = dataQueue;
    while (!tmpQueue.empty()) {
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
            static_cast<datastream::ReadOnlySlicedNetworkBuffer*>(tmpQueue.front());
        if (readOnlyBuffer == nullptr) {
            tmpQueue.pop();
            continue;
        }
        auto buffer = readOnlyBuffer->GetNetWorkBuffer();
        int offset = readOnlyBuffer->GetMemorySegmentOffset();
        int bufferLength = buffer->GetSize();
        auto oldmemorySegment = dynamic_cast<MemorySegment*>(buffer->GetSegment());
        if (readOnlyBuffer->isBuffer()) {
            if (bufferLength > IO_SIZE_512M) {
                INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
                tmpQueue.pop();
                continue;
            }
            uint8_t* bufferAddress = new uint8_t[bufferLength];
            MemorySegment* memorySegment = new MemorySegment(bufferAddress, bufferLength);
            memorySegment->put(0, oldmemorySegment->getData(), offset, bufferLength);
            ::datastream::NetworkBuffer* networkBuffer = new ::datastream::NetworkBuffer(
                memorySegment,
                bufferLength,
                0,
                std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER,
                true);

            inflightBuffers.push_back(networkBuffer);
            tmpQueue.pop();
            continue;
        }
        if (startSize_ != 0) {
            std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(readOnlyBuffer);
            if (event->GetEventClassName() == "CheckpointBarrier") {
                isNeedPersistence_ = false;
                break;
            }
        }
        tmpQueue.pop();
    }
    LOG("RemoteInputChannel get inflight buffers success, buffer num:" << inflightBuffers.size()
                                                                       << ", checkpointId: " << checkpointId);
    return inflightBuffers;
}

} // namespace omnistream
