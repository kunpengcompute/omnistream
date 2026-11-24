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

#include "OmniLocalInputChannel.h"

#include <buffer/ReadOnlySlicedNetworkBuffer.h>

#include "buffer/NetworkBuffer.h"
#include "buffer/ReadOnlySlicedNetworkBuffer.h"
#include "core/include/omni_const.h"

namespace omnistream {
OmniLocalInputChannel::OmniLocalInputChannel(
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
          stateWriter)
{
    originalNetworkBufferRecycler_ = std::make_shared<OriginalNetworkBufferRecycler>();
    channelInfo.setOmni();
    LOG("Get OmniLocalInputChannel omni: " << channelInfo.getOmni());
}

void OmniLocalInputChannel::notifyOriginalDataAvailable(
    long bufferAddress, int bufferLength, int readIndex, int sequenceNumber, int memorySegmentOffset, int bufferType)
{
    if (bufferLength > IO_SIZE_512M) {
        INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
        originalNetworkBufferRecycler_->recycle(bufferAddress);
        return;
    }
    int type = bufferType;
    if (bufferType > 1) {
        isBlockedByCheckpoint_.store(true);
        type = 1;
    }
    MemorySegment* memorySegment = new MemorySegment(reinterpret_cast<uint8_t*>(bufferAddress), bufferLength, this);
    datastream::NetworkBuffer* networkBuffer = new datastream::NetworkBuffer(
        memorySegment, bufferLength, readIndex, originalNetworkBufferRecycler_, type, true);
    datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
        new datastream::ReadOnlySlicedNetworkBuffer(networkBuffer, readIndex + memorySegmentOffset, bufferLength);
    std::unique_lock<std::recursive_mutex> lock(queueMutex);
    ObjectBufferDataType dataType = ObjectBufferDataType::DATA_BUFFER;
    if (bufferType == 3) {
        dataType = ObjectBufferDataType::NONE;
    }
    std::shared_ptr<BufferAndAvailability> data =
        std::make_shared<BufferAndAvailability>(readOnlyBuffer, dataType, dataQueue.size(), sequenceNumber);
    if (data != nullptr) {
        dataQueue.push(data);
        insize += bufferLength;
        if (isNeedPersistence_ && (readOnlyBuffer->isBuffer())) {
            uint8_t* newBufferAddress = new uint8_t[bufferLength];
            MemorySegment* newMemorySegment = new MemorySegment(newBufferAddress, bufferLength);
            newMemorySegment->put(
                0, reinterpret_cast<uint8_t*>(bufferAddress), readIndex + memorySegmentOffset, bufferLength);
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
                isNeedPersistence_ = false;
                startSize_ = insize;
            }
        }
    }
    // INFO_RELEASE("dataQueue size: " + std::to_string(dataQueue.size()))
    lock.unlock();
    notifyDataAvailable();
}

std::optional<BufferAndAvailability> OmniLocalInputChannel::getNextBuffer()
{
    std::unique_lock<std::recursive_mutex> lock(queueMutex);
    if (dataQueue.empty()) {
        return std::nullopt;
    }
    auto buffer = dataQueue.front();
    dataQueue.pop();
    datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
        (datastream::ReadOnlySlicedNetworkBuffer*)buffer->GetBuffer();
    outsize += buffer->GetBuffer()->GetSize();
    lock.unlock();
    return std::optional<BufferAndAvailability>{*buffer};
}

int OmniLocalInputChannel::unsynchronizedGetNumberOfQueuedBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    return static_cast<int>(dataQueue.size());
}

int OmniLocalInputChannel::unsynchronizedGetSizeOfQueuedBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::queue<std::shared_ptr<BufferAndAvailability>> tmpQueue = dataQueue;
    int num = tmpQueue.size();
    int size = 0;
    for (int i = 0; i < num; i++) {
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
            static_cast<datastream::ReadOnlySlicedNetworkBuffer*>(tmpQueue.front()->GetBuffer());
        if (readOnlyBuffer == nullptr) {
            tmpQueue.pop();
            continue;
        }
        size += readOnlyBuffer->GetSize();
        tmpQueue.pop();
    }
    return size;
}

size_t OmniLocalInputChannel::getConsumedBufferSize() const
{
    return outsize;
}

void OmniLocalInputChannel::requestSubpartition(int subpartitionIndex)
{
    // do nothing, because OmniLocalInputChannel target subpartition result is in java side
    LOG("OmniLocalInputChannel::requestSubpartition called, but it does nothing.");
}

long OmniLocalInputChannel::GetRecycleBufferAddress()
{
    return originalNetworkBufferRecycler_->getRecycleBufferAddress();
}

void OmniLocalInputChannel::releaseAllResources()
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    isNeedPersistence_ = false;
    startSize_ = 0;
    ReleaseInflightBuffers(inflightBuffers_);
    originalNetworkBufferRecycler_->stop();
}

void OmniLocalInputChannel::resumeConsumption()
{
    // Clear the hint before the bridge call so a newly arriving blocking event is not
    // overwritten after the producer has been resumed.
    isBlockedByCheckpoint_.store(false);
    omniLocalInputChannelBridge->InvokeDoResumeConsumption();
}

void OmniLocalInputChannel::TimeOutResumeConsumption()
{
    if (isBlockedByCheckpoint_.exchange(false)) {
        omniLocalInputChannelBridge->InvokeDoResumeConsumption();
    }
}

void OmniLocalInputChannel::SetOmniLocalInputChannelBridge(std::shared_ptr<OmniLocalInputChannelBridge> bridge)
{
    omniLocalInputChannelBridge = bridge;
}

void OmniLocalInputChannel::CheckpointStarted(
    const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    ReleaseInflightBuffers(inflightBuffers_);
    std::vector<Buffer*> knownBuffers;
    if (IsNeedPersistence()) {
        knownBuffers = GetInflightBuffersUnsafe(barrier.GetId());
    }
    if (channelStatePersister == nullptr) {
        SetChannelStateWriter(channelStateWriter);
    }
    channelStatePersister->StartPersisting(barrier.GetId(), knownBuffers);
}

void OmniLocalInputChannel::CheckpointStopped(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    isNeedPersistence_ = false;
    startSize_ = 0;
    ReleaseInflightBuffers(inflightBuffers_);
    if (!channelStatePersister) {
        LOG("OmniLocalInputChannel::CheckpointStopped skipped because channelStatePersister is not initialized, "
            "checkpointId="
            << checkpointId);
        return;
    }
    channelStatePersister->StopPersisting(checkpointId);
}
void OmniLocalInputChannel::AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    // The writer also recycles the transferred buffers if submission fails.
    std::vector<Buffer*> buffers;
    buffers.swap(inflightBuffers_);
    channelStatePersister->AddInputData(std::move(buffers), checkpointId, info);
}

std::vector<Buffer*> OmniLocalInputChannel::GetInflightBuffersUnsafe(long checkpointId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    std::vector<Buffer*> inflightBuffers;
    std::queue<std::shared_ptr<BufferAndAvailability>> tmpQueue = dataQueue;
    while (!tmpQueue.empty()) {
        datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer =
            static_cast<datastream::ReadOnlySlicedNetworkBuffer*>(tmpQueue.front()->GetBuffer());
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
