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
#include "RecoveredInputChannel.h"

#include "runtime/buffer/ReadOnlySlicedVectorBatchBuffer.h"
#include "buffer/ReadOnlySlicedNetworkBuffer.h"
#include "event/InnerRecoverEvent.h"
#include "buffer/VectorBatchBuffer.h"
#include "buffer/NetworkObjectBufferPool.h"
#include "runtime/buffer/NetworkBuffer.h"
#include "core/memory/MemorySegment.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

std::shared_ptr<omnistream::InputChannel> RecoveredInputChannel::toInputChannel()
{
    if (!stateConsumedFuture->IsDone()) {
        LOG("recovery not completed, do not convert to normal channel!");
        throw std::runtime_error("recovery not completed, do not convert to normal channel!");
    }
    if (!stateConsumedFuture1.load()) {
        LOG("recovery not completed, do not convert to normal channel!");
        throw std::runtime_error("recovery not completed, do not convert to normal channel!");
    }

    {
        std::lock_guard<std::mutex> lock(bufferLock);
        consumedRecoveredBufferOwners.clear();
    }

    std::shared_ptr<omnistream::InputChannel> inputChannel = toInputChannelInternal();
    inputChannel->CheckpointStopped(lastStoppedCheckpointId);
    return inputChannel;
}

void RecoveredInputChannel::onRecoveredStateBuffer(Buffer* buffer)
{
    bool recycleBuffer = true;
    bool wasEmpty = false;
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            wasEmpty = receivedBuffers.empty();
            receivedBuffers.emplace_back(buffer, nullptr);
            recycleBuffer = false;
        }
    }

    if (wasEmpty) {
        notifyChannelNonEmpty();
    }

    if (recycleBuffer && buffer != nullptr) {
        buffer->RecycleBuffer();
    }
}

void RecoveredInputChannel::onRecoveredStateBuffer2(Buffer* buffer)
{
    if (isObjectBufferPool()) {
        onRecoveredStateBufferForObjectBuffer(buffer);
        return;
    }
    bool recycleBuffer = true;
    bool wasEmpty = false;
    ReadOnlySlicedNetworkBuffer* readOnlyBuffer;
    {
        readOnlyBuffer = new ReadOnlySlicedNetworkBuffer(dynamic_cast<NetworkBuffer*>(buffer), 0, buffer->GetSize());
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            wasEmpty = receivedBuffers.empty();
            receivedBuffers.emplace_back(readOnlyBuffer, nullptr);
            recycleBuffer = false;
        }
    }

    if (wasEmpty) {
        notifyChannelNonEmpty();
    }

    if (recycleBuffer && readOnlyBuffer != nullptr) {
        readOnlyBuffer->RecycleBuffer();
    }
}

void RecoveredInputChannel::onRecoveredStateBufferForObjectBuffer(Buffer* buffer)
{
    bool recycleBuffer = true;
    bool wasEmpty = false;
    auto* memorySegment = dynamic_cast<MemorySegment*>(buffer->GetSegment());
    if (memorySegment == nullptr) {
        LOG("onRecoveredStateBufferForObjectBuffer: buffer segment is not MemorySegment, fallback to original path");
        Buffer* readOnlyBuffer;
        {
            readOnlyBuffer = new omnistream::ReadOnlySlicedVectorBatchBuffer(
                dynamic_cast<omnistream::VectorBatchBuffer*>(buffer), 0, buffer->GetSize());
            std::lock_guard<std::mutex> lock(bufferLock);
            if (!released) {
                wasEmpty = receivedBuffers.empty();
                receivedBuffers.emplace_back(readOnlyBuffer, nullptr);
                recycleBuffer = false;
            }
        }
        if (wasEmpty) {
            notifyChannelNonEmpty();
        }
        if (recycleBuffer && readOnlyBuffer != nullptr) {
            readOnlyBuffer->RecycleBuffer();
            delete readOnlyBuffer;
        }
        return;
    }

    uint8_t* data = memorySegment->getData();
    int dataLength = buffer->GetSize();
    uint8_t* dataPtr = data;
    int32_t elementNum;
    memcpy_s(&elementNum, sizeof(int32_t), dataPtr, sizeof(int32_t));
    dataPtr += sizeof(int32_t);
    std::shared_ptr<omnistream::ObjectSegment> objectSegment = std::make_shared<omnistream::ObjectSegment>(elementNum);
    for (int32_t i = 0; i < elementNum; i++) {
        int8_t dataType;
        memcpy_s(&dataType, sizeof(int8_t), dataPtr, sizeof(int8_t));
        dataPtr += sizeof(int8_t);
        StreamElementTag tagType = static_cast<StreamElementTag>(dataType);
        switch (tagType) {
            case StreamElementTag::TAG_STREAM_STATUS: {
                int32_t status;
                memcpy_s(&status, sizeof(status), dataPtr, sizeof(status));
                dataPtr += sizeof(status);
                objectSegment->putObject(i, new WatermarkStatus(status));
                break;
            }
            case StreamElementTag::TAG_WATERMARK: {
                long timestamp = omnistream::VectorBatchDeserializationUtils::derializeWatermark(dataPtr);
                Watermark* watermark = new Watermark(timestamp);
                objectSegment->putObject(i, watermark);
                break;
            }
            case StreamElementTag::VECTOR_BATCH: {
                omnistream::VectorBatch* vb =
                    omnistream::VectorBatchDeserializationUtils::deserializeVectorBatch(dataPtr);
                StreamRecord* streamRecord = new StreamRecord(vb);
                objectSegment->putObject(i, streamRecord);
                break;
            }
            default: break;
        }
    }

    auto* vectorBatchBuffer = new omnistream::VectorBatchBuffer(objectSegment, nullptr);
    vectorBatchBuffer->SetSize(objectSegment->getSize());
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            wasEmpty = receivedBuffers.empty();
            receivedBuffers.emplace_back(vectorBatchBuffer, nullptr);
            recycleBuffer = false;
        }
    }

    if (wasEmpty) {
        notifyChannelNonEmpty();
    }

    if (recycleBuffer && vectorBatchBuffer != nullptr) {
        delete vectorBatchBuffer;
    }
}

bool RecoveredInputChannel::isObjectBufferPool()
{
    auto gate = inputGate;
    if (!gate) {
        return false;
    }
    auto segmentProvider = gate->getSegmentProvider();
    if (!segmentProvider) {
        return false;
    }
    return std::dynamic_pointer_cast<omnistream::NetworkObjectBufferPool>(segmentProvider) != nullptr;
}

void RecoveredInputChannel::finishReadRecoveredState()
{
    LOG("Recovered input channel finishReadRecoveredState!");
    NetworkBuffer* networkBuffer = omnistream::EventSerializer::toBuffer(EndOfChannelStateEvent::getInstance(), false);
    if (networkBuffer != nullptr) {
        onRecoveredStateBuffer(networkBuffer);
        bufferManager->releaseFloatingBuffers();
        LOG(inputGate->getOwningTaskName() << "/" << channelInfo.toString() << " finished recovering input!");
    }
}

void RecoveredInputChannel::finishInnerRecoveredState()
{
    INFO_RELEASE("Recovered input channel finishInnerRecoveredState!");
    NetworkBuffer* networkBuffer = omnistream::EventSerializer::toBuffer(InnerRecoverEvent::getInstance(), false);
    if (networkBuffer != nullptr) {
        onRecoveredStateBuffer(networkBuffer);
        bufferManager->releaseFloatingBuffers();
        LOG(inputGate->getOwningTaskName() << "/" << channelInfo.toString() << " finished recovering input!");
    }
}

std::optional<omnistream::BufferAndAvailability> RecoveredInputChannel::getNextRecoveredStateBuffer()
{
    LOG("Recovered input channel get Next record buffer!");
    Buffer* next = nullptr;
    omnistream::ObjectBufferDataType nextDataType;

    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (released) {
            LOG("Trying to read from released RecoveredInputChannel");
        }

        if (receivedBuffers.empty()) {
            return std::nullopt;
        }

        auto entry = std::move(receivedBuffers.front());
        receivedBuffers.pop_front();

        next = entry.buffer;
        if (entry.owner) {
            consumedRecoveredBufferOwners.emplace_back(std::move(entry.owner));
        }

        nextDataType = peekDataTypeUnsafe();
    }

    if (next == nullptr) {
        LOG("Recovered input channel next ele is null! to test, send a end of recover event");
        finishInnerRecoveredState();
        return std::nullopt;
    } else if (isEndOfChannelStateEvent(next)) {
        LOG("Recovered input channel end of event!");
        return std::nullopt;
    } else if (isInnerRecoverEvent(next)) {
        INFO_RELEASE("recovered input channel received InnerRecoverEvent!");
        stateConsumedFuture->Complete();
        stateConsumedFuture1.store(true);
        return omnistream::BufferAndAvailability{next, nextDataType, 0, sequenceNumber++};
    } else {
        return omnistream::BufferAndAvailability{next, nextDataType, 0, sequenceNumber++};
    }
}

omnistream::ObjectBufferDataType RecoveredInputChannel::peekDataTypeUnsafe()
{
    if (receivedBuffers.empty()) {
        return ObjectBufferDataType(ObjectBufferDataType::NONE);
    }
    if (receivedBuffers.front().buffer == nullptr) {
        return ObjectBufferDataType(ObjectBufferDataType::NONE);
    }
    return ObjectBufferDataType(receivedBuffers.front().buffer->GetDataType());
}

bool RecoveredInputChannel::isEndOfChannelStateEvent(Buffer* buffer)
{
    if (buffer->isBuffer()) {
        return false;
    }

    std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(buffer);
    buffer->SetReaderIndex(0);
    if (dynamic_cast<EndOfChannelStateEvent*>(event.get())) {
        return true;
    }
    return false;
}

bool RecoveredInputChannel::isInnerRecoverEvent(Buffer* buffer)
{
    if (buffer->isBuffer()) {
        return false;
    }

    std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(buffer);
    buffer->SetReaderIndex(0);
    if (dynamic_cast<InnerRecoverEvent*>(event.get())) {
        return true;
    }
    return false;
}

std::optional<BufferAndAvailability> RecoveredInputChannel::getNextBuffer()
{
    checkError();
    return getNextRecoveredStateBuffer();
}

void RecoveredInputChannel::releaseAllResources()
{
    std::deque<Buffer*> releasedBuffers;
    bool shouldRelease = false;
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        if (!released) {
            released = true;
            shouldRelease = true;

            while (!receivedBuffers.empty()) {
                auto entry = std::move(receivedBuffers.front());
                receivedBuffers.pop_front();
                releasedBuffers.emplace_back(entry.buffer);
            }

            consumedRecoveredBufferOwners.clear();
        }
    }

    if (shouldRelease) {
        bufferManager->releaseAllBuffers(releasedBuffers);
    }
}

std::shared_ptr<omnistream::Buffer> RecoveredInputChannel::requestBufferBlocking()
{
    LOG("RecoveredInputChannel requestBufferBlocking111");
    if (!exclusiveBuffersAssigned) {
        LOG("RecoveredInputChannel requestBufferBlocking222");
        //        bufferManager->requestExclusiveBuffers(networkBuffersPerChannel);
        bufferManager->requestExclusiveBuffers(1);
        exclusiveBuffersAssigned = true;
    }
    LOG("RecoveredInputChannel requestBufferBlocking333");
    return bufferManager->requestBufferBlocking();
}
