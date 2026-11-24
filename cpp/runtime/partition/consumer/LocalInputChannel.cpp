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

// LocalInputChannel.cpp
#include "LocalInputChannel.h"
#include <iostream>
#include <chrono>
#include <thread>

#include "runtime/buffer/ObjectSegment.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"
#include "runtime/partition/PartitionNotFoundException.h"
#include "runtime/buffer/ReadOnlySlicedNetworkBuffer.h"
#include "runtime/checkpoint/channel/CheckpointBufferUtils.h"
#include "core/include/omni_const.h"
namespace omnistream {
LocalInputChannel::LocalInputChannel(
    std::shared_ptr<SingleInputGate> inputGate,
    int channelIndex,
    ResultPartitionIDPOD partitionId,
    std::shared_ptr<ResultPartitionManager> _partitionManager,
    // std::shared_ptr<TaskEventPublisher> taskEventPublisher,
    int initialBackoff,
    int maxBackoff,
    std::shared_ptr<Counter> numBytesIn,
    std::shared_ptr<Counter> numBuffersIn
    //  std::shared_ptr<ChannelStateWriter> stateWriter
    )
    : InputChannel(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, numBytesIn, numBuffersIn),
      partitionManager(_partitionManager),
      channelStatePersister(nullptr)
// taskEventPublisher(taskEventPublisher),
//   channelStatePersister(stateWriter, getChannelInfo()
{
}

LocalInputChannel::LocalInputChannel(
    std::shared_ptr<SingleInputGate> inputGate,
    int channelIndex,
    ResultPartitionIDPOD partitionId,
    std::shared_ptr<ResultPartitionManager> _partitionManager,
    // std::shared_ptr<TaskEventPublisher> taskEventPublisher,
    int initialBackoff,
    int maxBackoff,
    std::shared_ptr<Counter> numBytesIn,
    std::shared_ptr<Counter> numBuffersIn,
    std::shared_ptr<ChannelStateWriter> stateWriter)
    : InputChannel(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, numBytesIn, numBuffersIn),
      partitionManager(_partitionManager)
// taskEventPublisher(taskEventPublisher),
{
    if (stateWriter == nullptr) {
        channelStatePersister = nullptr;
    } else {
        channelStatePersister = std::make_shared<ChannelStatePersister>(stateWriter, getChannelInfo());
    }
}

void LocalInputChannel::CheckpointStarted(
    const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    std::vector<Buffer*> knownBuffers;
    if (channelStatePersister == nullptr) {
        SetChannelStateWriter(channelStateWriter);
    }
    ReleaseInflightBuffers(inflightBuffers_);
    channelStatePersister->StartPersisting(barrier.GetId(), knownBuffers);
}

void LocalInputChannel::SetPersistenceFlag(bool flag)
{
    isNeedPersistence_ = flag;
}
bool LocalInputChannel::IsNeedPersistence()
{
    if (startSize_ != 0) {
        return false;
    }
    return true;
}

void LocalInputChannel::AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
{
    // The writer also recycles the transferred buffers if submission fails.
    std::vector<Buffer*> buffers;
    buffers.swap(inflightBuffers_);
    channelStatePersister->AddInputData(std::move(buffers), checkpointId, info);
}
void LocalInputChannel::SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    channelStatePersister = std::make_shared<ChannelStatePersister>(channelStateWriter, getChannelInfo());
}

void LocalInputChannel::CheckpointStopped(long checkpointId)
{
    isNeedPersistence_ = false;
    startSize_ = 0;
    ReleaseInflightBuffers(inflightBuffers_);
    if (!channelStatePersister) {
        INFO_RELEASE("LocalInputChannel::CheckpointStopped skipped because channelStatePersister is not initialized.");
        return;
    }
    channelStatePersister->StopPersisting(checkpointId);
}

void LocalInputChannel::ReleaseInflightBuffers(std::vector<Buffer*>& buffers)
{
    // AddInputData removes transferred copies, so only channel-owned buffers remain here.
    for (auto* buffer : buffers) {
        ReleaseCheckpointBuffer(buffer);
    }
    buffers.clear();
}

void LocalInputChannel::requestSubpartition(int subpartitionIndex)
{
    bool retriggerRequestFlag = false;
    bool notifyDataAvailableFlag = false;

    LOG_PART("requestSubpartition with " << std::to_string(subpartitionIndex));
    LOCK_BEFORE();
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    if (isReleased_.load()) {
        throw std::runtime_error("LocalInputChannel has been released already");
    }

    if (!subpartitionView) {
        LOG("partitionManager->createSubpartitionView partitionId " << partitionId.toString() << " subpartitionIndex "
                                                                    << std::to_string(subpartitionIndex));

        LOG_PART("partitionManager " << std::to_string(reinterpret_cast<long>(partitionManager.get())));
        try {
            auto subpartitionViewTmp = partitionManager->createSubpartitionView(partitionId, subpartitionIndex, this);

            LOG("after partitionManager->createSubpartitionView subpartitionView "
                << std::to_string(reinterpret_cast<long>(subpartitionView.get())));
            // std::shared_ptr<BufferAvailabilityListener>(
            if (!subpartitionViewTmp) {
                THROW_RUNTIME_ERROR("Error requesting subpartition.");
            }
            subpartitionView = subpartitionViewTmp;
            if (isReleased_.load()) {
                subpartitionView->releaseAllResources();
                subpartitionView.reset();
            } else {
                notifyDataAvailableFlag = true;
            }
        } catch (const PartitionNotFoundException& e) {
            if (increaseBackoff()) {
                LOG("retriggerRequest happens and currentBackoff is " << std::to_string(currentBackoff));
                retriggerRequestFlag = true;
            } else {
                LOG("after doing enought retriggerRequest failed, throw PartitionNotFoundException to "
                    << "let current task stop and currentBackoff is  " << std::to_string(currentBackoff));
                throw e;
            }
        }

        if (notifyDataAvailableFlag) {
            notifyDataAvailable();
        }

        if (retriggerRequestFlag) {
            inputGate->retriggerPartitionRequest(partitionId.getPartitionId());
        }
    }
}

void LocalInputChannel::retriggerSubpartitionRequest(
    std::shared_ptr<std::chrono::steady_clock::time_point> timer, int subpartitionIndex)
{
    LOCK_BEFORE();
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    if (subpartitionView) {
        throw std::runtime_error("already requested partition");
    }
    std::thread([this, subpartitionIndex]() {
        INFO_RELEASE("LocalInputChannel sleep time: " << std::to_string(getCurrentBackoff()));
        std::this_thread::sleep_for(std::chrono::milliseconds(getCurrentBackoff()));
        try {
            requestSubpartition(subpartitionIndex);
        } catch (...) {
            std::exception_ptr eptr = std::current_exception();
            setError(eptr);
        }
    }).detach();
}

std::optional<BufferAndAvailability> LocalInputChannel::getNextBuffer()
{
    checkError();

    std::shared_ptr<ResultSubpartitionView> subpartitionViewPtr = subpartitionView;
    if (!subpartitionViewPtr) {
        if (isReleased_.load()) {
            return std::nullopt;
        }

        subpartitionViewPtr = checkAndWaitForSubpartitionView();
    }
    LOG("subpartitionViewPtr.get()" << subpartitionViewPtr.get());
    BufferAndBacklog* next = subpartitionViewPtr->getNextBuffer();
    while (next) {
        Buffer* emptyCandidate = next->getBuffer();
        if (emptyCandidate == nullptr || emptyCandidate->GetSize() != 0) {
            break;
        }
        emptyCandidate->RecycleBuffer();
        delete emptyCandidate;
        delete next;
        next = subpartitionViewPtr->getNextBuffer();
        numBuffersIn->Inc();
    }
    if (!next) {
        if (subpartitionViewPtr->isReleased()) {
            THROW_LOGIC_EXCEPTION("TBD");
        } else {
            return std::nullopt;
        }
    }

    LOG("before LocalInputChannel::next->getBuffer()");

    // std::shared_ptr<ObjectBuffer> buffer = next->getBuffer();
    Buffer* buffer = next->getBuffer();
    if (buffer == nullptr) {
        delete next;
        return std::nullopt;
    }
    // todo: need realization
    LOG("after LocalInputChannel::next->getBuffer()");
    /**
        if (auto fileRegionBuffer = std::dynamic_pointer_cast<FileRegionBuffer>(buffer)) {
            buffer = fileRegionBuffer->readInto(inputGate->getUnpooledSegment());
        }
        **/

    // LOG("after LocalInputChannel::next->getBuffer() 1")
    LOG("after LocalInputChannel::next->getBuffer() 2");
    if (channelStatePersister) {
        channelStatePersister->CheckForBarrier(buffer);
    }

    int bufferLength = buffer->GetSize();
    if (bufferLength > IO_SIZE_512M) {
        INFO_RELEASE("Error: invalid buffer size:" << bufferLength);
        buffer->RecycleBuffer();
        delete buffer;
        delete next;
        return std::nullopt;
    }
    insize += bufferLength;
    if (isNeedPersistence_ && buffer->isBuffer()) {
        auto segment = buffer->GetSegment();
        if (segment->isObjectSegment()) {
            ObjectSegment* newObjectSegment = new ObjectSegment(bufferLength);
            const ObjectSegment* objectSegment = dynamic_cast<const ObjectSegment*>(segment);
            try {
                newObjectSegment->put(0, objectSegment, buffer->GetOffset(), bufferLength);
            } catch (...) {
                delete newObjectSegment;
                buffer->RecycleBuffer();
                delete buffer;
                delete next;
                throw;
            }
            auto* copiedBuffer =
                new VectorBatchBuffer(newObjectSegment, std::make_shared<DeepCopiedObjectBufferRecycler>());
            copiedBuffer->SetSize(bufferLength);
            inflightBuffers_.push_back(copiedBuffer);
        } else {
            uint8_t* newBufferAddress = new uint8_t[bufferLength];
            datastream::ReadOnlySlicedNetworkBuffer* readOnlyBuffer = (datastream::ReadOnlySlicedNetworkBuffer*)buffer;
            int readIndex = readOnlyBuffer->GetMemorySegmentOffset();
            MemorySegment* newMemorySegment = new MemorySegment(newBufferAddress, bufferLength);
            auto memorySegment = dynamic_cast<MemorySegment*>(segment);
            if (memorySegment == nullptr) {
                throw std::runtime_error("ChannelStateSerializerImpl::WriteData requires MemorySegment-backed buffer");
            }
            newMemorySegment->put(0, reinterpret_cast<uint8_t*>(memorySegment->getData()), readIndex, bufferLength);
            ::datastream::NetworkBuffer* newNetworkBuffer = new ::datastream::NetworkBuffer(
                newMemorySegment,
                bufferLength,
                0,
                std::make_shared<OriginalNetworkBufferRecycler>(),
                ObjectBufferDataType::DATA_BUFFER,
                true);
            inflightBuffers_.push_back(newNetworkBuffer);
        }
    }
    if (!buffer->isBuffer()) {
        std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(buffer);
        if (event->GetEventClassName() == "CheckpointBarrier") {
            isNeedPersistence_ = false;
            startSize_ = insize;
        }
    }

    if (next->getNextDataType().isEvent()) {
        LOG_TRACE("event buffer " << buffer->ToDebugString(false));
    }
    const ObjectBufferDataType& bufferDataType = next->getNextDataType();
    int buffersInBacklog = next->getBuffersInBacklog();
    int sequenceNumber = next->getSequenceNumber();
    delete next;
    // The buffer is now handed to the gate/operator. Track consumed bytes so
    // channel selection can dynamically compensate for consumption skew.
    outsize += static_cast<size_t>(bufferLength);
    return BufferAndAvailability{buffer, bufferDataType, buffersInBacklog, sequenceNumber};
}

void LocalInputChannel::notifyDataAvailable()
{
    // LOG_PART("Beginnging")
    LOG("LocalInputChannel notifyDataAvailable invoke!");
    notifyChannelNonEmpty();
}

void LocalInputChannel::notifyPriorityEvent(int prioritySequenceNumber)
{
    NotifyPriorityEvent(prioritySequenceNumber);
}

void LocalInputChannel::ConvertToPriorityEvent(int sequenceNumber)
{
    if (isReleased_) {
        return;
    }
    if (subpartitionView) {
        LOG("subpartitionView->ConvertToPriorityEvent(sequenceNumber)");
        subpartitionView->ConvertToPriorityEvent(sequenceNumber);
    }

    if (inputGate) {
        LOG("inputGate->notifyPriorityEventForce");
        inputGate->notifyPriorityEventForce(InputChannel::shared_from_this());
    }
}

std::shared_ptr<ResultSubpartitionView> LocalInputChannel::checkAndWaitForSubpartitionView()
{
    LOCK_BEFORE();
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    if (isReleased_.load()) {
        throw std::runtime_error("released");
    }
    if (!subpartitionView) {
        throw std::runtime_error("Queried for a buffer before requesting the subpartition.");
    }
    return subpartitionView;
}

void LocalInputChannel::resumeConsumption()
{
    if (isReleased_.load()) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->resumeConsumption();
        if (subpartitionView->getAvailabilityAndBacklog(INT_MAX).getIsAvailable()) {
            notifyChannelNonEmpty();
        }
    }
}

void LocalInputChannel::TimeOutResumeConsumption()
{
    if (isReleased_.load()) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->resumeConsumption();
        if (subpartitionView->getAvailabilityAndBacklog(INT_MAX).getIsAvailable()) {
            notifyChannelNonEmpty();
        }
    }
}

void LocalInputChannel::acknowledgeAllRecordsProcessed()
{
    if (isReleased_.load()) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->acknowledgeAllDataProcessed();
    }
}
/**
void LocalInputChannel::sendTaskEvent(TaskEvent event) {
    checkError();
    if (!subpartitionView) {
        throw std::runtime_error("Tried to send task event to producer before requesting the subpartition.");
    }

    if (!taskEventPublisher.publish(partitionId, event)) {
        throw std::runtime_error("Error while publishing event to producer. The producer could not be found.");
    }
}

**/

bool LocalInputChannel::isReleased()
{
    return isReleased_.load();
}

void LocalInputChannel::releaseAllResources()
{
    isNeedPersistence_ = false;
    startSize_ = 0;
    ReleaseInflightBuffers(inflightBuffers_);
    if (!isReleased_.load()) {
        isReleased_.store(true);

        if (subpartitionView) {
            subpartitionView->releaseAllResources();
            subpartitionView.reset();
        }
    }
}

void LocalInputChannel::announceBufferSize(int newBufferSize)
{
    if (isReleased_.load()) {
        throw std::runtime_error("Channel released.");
    }

    if (subpartitionView) {
        subpartitionView->notifyNewBufferSize(newBufferSize);
    }
}

int LocalInputChannel::getBuffersInUseCount()
{
    if (subpartitionView) {
        return subpartitionView->getNumberOfQueuedBuffers();
    }
    return 0;
}

int LocalInputChannel::unsynchronizedGetNumberOfQueuedBuffers()
{
    if (subpartitionView) {
        return subpartitionView->unsynchronizedGetNumberOfQueuedBuffers();
    }
    return 0;
}

size_t LocalInputChannel::getConsumedBufferSize() const
{
    return outsize;
}

std::string LocalInputChannel::toString()
{
    return "LocalInputChannel [" + partitionId.toString() + "]";
}

std::shared_ptr<ResultSubpartitionView> LocalInputChannel::getSubpartitionView()
{
    return subpartitionView;
}

void LocalInputChannel::notifyBufferAvailable(int subpartitionId)
{
}

void LocalInputChannel::sendTaskEvent(std::shared_ptr<TaskEvent> event)
{
}
} // namespace omnistream
