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

#include "SingleInputGate.h"

#include <algorithm>
#include <climits>
#include <stdexcept>
#include <sstream>
#include <objectsegment/ObjectSegmentFactory.h>
#include "LocalInputChannel.h"

#include "io/network/api/serialization/EventSerializer.h"
#include "RemoteInputChannel.h"
#include "event/EndOfData.h"
#include "event/EndOfPartitionEvent.h"
#include "LocalRecoveredInputChannel.h"
#include "RemoteRecoveredInputChannel.h"
#include "OmniLocalInputChannel.h"
#include "buffer/ReadOnlySlicedNetworkBuffer.h"

namespace omnistream {

SingleInputGate::SingleInputGate(
    const std::string& owningTaskName,
    int gateIndex,
    const IntermediateDataSetIDPOD& consumedResultId,
    const int consumedPartitionType,
    int consumedSubpartitionIndex,
    int numberOfInputChannels,
    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
    std::function<std::shared_ptr<BufferPool>()> bufferPoolFactory,
    std::shared_ptr<SegmentProvider> segmentProvider,
    int segmentSize)
    : owningTaskName(owningTaskName),
      gateIndex(gateIndex),
      consumedResultId(consumedResultId),
      consumedPartitionType(consumedPartitionType),
      consumedSubpartitionIndex(consumedSubpartitionIndex),
      numberOfInputChannels(numberOfInputChannels),
      partitionProducerStateProvider(partitionProducerStateProvider),
      bufferPoolFactory(bufferPoolFactory),
      segmentProvider(segmentProvider),
      hasReceivedAllEndOfPartitionEvents(false),
      hasReceivedEndOfData_(false),
      requestedPartitionsFlag(false),
      numberOfUninitializedChannels(0),
      closeFuture(std::make_shared<CompletableFuture>())
{
    LOG_PART("Beginning of constructor");

    if (gateIndex < 0) {
        throw std::invalid_argument("The gate index must be positive.");
    }

    if (consumedSubpartitionIndex < 0) {
        throw std::invalid_argument("consumedSubpartitionIndex must be non-negative.");
    }

    if (numberOfInputChannels <= 0) {
        throw std::invalid_argument("numberOfInputChannels must be greater than 0.");
    }

    channels.resize(numberOfInputChannels);
    lastPrioritySequenceNumber.resize(numberOfInputChannels, INT_MIN);

    // unpooledSegment = ObjectSegmentFactory::allocateUnpooledSegment(segmentSize);

    enqueuedInputChannelsWithData.resize(numberOfInputChannels, 0);
    channelsWithEndOfPartitionEvents.resize(numberOfInputChannels, 0);
    channelsWithEndOfUserRecords.resize(numberOfInputChannels, 0);
}

PrioritizedDeque<InputChannel> SingleInputGate::getInputChannelsWithData()
{
    return inputChannelsWithData;
}

void SingleInputGate::setup()
{
    if (bufferPool != nullptr) {
        throw std::runtime_error("Bug in input gate setup logic: Already registered buffer pool.");
    }

    auto buffer_pool = bufferPoolFactory();
    setBufferPool(buffer_pool);
    LOG("after setBufferPool");
    setupChannels();
    LOG("after setupChannels");
}

std::shared_ptr<CompletableFutureV2<void>> SingleInputGate::getStateConsumedFuture()
{
    // LOCK_BEFORE()
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    // LOCK_AFTER()

    std::vector<std::shared_ptr<CompletableFutureV2<void>>> futures;
    for (const auto& entry : inputChannels) {
        auto inputChannel = entry.second;
        //  orginal begin

        auto recoveredChannel = std::dynamic_pointer_cast<RecoveredInputChannel>(inputChannel);
        if (recoveredChannel) {
            futures.push_back(recoveredChannel->getStateConsumedFuture());
        }

        // orignal end
    }
    return CompletableFutureV2<void>::AllOf(futures);
}

std::vector<bool> SingleInputGate::getStateConsumedFuture1()
{
    LOCK_BEFORE();
    std::lock_guard<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    std::vector<bool> futures;
    for (const auto& entry : inputChannels) {
        auto inputChannel = entry.second;
        //  orginal begin
        auto recoveredChannel = std::dynamic_pointer_cast<RecoveredInputChannel>(inputChannel);
        if (recoveredChannel) {
            futures.push_back(recoveredChannel->getStateConsumedFuture1());
        }
        // orignal end
    }
    return futures;
}

void SingleInputGate::RequestPartitions()
{
    // LOCK_BEFORE()
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    // LOCK_AFTER()

    // LOG_PART("beginning ")
    if (!requestedPartitionsFlag) {
        if (closeFuture->isDone()) {
            THROW_RUNTIME_ERROR("Already released.");
        }

        // Sanity checks
        if (static_cast<size_t>(numberOfInputChannels) != inputChannels.size()) {
            std::stringstream ss;
            ss << "Bug in input gate setup logic: mismatch between "
               << "number of total input channels [" << inputChannels.size()
               << "] and the currently set number of input "
               << "channels [" << numberOfInputChannels << "].";
            THROW_RUNTIME_ERROR(ss.str());
        }

        convertRecoveredInputChannels();

        // LOG_PART("before intneral request partions ")
        internalRequestPartitions();
    }

    requestedPartitionsFlag = true;
}

void SingleInputGate::convertRecoveredInputChannels()
{
    LOG("covert recovered input channels (" << numberOfInputChannels
                                            << " channels, inputChannels.size:" << inputChannels.size());
    for (auto& entry : inputChannels) {
        std::shared_ptr<InputChannel> inputChannel = entry.second;
        if (auto local = std::dynamic_pointer_cast<LocalRecoveredInputChannel>(inputChannel)) {
            LOG("before instance of LocalRecoveredInputChannel, to convert to normal channel!");
        } else if (auto remote = std::dynamic_pointer_cast<RemoteRecoveredInputChannel>(inputChannel)) {
            LOG("before instance of RemoteRecoveredInputChannel, to convert to normal channel!");
        } else if (auto local2 = std::dynamic_pointer_cast<OmniLocalInputChannel>(inputChannel)) {
            LOG("before instance of OmniLocalInputChannel!");
        } else if (auto remote1 = std::dynamic_pointer_cast<RemoteInputChannel>(inputChannel)) {
            LOG("before instance of RemoteInputChannel!");
        } else if (auto local1 = std::dynamic_pointer_cast<LocalInputChannel>(inputChannel)) {
            LOG("before instance of LocalInputChannel!");
        } else {
            LOG("before unKnown channel type!");
        }
    }
    for (auto& entry : inputChannels) {
        std::shared_ptr<InputChannel> inputChannel = entry.second;
        IntermediateResultPartitionIDPOD key = entry.first;
        auto recoveredChannel = std::dynamic_pointer_cast<RecoveredInputChannel>(inputChannel);
        if (recoveredChannel) {
            LOG("instance of RecoveredInputChannel, to convert to normal channel!");
            if (auto local = std::dynamic_pointer_cast<LocalRecoveredInputChannel>(inputChannel)) {
                LOG("instance of LocalRecoveredInputChannel, to convert to normal channel!");
            } else if (auto remote = std::dynamic_pointer_cast<RemoteRecoveredInputChannel>(inputChannel)) {
                LOG("instance of RemoteRecoveredInputChannel, to convert to normal channel!");
            } else {
                LOG("unKnown recover channel type!");
            }
            std::shared_ptr<omnistream::InputChannel> realChannel = recoveredChannel->toInputChannel();
            recoveredChannel->releaseAllResources();
            if (auto remote = std::dynamic_pointer_cast<RemoteInputChannel>(realChannel)) {
                LOG("realChannel of RemoteRecoveredInputChannel, convert to normal channel!");
            } else if (auto omniLocal = std::dynamic_pointer_cast<OmniLocalInputChannel>(realChannel)) {
                LOG("realChannel of OmniLocalInputChannel, convert to normal channel!");
            } else if (auto local = std::dynamic_pointer_cast<LocalInputChannel>(realChannel)) {
                LOG("realChannel of LocalRecoveredInputChannel, convert to normal channel!");
            } else {
                LOG("unKnown realChannel recover channel type!");
            }
            inputChannels[key] = realChannel;
            channels[recoveredChannel->getChannelIndex()] = realChannel;
        } else {
            LOG("channel is not a recover type!");
            if (auto remote = std::dynamic_pointer_cast<RemoteInputChannel>(inputChannel)) {
                LOG("instance of RemoteInputChannel!");
            } else if (auto omniLocal = std::dynamic_pointer_cast<OmniLocalInputChannel>(inputChannel)) {
                LOG("realChannel of OmniLocalInputChannel,  convert to normal channel!");
            } else if (auto local = std::dynamic_pointer_cast<LocalInputChannel>(inputChannel)) {
                LOG("instance of LocalInputChannel!");
            } else {
                LOG("unKnown channel type!");
            }
        }
    }

    for (auto& entry : inputChannels) {
        std::shared_ptr<InputChannel> inputChannel = entry.second;
        if (auto local = std::dynamic_pointer_cast<LocalRecoveredInputChannel>(inputChannel)) {
            LOG("after instance of LocalRecoveredInputChannel, to convert to normal channel!");
        } else if (auto remote = std::dynamic_pointer_cast<RemoteRecoveredInputChannel>(inputChannel)) {
            LOG("after instance of RemoteRecoveredInputChannel, to convert to normal channel!");
        } else if (auto local2 = std::dynamic_pointer_cast<OmniLocalInputChannel>(inputChannel)) {
            LOG("after instance of OmniLocalInputChannel!");
        } else if (auto remote1 = std::dynamic_pointer_cast<RemoteInputChannel>(inputChannel)) {
            LOG("after instance of RemoteInputChannel!");
        } else if (auto local1 = std::dynamic_pointer_cast<LocalInputChannel>(inputChannel)) {
            LOG("after instance of LocalInputChannel!");
        } else {
            LOG("after unKnown channel type!");
        }
    }
}

void SingleInputGate::internalRequestPartitions()
{
    for (auto& entry : inputChannels) {
        auto& inputChannel = entry.second;
        inputChannel->requestSubpartition(consumedSubpartitionIndex);
    }
}

void SingleInputGate::FinishReadRecoveredState()
{
    LOG("single input gate FinishReadRecoveredState!");
    for (auto& channel : channels) {
        if (auto recoveredChannel = std::dynamic_pointer_cast<RecoveredInputChannel>(channel)) {
            recoveredChannel->finishReadRecoveredState();
        }
    }
}

void SingleInputGate::FinishInnerRecoveredState()
{
    INFO_RELEASE("single input gate FinishInnerRecoveredState!");
    for (auto& channel : channels) {
        if (auto recoveredChannel = std::dynamic_pointer_cast<RecoveredInputChannel>(channel)) {
            recoveredChannel->finishInnerRecoveredState();
        }
    }
}

int SingleInputGate::GetNumberOfInputChannels()
{
    return numberOfInputChannels;
}

int SingleInputGate::GetGateIndex()
{
    return gateIndex;
}

std::vector<InputChannelInfo> SingleInputGate::getUnfinishedChannels()
{
    std::vector<InputChannelInfo> unfinishedChannels;
    auto count = std::count(channelsWithEndOfPartitionEvents.begin(), channelsWithEndOfPartitionEvents.end(), true);
    unfinishedChannels.reserve(numberOfInputChannels - count);

    LOCK_BEFORE();
    std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
    LOCK_AFTER();
    ///
    ///
    /***
            for (int i = channelsWithEndOfPartitionEvents._Find_first_zero();
                 i < numberOfInputChannels;
                 i = channelsWithEndOfPartitionEvents._Find_next_zero(i)) {

                unfinishedChannels.push_back(channels[i]->getChannelInfo());
                 }
            */
    return unfinishedChannels;
}

int SingleInputGate::getBuffersInUseCount()
{
    int total = 0;
    for (auto& channel : channels) {
        total += channel->getBuffersInUseCount();
    }
    return total;
}

void SingleInputGate::announceBufferSize(int newBufferSize)
{
    for (auto& channel : channels) {
        if (!channel->isReleased()) {
            channel->announceBufferSize(newBufferSize);
        }
    }
}

int SingleInputGate::getConsumedPartitionType()
{
    return consumedPartitionType;
}

std::shared_ptr<BufferProvider> SingleInputGate::getBufferProvider()
{
    return bufferPool;
}

std::shared_ptr<BufferPool> SingleInputGate::getBufferPool()
{
    return bufferPool;
}

std::shared_ptr<SegmentProvider> SingleInputGate::getSegmentProvider()
{
    return segmentProvider;
}

std::string SingleInputGate::getOwningTaskName()
{
    return owningTaskName;
}

int SingleInputGate::getNumberOfQueuedBuffers()
{
    // re-try 3 times, if fails, return 0 for "unknown"
    for (int retry = 0; retry < 3; retry++) {
        try {
            int totalBuffers = 0;

            for (const auto& entry : inputChannels) {
                totalBuffers += entry.second->unsynchronizedGetNumberOfQueuedBuffers();
            }

            return totalBuffers;
        } catch (...) {
            // Ignore and retry
        }
    }

    return 0;
}

std::shared_ptr<CompletableFuture> SingleInputGate::getCloseFuture()
{
    return closeFuture;
}

std::shared_ptr<InputChannel> SingleInputGate::getChannel(int channelIndex)
{
    return channels[channelIndex];
}

// void SingleInputGate::setBufferPool(std::shared_ptr<ObjectBufferPool> pool)
void SingleInputGate::setBufferPool(std::shared_ptr<BufferPool> pool)
{
    if (this->bufferPool != nullptr) {
        throw std::runtime_error(
            "Bug in input gate setup logic: buffer pool has "
            "already been set for this input gate.");
    }

    this->bufferPool = pool;
}

void SingleInputGate::setupChannels()
{
    // Allocate a single floating buffer to avoid potential deadlock
    bufferPool->reserveSegments(1);

    // Allocate the exclusive buffers per channel
    LOCK_BEFORE();
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    LOG("entry.second->setup() will running");
    for (auto& entry : inputChannels) {
        entry.second->setup();
    }
}

void SingleInputGate::setInputChannels(std::vector<std::shared_ptr<InputChannel>> newChannels)
{
    LOG_PART("beginning of setInputChannels");
    if (newChannels.size() != static_cast<size_t>(numberOfInputChannels)) {
        std::stringstream ss;
        ss << "Expected " << numberOfInputChannels << " channels, "
           << "but got " << newChannels.size();
        throw std::invalid_argument(ss.str());
    }

    LOCK_BEFORE();
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    std::copy(newChannels.begin(), newChannels.end(), channels.begin());

    for (auto& inputChannel : newChannels) {
        IntermediateResultPartitionIDPOD partitionId = inputChannel->getPartitionId().getPartitionId();
        if (auto local = std::dynamic_pointer_cast<LocalRecoveredInputChannel>(inputChannel)) {
            LOG_PART("setupChannel instance of LocalRecoveredInputChannel, to convert to normal channel!");
        } else if (auto remote = std::dynamic_pointer_cast<RemoteRecoveredInputChannel>(inputChannel)) {
            LOG_PART("setupChannel instance of RemoteRecoveredInputChannel, to convert to normal channel!");
        } else if (auto local1 = std::dynamic_pointer_cast<LocalInputChannel>(inputChannel)) {
            LOG_PART("setupChannel instance of LocalInputChannel!");
        } else if (auto remote1 = std::dynamic_pointer_cast<RemoteInputChannel>(inputChannel)) {
            LOG_PART("setupChannel instance of RemoteInputChannel!");
        } else {
            LOG_PART("setupChannel unKnown channel type!");
        }
        auto result = inputChannels.insert({partitionId, inputChannel});

        //        if (result.second && std::dynamic_pointer_cast<UnknownInputChannel>(inputChannel)) {
        //            numberOfUninitializedChannels++;
        //        }
        if (result.second) {
            numberOfUninitializedChannels++;
        }
    }
}

// todo: need realization
void SingleInputGate::updateInputChannel(
    const ResourceIDPOD& localLocation, const ShuffleDescriptorPOD& shuffleDescriptor)
{
    LOCK_BEFORE();
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    if (closeFuture->isDone()) {
        // There was a race with a task failure/cancel
        return;
    }

    /**
    if (it != inputChannels.end() && std::dynamic_pointer_cast<UnknownInputChannel>(it->second)) {
        auto unknownChannel = std::dynamic_pointer_cast<UnknownInputChannel>(it->second);
        bool isLocal = shuffleDescriptor.isLocalTo(localLocation);
        std::shared_ptr<InputChannel> newChannel;

        if (isLocal) {
            newChannel = unknownChannel->toLocalInputChannel();
        } else {
            auto remoteInputChannel = unknownChannel->toRemoteInputChannel(shuffleDescriptor.getConnectionId());
            remoteInputChannel->setup();
            newChannel = remoteInputChannel;
        }

        // Log debug message would go here

        inputChannels[partitionId] = newChannel;
        channels[it->second->getChannelIndex()] = newChannel;

        if (requestedPartitionsFlag) {
            newChannel->requestSubpartition(consumedSubpartitionIndex);
        }

        for (const auto& event : pendingEvents) {
            newChannel->sendTaskEvent(event);
        }

        if (--numberOfUninitializedChannels == 0) {
            pendingEvents.clear();
        }
    }
    **/
}

void SingleInputGate::retriggerPartitionRequest(const IntermediateResultPartitionIDPOD& partitionId)
{
    LOG("beginnig of retriggerPartitionRequest ");

    LOCK_BEFORE();
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    if (!closeFuture->isDone()) {
        auto it = inputChannels.find(partitionId);
        if (it == inputChannels.end()) {
            std::stringstream ss;
            ss << "Unknown input channel with ID " << partitionId.toString();
            throw std::runtime_error(ss.str());
        }

        auto& ch = it->second;

        // Debug log would go here

        // Check if ch is a RemoteInputChannel, and throw an exception if it is
        auto remoteChannel = std::dynamic_pointer_cast<RemoteInputChannel>(ch);
        if (remoteChannel) {
            throw std::runtime_error("RemoteInputChannel should be initialized on the Java side.");
        } else {
            auto localChannel = std::dynamic_pointer_cast<LocalInputChannel>(ch);
            if (localChannel) {
                localChannel->retriggerSubpartitionRequest(nullptr, consumedSubpartitionIndex);
            } else {
                std::stringstream ss;
                ss << "Unexpected type of channel to retrigger partition: " << typeid(*ch).name();
                throw std::runtime_error(ss.str());
            }
        }
    }
}

void SingleInputGate::close()
{
    bool released = false;
    std::cout << "you are in SingleInputGate::close()" << std::endl;
    {
        LOCK_BEFORE();
        std::unique_lock<std::recursive_mutex> lock(requestLock);
        LOCK_AFTER();

        if (!closeFuture->isDone()) {
            std::cout << "you are in SingleInputGate::close()  closeFuture->isDone()" << std::endl;

            try {
                // Debug log would go here

                for (auto& entry : inputChannels) {
                    try {
                        entry.second->releaseAllResources();
                    } catch (const std::exception& e) {
                        // Warning log would go here
                    }
                }

                // Every channel holds a shared_ptr back to this gate, so the gate and its
                // channels keep each other alive. Dropping the gate's references here breaks
                // that cycle; otherwise the channels and everything they own (queues, bridges,
                // recyclers) survive for the lifetime of the process.
                inputChannels.clear();
                channels.clear();
                pendingEvents.clear();
                numberOfUninitializedChannels = 0;

                if (bufferPool) {
                    bufferPool->lazyDestroy();
                }
            } catch (...) {
                // Ignore exceptions
            }

            released = true;
            closeFuture->complete();
        }
    }

    if (released) {
        std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
        // The availability queue holds channel references of its own. The channels were
        // already released above, so nothing here is still needed.
        inputChannelsWithData.clear();
        std::fill(enqueuedInputChannelsWithData.begin(), enqueuedInputChannelsWithData.end(), false);
        cv.notify_all();
    }
}

bool SingleInputGate::IsFinished()
{
    return hasReceivedAllEndOfPartitionEvents;
}

bool SingleInputGate::HasReceivedEndOfData()
{
    return hasReceivedEndOfData_;
}

BufferOrEvent* SingleInputGate::GetNext()
{
    return getNextBufferOrEvent(true);
}

BufferOrEvent* SingleInputGate::PollNext()
{
    // LOG_PART(">>>>")
    // LOG_PART("<<<<<" << toString())
    LOG_PART("single input gate poll next");
    return getNextBufferOrEvent(false);
}

BufferOrEvent* SingleInputGate::getNextBufferOrEvent(bool blocking)
{
    if (hasReceivedAllEndOfPartitionEvents) {
        // THROW_LOGIC_EXCEPTION("hasReceivedAllEndOfPartitionEvents is true.")
        return nullptr;
    }

    if (closeFuture->isDone()) {
        THROW_LOGIC_EXCEPTION("nput gate is already closed.");
    }

    // LOG_PART("before waitAndGetNextData")

    auto inputWithData = waitAndGetNextData(blocking);
    // LOG_PART("after  waitAndGetNextData: " << next.has_value())
    if (!inputWithData) {
        return nullptr;
    }
    auto bufferOrEvent = transformToBufferOrEvent(
        inputWithData->data.buffer,
        inputWithData->moreAvailable,
        inputWithData->input,
        inputWithData->morePriorityEvents);
    delete inputWithData;
    return bufferOrEvent;
}

SingleInputGate::InputWithData<BufferAndAvailability>* SingleInputGate::waitAndGetNextData(bool blocking)
{
    // LOG_PART("begining of waitAndGetNextData")
    while (true) {
        std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
        auto inputChannelOpt = getChannel(blocking, lock);
        // LOG(">>>>inputChannelOpt  " << inputChannelOpt.has_value())
        if (!inputChannelOpt) {
            // INFO_RELEASE("---- channel not have data ---- ")
            // const int sleepTime = getWaitSizeFromEnv();
            // std::this_thread::sleep_for(std::chrono::microseconds(sleepTime)); //
            // sleep使inputChannelsWithDataMutex读写分配均匀
            return nullptr;
        }
        LOG(">>>>>>>inputChannelOpt.value(): " << inputChannelOpt.value());
        auto inputChannel = inputChannelOpt.value();
        LOG("inputChannel->getNextBuffer()" << inputChannel.get());

        if (auto ca = std::dynamic_pointer_cast<RecoveredInputChannel>(inputChannel)) {
            LOG("the channel is recover input channel !");
        }

        auto bufferAndAvailabilityOpt = inputChannel->getNextBuffer();
        if (!bufferAndAvailabilityOpt) {
            checkUnavailability();
            continue;
        }

        auto& bufferAndAvailability = *bufferAndAvailabilityOpt;
        if (bufferAndAvailability.moreAvailable()) {
            // enqueue the inputChannel at the end to avoid starvation
            queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
        } else {
            // diagnostic : why there is buffer  NO moreAvailable.
            // For event buffer, assume only ENDOFPARTITION
            // let us show what is inside the buffer
            LOG_TRACE(" bufferAndAvailability.moreAvailable() is false");
            auto buffer = bufferAndAvailability.buffer;
            LOG_TRACE(" bufferAndAvailability.moreAvailable(): buffer " << buffer);
            if (buffer) {
                LOG_TRACE(
                    " bufferAndAvailability.moreAvailable(): buffer size"
                    << buffer->GetSize() << "datatype  is data "
                    << (buffer->GetDataType() == ObjectBufferDataType::DATA_BUFFER) << "datatype is event "
                    << (buffer->GetDataType() == ObjectBufferDataType::EVENT_BUFFER));
            }
        }

        bool morePriorityEvents = inputChannelsWithData.getNumPriorityElements() > 0;
        if (bufferAndAvailability.hasPriority()) {
            lastPrioritySequenceNumber[inputChannel->getChannelIndex()] = bufferAndAvailability.sequenceNumber;
            if (!morePriorityEvents) {
                priorityAvailabilityHelper.resetUnavailable(); // - Logic would go here
            }
        }

        checkUnavailability();

        return new InputWithData<BufferAndAvailability>(
            inputChannel, bufferAndAvailability, !inputChannelsWithData.isEmpty(), morePriorityEvents);
    }
}

void SingleInputGate::checkUnavailability()
{
    if (inputChannelsWithData.isEmpty()) {
        availabilityHelper.resetUnavailable();
        // availabilityHelper.resetUnavailable(); - Logic would go here
    }
}

BufferOrEvent* SingleInputGate::transformToBufferOrEvent(
    Buffer* buffer, bool moreAvailable, std::shared_ptr<InputChannel> currentChannel, bool morePriorityEvents)
{
    if (buffer->isBuffer()) {
        return transformBuffer(buffer, moreAvailable, currentChannel, morePriorityEvents);
    } else {
        // LOG_TRACE("transformEvent")
        return transformEvent(buffer, moreAvailable, currentChannel, morePriorityEvents);
    }
}

BufferOrEvent* SingleInputGate::transformBuffer(
    Buffer* buffer, bool moreAvailable, std::shared_ptr<InputChannel> currentChannel, bool morePriorityEvents)
{
    return new BufferOrEvent(
        decompressBufferIfNeeded(buffer), currentChannel->getChannelInfo(), moreAvailable, morePriorityEvents);
}

BufferOrEvent* SingleInputGate::transformEvent(
    Buffer* buffer, bool moreAvailable, std::shared_ptr<InputChannel> currentChannel, bool morePriorityEvents)
{
    bool hasPriority = buffer->GetDataType().hasPriority();
    int size = buffer->GetSize();

    bool bufferIsReadOnly = false;
    if (dynamic_cast<ReadOnlySlicedNetworkBuffer*>(buffer)) {
        bufferIsReadOnly = true;
    }
    std::shared_ptr<AbstractEvent> event = EventSerializer::fromBuffer(buffer);

    if (bufferIsReadOnly) {
        delete buffer;
    }

    if (dynamic_cast<EndOfPartitionEvent*>(event.get())) {
        INFO_RELEASE(
            "END_OF_PARTITION_EVENT received by channel :" << currentChannel->getChannelIndex()
                                                           << " of Task :" << owningTaskName);
        std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
        if (channelsWithEndOfPartitionEvents[currentChannel->getChannelIndex()]) {
            throw std::runtime_error("Received more than one EndOfPartitionEvent from the same channel.");
        }
        channelsWithEndOfPartitionEvents[currentChannel->getChannelIndex()] = true;
        auto count = std::count(channelsWithEndOfPartitionEvents.begin(), channelsWithEndOfPartitionEvents.end(), true);
        hasReceivedAllEndOfPartitionEvents = count == static_cast<long>(numberOfInputChannels);

        enqueuedInputChannelsWithData[currentChannel->getChannelIndex()] = false;
        if (inputChannelsWithData.contains(currentChannel)) {
            inputChannelsWithData.getAndRemove(
                [currentChannel](const std::shared_ptr<InputChannel>& channel) { return channel == currentChannel; });
        }
        lock.unlock();
        if (hasReceivedAllEndOfPartitionEvents) {
            LOG_TRACE("hasReceivedAllEndOfPartitionEvents");
            // Because of race condition between:
            // 1. releasing inputChannelsWithData lock in this method and reaching this place
            // 2. empty data notification that re-enqueues a channel we can end up with
            // moreAvailable flag set to true, while we expect no more data.
            BufferOrEvent* bufferOrEvent = PollNext();
            if (moreAvailable && bufferOrEvent) {
                delete bufferOrEvent;
                throw std::runtime_error(
                    "Bug in input gate logic: moreAvailable flag is true when all "
                    "EndOfPartitionEvents have been received.");
            } else if (bufferOrEvent) {
                delete bufferOrEvent;
            }
            moreAvailable = false;
            markAvailable();
        }

        currentChannel->releaseAllResources();
        LOG_TRACE("This Gate is " << (IsFinished() ? "finished" : "not finished"));
    } else if (dynamic_cast<EndOfData*>(event.get())) {
        INFO_RELEASE(
            "END_OF_USER_RECORDS_EVENT received by channel :" << currentChannel->getChannelIndex()
                                                              << " of Task :" << owningTaskName);
        std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
        if (channelsWithEndOfUserRecords[currentChannel->getChannelIndex()]) {
            throw std::runtime_error("Received more than one EndOfData from the same channel.");
        }

        channelsWithEndOfUserRecords[currentChannel->getChannelIndex()] = true;
        auto count = std::count(channelsWithEndOfUserRecords.begin(), channelsWithEndOfUserRecords.end(), true);
        hasReceivedEndOfData_ = count == static_cast<long>(numberOfInputChannels);
    }

    return new BufferOrEvent(
        event, hasPriority, currentChannel->getChannelInfo(), moreAvailable, size, morePriorityEvents);
}

Buffer* SingleInputGate::decompressBufferIfNeeded(Buffer* buffer)
{
    return buffer;
}

void SingleInputGate::markAvailable()
{
    std::shared_ptr<CompletableFuture> toNotify;
    {
        LOCK_BEFORE();
        std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
        LOCK_AFTER();

        toNotify = availabilityHelper.getUnavailableToResetAvailable();
    }
    toNotify->complete();
}

void SingleInputGate::sendTaskEvent(const std::shared_ptr<TaskEvent>& event)
{
    LOCK_BEFORE();
    std::unique_lock<std::recursive_mutex> lock(requestLock);
    LOCK_AFTER();

    for (auto& entry : inputChannels) {
        entry.second->sendTaskEvent(event);
    }

    if (numberOfUninitializedChannels > 0) {
        pendingEvents.push_back(event);
    }
}

void SingleInputGate::ResumeConsumption(const InputChannelInfo& channelInfo)
{
    if (IsFinished()) {
        throw std::runtime_error("Input gate is already finished.");
    }

    // BEWARE: consumption resumption only happens for streaming jobs in which all slots
    // are allocated together so there should be no UnknownInputChannel. As a result, it
    // is safe to not synchronize the requestLock here. We will refactor the code to not
    // rely on this assumption in the future.
    channels[channelInfo.getInputChannelIdx()]->resumeConsumption();
}

void SingleInputGate::TimeOutResumeConsumption(const InputChannelInfo& channelInfo)
{
    if (IsFinished()) {
        throw std::runtime_error("Input gate is already finished.");
    }
    channels[channelInfo.getInputChannelIdx()]->TimeOutResumeConsumption();
}

void SingleInputGate::acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo)
{
    if (IsFinished()) {
        throw std::runtime_error("Input gate is already finished.");
    }
    channels[channelInfo.getInputChannelIdx()]->acknowledgeAllRecordsProcessed();
}

// ------------------------------------------------------------------------
// Channel notifications
// ------------------------------------------------------------------------

void SingleInputGate::notifyChannelNonEmpty(std::shared_ptr<InputChannel> channel)
{
    if (!channel) {
        throw std::runtime_error("Input channel is null.");
    }
    // LOG_PART("begnning")
    queueChannel(channel, std::nullopt, false);
}

/**
 * Notifies that the respective channel has a priority event at the head for the given buffer
 * number.
 *
 * <p>The buffer number limits the notification to the respective buffer and voids the whole
 * notification in case that the buffer has been polled in the meantime. That is, if task thread
 * polls the enqueued priority buffer before this notification occurs (notification is not
 * performed under lock), this buffer number allows queueChannel to avoid spurious priority wake-ups.
 */
void SingleInputGate::notifyPriorityEvent(std::shared_ptr<InputChannel> inputChannel, int prioritySequenceNumber)
{
    if (!inputChannel) {
        throw std::invalid_argument("Input channel is null.");
    }
    queueChannel(inputChannel, prioritySequenceNumber, false);
}

void SingleInputGate::notifyPriorityEventForce(std::shared_ptr<InputChannel> inputChannel)
{
    if (!inputChannel) {
        throw std::runtime_error("Input channel is null.");
    }

    queueChannel(inputChannel, std::nullopt, true);
}

void SingleInputGate::triggerPartitionStateCheck(const ResultPartitionIDPOD& partitionId)
{
    NOT_IMPL_EXCEPTION;
    /**
    partitionProducerStateProvider->requestPartitionProducerState(
            consumedResultId,
            partitionId,
            [this, partitionId](std::shared_ptr<PartitionProducerStateProvider::ResponseHandle> responseHandle) {
                RemoteChannelStateChecker checker(partitionId, owningTaskName);
                bool isProducingState = checker.isProducerReadyOrAbortConsumption(responseHandle);
                if (isProducingState) {
                    try {
                        retriggerPartitionRequest(partitionId.getPartitionId());
                    } catch (const std::exception& t) {
                        responseHandle->failConsumption(t);
                    }
                }
            });
            **/
}

void SingleInputGate::queueChannel(
    std::shared_ptr<InputChannel> channel, std::optional<int> prioritySequenceNumber, bool forcePriority)
{
    // Using RAII-style notification helper (equivalent to Java's try-with-resources)
    class GateNotificationHelper {
    public:
        GateNotificationHelper(SingleInputGate* gate, std::condition_variable_any& cv) : inputGate(gate), cv_self(&cv)
        {
        }

        ~GateNotificationHelper()
        {
            if (toNotifyPriority != nullptr) {
                toNotifyPriority->complete();
            }
            if (toNotify != nullptr) {
                toNotify->complete();
            }
        }

        void notifyPriority()
        {
            toNotifyPriority = inputGate->priorityAvailabilityHelper.getUnavailableToResetAvailable();
        }
        void notifyDataAvailable()
        {
            cv_self->notify_all();
            toNotify = inputGate->availabilityHelper.getUnavailableToResetAvailable();
        }

    private:
        SingleInputGate* inputGate = nullptr;
        std::condition_variable_any* cv_self;
        std::shared_ptr<CompletableFuture> toNotifyPriority = nullptr;
        std::shared_ptr<CompletableFuture> toNotify = nullptr;
    };

    GateNotificationHelper notification(this, cv);

    {
        // INFO_RELEASE("notification start")
        std::unique_lock<std::recursive_mutex> lock(inputChannelsWithDataMutex);
        // INFO_RELEASE("notification get lock")
        bool priority = prioritySequenceNumber.has_value() || forcePriority;
        const int sleepTime = 100;
        if (!forcePriority && priority && prioritySequenceNumber.has_value() &&
            isOutdated(prioritySequenceNumber.value(), lastPrioritySequenceNumber[channel->getChannelIndex()])) {
            // priority event at the given offset already polled (notification is not atomic
            // in respect to buffer enqueuing), so just ignore the notification
            INFO_RELEASE("notify data buffer sleep1");
            // std::this_thread::sleep_for(std::chrono::microseconds(sleepTime)); //
            // sleep使inputChannelsWithDataMutex读写分配均匀
            return;
        }

        if (!queueChannelUnsafe(channel, priority)) {
            // INFO_RELEASE("data exist")
            //            std::this_thread::sleep_for(std::chrono::microseconds(sleepTime)); //
            //            sleep使inputChannelsWithDataMutex读写分配均匀
            return;
        }

        if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
            notification.notifyPriority();
        }
        if (inputChannelsWithData.size() == 1) {
            notification.notifyDataAvailable();
        }
        // INFO_RELEASE("notification release lock")
    }
}

bool SingleInputGate::isOutdated(int sequenceNumber, int lastSequenceNumber)
{
    if ((lastSequenceNumber < 0) != (sequenceNumber < 0) &&
        std::max(lastSequenceNumber, sequenceNumber) > INT32_MAX / 2) {
        // probably overflow of one of the two numbers, the negative one is greater then
        return lastSequenceNumber < 0;
    }
    return lastSequenceNumber >= sequenceNumber;
}

/**
 * Queues the channel if not already enqueued and not received EndOfPartition, potentially
 * raising the priority.
 *
 * @return true iff it has been enqueued/prioritized = some change to inputChannelsWithData happened
 */
bool SingleInputGate::queueChannelUnsafe(const std::shared_ptr<InputChannel>& channel, bool priority)
{
    if (channelsWithEndOfPartitionEvents[channel->getChannelIndex()]) {
        INFO_RELEASE("singleInputGate error");
        return false;
    }

    const bool alreadyEnqueued = enqueuedInputChannelsWithData[channel->getChannelIndex()];
    if (alreadyEnqueued && (!priority || inputChannelsWithData.containsPriorityElement(channel))) {
        // already notified / prioritized (double notification), ignore
        // INFO_RELEASE("channel exist, alreadyEnqueued: " + std::to_string(alreadyEnqueued) + ", priority: " +
        // std::to_string(priority) + ", contains: " +
        // std::to_string(inputChannelsWithData.containsPriorityElement(channel)))
        return false;
    }

    inputChannelsWithData.add(channel, priority, alreadyEnqueued);
    if (!alreadyEnqueued) {
        enqueuedInputChannelsWithData[channel->getChannelIndex()] = true;
    }
    return true;
}

std::optional<std::shared_ptr<InputChannel>> SingleInputGate::getChannel(
    bool blocking, std::unique_lock<std::recursive_mutex>& lock)
{
    while (inputChannelsWithData.isEmpty()) {
        if (closeFuture->isDone()) {
            throw std::runtime_error("Released");
        }

        if (blocking) {
            cv.wait(lock, [this] { return !inputChannelsWithData.isEmpty(); });
        } else {
            availabilityHelper.resetUnavailable();
            return std::nullopt;
        }
    }

    std::shared_ptr<InputChannel> inputChannel = inputChannelsWithData.poll();
    if (!inputChannel) {
        INFO_RELEASE("input channel is null");
        throw std::runtime_error("input channel is null");
    }
    enqueuedInputChannelsWithData[inputChannel->getChannelIndex()] = false;

    return inputChannel;
}

// ------------------------------------------------------------------------

std::unordered_map<IntermediateResultPartitionIDPOD, std::shared_ptr<InputChannel>>& SingleInputGate::getInputChannels()
{
    return inputChannels;
}

std::string SingleInputGate::toString()
{
    std::stringstream ss;
    ss << "SingleInputGate {" << std::endl;
    ss << "  owningTaskName: \"" << owningTaskName << "\"," << std::endl;
    ss << "  gateIndex: " << gateIndex << "," << std::endl;
    ss << "  consumedResultId: " << consumedResultId.toString() << ","
       << std::endl; // Assuming IntermediateDataSetIDPOD has a toString()
    ss << "  consumedPartitionType: " << consumedPartitionType << "," << std::endl;
    ss << "  consumedSubpartitionIndex: " << consumedSubpartitionIndex << "," << std::endl;
    ss << "  numberOfInputChannels: " << numberOfInputChannels << "," << std::endl;

    ss << "  inputChannels: {" << std::endl;
    for (const auto& pair : inputChannels) {
        ss << "    [" << pair.first.toString() << "]: " << (pair.second ? pair.second->toString() : "nullptr") << ","
           << std::endl; // Assuming InputChannel has a toString()
    }
    ss << "  }," << std::endl;

    ss << "  channels: [" << std::endl;
    for (const auto& channel : channels) {
        ss << "    " << (channel ? channel->toString() : "nullptr") << ","
           << std::endl; // Assuming InputChannel has a toString()
    }
    ss << "  ]," << std::endl;

    ss << "  inputChannelsWithData: [" << std::endl;
    for (const auto& channel : inputChannelsWithData.asUnmodifiableCollection()) {
        ss << "    " << (channel ? channel->toString() : "nullptr") << ","
           << std::endl; // Assuming InputChannel has a toString()
    }
    ss << "  ]," << std::endl;

    ss << "  lastPrioritySequenceNumber: [";
    for (size_t i = 0; i < lastPrioritySequenceNumber.size(); ++i) {
        ss << lastPrioritySequenceNumber[i];
        if (i < lastPrioritySequenceNumber.size() - 1) {
            ss << ", ";
        }
    }
    ss << "]," << std::endl;

    ss << "  partitionProducerStateProvider: " << (partitionProducerStateProvider ? "present" : "nullptr") << ","
       << std::endl; // No good way to show shared pointer content directly
    ss << "  bufferPool: " << (bufferPool ? "present" : "nullptr") << ","
       << std::endl; // No good way to show shared pointer content directly
    ss << "  hasReceivedAllEndOfPartitionEvents: " << std::boolalpha << hasReceivedAllEndOfPartitionEvents << ","
       << std::endl;
    ss << "  hasReceivedEndOfData_: " << std::boolalpha << hasReceivedEndOfData_ << "," << std::endl;
    ss << "  requestedPartitionsFlag: " << std::boolalpha << requestedPartitionsFlag << "," << std::endl;

    ss << "  pendingEvents: [" << std::endl;
    for (const auto& event : pendingEvents) {
        ss << "    " << (event ? "event->toString()" : "nullptr") << ","
           << std::endl; // Assuming TaskEvent has a toString()
    }
    ss << "  ]," << std::endl;

    ss << "  numberOfUninitializedChannels: " << numberOfUninitializedChannels << "," << std::endl;
    ss << "  bufferPoolFactory: " << (bufferPoolFactory ? "present" : "nullptr") << ","
       << std::endl; // Can't show function pointer content
    ss << "  closeFuture: " << (closeFuture ? "present" : "nullptr") << ","
       << std::endl; // No good way to show future data

    ss << "  objectSegmentProvider: " << (segmentProvider ? "present" : "nullptr") << "," << std::endl;

    ss << "}";
    return ss.str();
}

void SingleInputGate::changeLocalInputChannelToOriginal(int channelIndex, std::shared_ptr<InputChannel> original)
{
    channels[channelIndex] = original;
}

} // namespace omnistream

/////////////////////////////
