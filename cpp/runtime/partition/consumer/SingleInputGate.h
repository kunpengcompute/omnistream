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

#ifndef SINGLEINPUTGATE_H
#define SINGLEINPUTGATE_H

#pragma once

#include <vector>
#include <map>
#include <string>
#include <mutex>
#include <memory>
#include <optional>
#include <functional>
#include <bitset>
#include <buffer/ObjectBufferPool.h>

#include "IndexedInputGate.h"
#include "InputChannel.h"
#include "InputChannelInfo.h"
#include <buffer/ObjectSegment.h>
#include <buffer/ObjectBufferProvider.h>
#include <buffer/ObjectBufferPool.h>
#include <buffer/ObjectBufferRecycler.h>

#include <partition/PrioritizedDeque.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>

#include <buffer/ObjectSegmentProvider.h>
#include <executiongraph/descriptor/IntermediateResultPartitionIDPOD.h>

#include "BufferOrEvent.h"
#include <executiongraph/descriptor/ResourceIDPOD.h>
#include <partition/PartitionProducerStateProvider.h>

#include <event/AbstractEvent.h>
#include <event/TaskEvent.h>
#include <executiongraph/descriptor/ShuffleDescriptorPOD.h>
#include <utils/lang/AutoCloseable.h>
#include "buffer/BufferPool.h"
#include "buffer/SegmentProvider.h"

namespace omnistream {
class SingleInputGate : public IndexedInputGate, public AutoCloseable {
public:
    SingleInputGate(
        const std::string& owningTaskName,
        int gateIndex,
        const IntermediateDataSetIDPOD& consumedResultId,
        const int consumedPartitionType,
        int consumedSubpartitionIndex,
        int numberOfInputChannels,
        std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
        std::function<std::shared_ptr<BufferPool>()> bufferPoolFactory,
        std::shared_ptr<SegmentProvider> segmentProvider,
        int segmentSize);

    void setup() override;
    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture() override;
    std::vector<bool> getStateConsumedFuture1() override;
    void RequestPartitions() override;
    void FinishReadRecoveredState() override;
    void FinishInnerRecoveredState() override;

    int GetNumberOfInputChannels() override;
    int GetGateIndex() override;
    std::vector<InputChannelInfo> getUnfinishedChannels() override;
    int getBuffersInUseCount() override;
    void announceBufferSize(int newBufferSize) override;
    std::shared_ptr<InputChannel> getChannel(int channelIndex) override;

    int getConsumedPartitionType();
    // std::shared_ptr<ObjectBufferProvider> getBufferProvider();
    std::shared_ptr<BufferProvider> getBufferProvider();
    // std::shared_ptr<ObjectBufferPool> getBufferPool();
    std::shared_ptr<BufferPool> getBufferPool();
    std::shared_ptr<SegmentProvider> getSegmentProvider();
    std::string getOwningTaskName();
    int getNumberOfQueuedBuffers();
    std::shared_ptr<CompletableFuture> getCloseFuture();

    void setBufferPool(std::shared_ptr<BufferPool> bufferPool);
    void setupChannels();
    void setInputChannels(std::vector<std::shared_ptr<InputChannel>> channels);
    void updateInputChannel(const ResourceIDPOD& localLocation, const ShuffleDescriptorPOD& shuffleDescriptor);
    void retriggerPartitionRequest(const IntermediateResultPartitionIDPOD& partitionId);

    void close() override;
    bool IsFinished() override;
    bool HasReceivedEndOfData() override;

    BufferOrEvent* GetNext() override;
    BufferOrEvent* PollNext() override;
    BufferOrEvent* getNextBufferOrEvent(bool blocking);

    void sendTaskEvent(const std::shared_ptr<TaskEvent>& event) override;
    void ResumeConsumption(const InputChannelInfo& channelInfo) override;
    void TimeOutResumeConsumption(const InputChannelInfo& channelInfo) override;
    void acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo) override;

    void notifyChannelNonEmpty(std::shared_ptr<InputChannel> channel);
    void notifyPriorityEvent(std::shared_ptr<InputChannel> inputChannel, int prioritySequenceNumber);
    void notifyPriorityEventForce(std::shared_ptr<InputChannel> inputChannel);
    void triggerPartitionStateCheck(const ResultPartitionIDPOD& partitionId);
    void queueChannel(
        std::shared_ptr<InputChannel> channel, std::optional<int> prioritySequenceNumber, bool forcePriority);

    PrioritizedDeque<InputChannel> getInputChannelsWithData();
    std::unordered_map<IntermediateResultPartitionIDPOD, std::shared_ptr<InputChannel>>& getInputChannels();

    std::string toString() override;
    void changeLocalInputChannelToOriginal(int channelIndex, std::shared_ptr<InputChannel> original);

private:
    void convertRecoveredInputChannels();
    void internalRequestPartitions();

    template <typename T>
    class InputWithData {
    public:
        std::shared_ptr<InputChannel> input;
        T data;
        bool moreAvailable;
        bool morePriorityEvents;

        InputWithData(std::shared_ptr<InputChannel> input, T data, bool moreAvailable, bool morePriorityEvents)
            : input(input),
              data(data),
              moreAvailable(moreAvailable),
              morePriorityEvents(morePriorityEvents)
        {
        }
    };

    SingleInputGate::InputWithData<BufferAndAvailability>* waitAndGetNextData(bool blocking);
    void checkUnavailability();
    BufferOrEvent* transformToBufferOrEvent(
        Buffer* buffer, bool moreAvailable, std::shared_ptr<InputChannel> currentChannel, bool morePriorityEvents);
    BufferOrEvent* transformBuffer(
        Buffer* buffer, bool moreAvailable, std::shared_ptr<InputChannel> currentChannel, bool morePriorityEvents);
    BufferOrEvent* transformEvent(
        Buffer* buffer, bool moreAvailable, std::shared_ptr<InputChannel> currentChannel, bool morePriorityEvents);
    Buffer* decompressBufferIfNeeded(Buffer* buffer);
    void markAvailable();
    bool isOutdated(int sequenceNumber, int lastSequenceNumber);
    bool queueChannelUnsafe(const std::shared_ptr<InputChannel>& channel, bool priority);
    std::optional<std::shared_ptr<InputChannel>> getChannel(
        bool blocking, std::unique_lock<std::recursive_mutex>& lock);

    // Lock object to guard partition requests and runtime channel updates
    std::recursive_mutex requestLock;

    // The name of the owning task, for logging purposes
    std::string owningTaskName;

    int gateIndex;

    // The ID of the consumed intermediate result
    IntermediateDataSetIDPOD consumedResultId;

    // The type of the partition the input gate is consuming
    int consumedPartitionType;

    // The index of the consumed subpartition of each consumed partition
    int consumedSubpartitionIndex;

    // The number of input channels
    int numberOfInputChannels;

    // Input channels
    std::unordered_map<IntermediateResultPartitionIDPOD, std::shared_ptr<InputChannel>> inputChannels;

    std::vector<std::shared_ptr<InputChannel>> channels;

    // Channels, which notified this input gate about available data
    PrioritizedDeque<InputChannel> inputChannelsWithData;

    // Synchronization for inputChannelsWithData
    std::recursive_mutex inputChannelsWithDataMutex;
    std::condition_variable_any cv;

    // Field guaranteeing uniqueness for inputChannelsWithData queue
    std::vector<bool> enqueuedInputChannelsWithData;

    std::vector<bool> channelsWithEndOfPartitionEvents;
    std::vector<bool> channelsWithEndOfUserRecords;

    std::vector<int> lastPrioritySequenceNumber;

    // The partition producer state listener
    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider;

    // Buffer pool for incoming buffers, e.g. LocalMemoryBufferPool
    std::shared_ptr<BufferPool> bufferPool;

    // Factory for creating buffer pool
    std::function<std::shared_ptr<BufferPool>()> bufferPoolFactory;

    std::shared_ptr<SegmentProvider> segmentProvider;

    bool hasReceivedAllEndOfPartitionEvents;
    bool hasReceivedEndOfData_;

    // Flag indicating whether partitions have been requested
    bool requestedPartitionsFlag;

    std::vector<std::shared_ptr<TaskEvent>> pendingEvents;

    int numberOfUninitializedChannels;

    std::shared_ptr<CompletableFuture> closeFuture;

    // Buffer decompressor
    // Segment to read data from file region
    // ObjectSegment *unpooledSegment; // todo: need fix
    bool shouldDrainOnEndOfData = true;
};
} // namespace omnistream

#endif
