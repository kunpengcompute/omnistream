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

#ifndef OMNISTREAM_RECOVEREDINPUTCHANNEL_H
#define OMNISTREAM_RECOVEREDINPUTCHANNEL_H

#include "deque"
#include "stdint.h"
#include "InputChannel.h"
#include "mutex"
#include "runtime/buffer/Buffer.h"
#include "runtime/buffer/ObjectBufferDataType.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/partition/consumer/SingleInputGate.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "runtime/metrics/Counter.h"
#include "core/include/common.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "runtime/event/EndOfChannelStateEvent.h"
#include "BufferAndAvailability.h"
#include "BufferManager.h"
#include "core/utils/threads/CompletableFutureV2.h"

class RecoveredInputChannel : public omnistream::InputChannel {
public:
    RecoveredInputChannel(
        std::shared_ptr<omnistream::SingleInputGate> inputGate,
        int channelIndex,
        omnistream::ResultPartitionIDPOD partitionId,
        int consumedSubpartitionIndex,
        int initialBackoff,
        int maxBackoff,
        std::shared_ptr<omnistream::Counter> numBytesIn,
        std::shared_ptr<omnistream::Counter> numBuffersIn,
        int networkBuffersPerChannel)
        : omnistream::InputChannel(
              inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, numBytesIn, numBuffersIn)
    {
        setConsumedSubpartitionIndex(consumedSubpartitionIndex);
        bufferManager = std::make_shared<BufferManager>(inputGate->getSegmentProvider(), this, 1);
        this->networkBuffersPerChannel = networkBuffersPerChannel;
        stateConsumedFuture = std::make_shared<CompletableFutureV2<void>>();
    }

    void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> stateWriter)
    {
        if (stateWriter == nullptr) {
            LOG("invalid param, nullptr!");
            return;
        }
        if (this->channelStateWriter != nullptr) {
            LOG("channel state writer already init!");
            return;
        }
        this->channelStateWriter = stateWriter;
    }

    std::shared_ptr<ChannelStateWriter> GetChannelStateWriter() const
    {
        return channelStateWriter;
    }

    std::shared_ptr<omnistream::InputChannel> toInputChannel();

    virtual std::shared_ptr<omnistream::InputChannel> toInputChannelInternal() = 0;

    void CheckpointStopped(long checkpointId) override
    {
        lastStoppedCheckpointId = checkpointId;
    }

    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture()
    {
        return stateConsumedFuture;
    }

    bool getStateConsumedFuture1()
    {
        return stateConsumedFuture1.load();
    }

    void onRecoveredStateBuffer(Buffer* buffer);
    void onRecoveredStateBuffer2(Buffer* buffer);
    void onRecoveredStateBufferForObjectBuffer(Buffer* buffer);
    bool isObjectBufferPool();

    void finishReadRecoveredState();

    void finishInnerRecoveredState();

    std::optional<omnistream::BufferAndAvailability> getNextRecoveredStateBuffer();

    omnistream::ObjectBufferDataType peekDataTypeUnsafe();

    bool isEndOfChannelStateEvent(Buffer* buffer);

    bool isInnerRecoverEvent(Buffer* buffer);

    std::optional<BufferAndAvailability> getNextBuffer() override;

    int getBuffersInUseCount() override
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        return static_cast<int>(receivedBuffers.size());
    }

    void resumeConsumption() override
    {
        throw std::invalid_argument("RecoveredInputChannel should never be blocked.");
    }
    void TimeOutResumeConsumption() override
    {
    }
    void acknowledgeAllRecordsProcessed() override
    {
        throw std::invalid_argument("RecoveredInputChannel should not need acknowledge all records processed.");
    }

    void requestSubpartition(int subpartitionIndex) override
    {
        throw std::invalid_argument("RecoveredInputChannel should never request partition.");
    }

    void sendTaskEvent(std::shared_ptr<TaskEvent> event) override
    {
        throw std::invalid_argument("RecoveredInputChannel should never send any task events.");
    }

    bool isReleased() override
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        return released;
    }

    void releaseAllResources() override;

    int getNumberOfQueuedBuffers()
    {
        std::lock_guard<std::mutex> lock(bufferLock);
        return static_cast<int>(receivedBuffers.size());
    }

    std::shared_ptr<omnistream::Buffer> requestBufferBlocking();

    void CheckpointStarted(
        const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter) override
    {
        throw std::invalid_argument("Checkpoint was declined (tasks not ready)");
    }

    void announceBufferSize(int newBufferSize) override
    {
    }

    int getNetworkBuffersPerChannel()
    {
        return networkBuffersPerChannel;
    }

    void SetIsOmniChannel(bool isOmniChannel)
    {
        toOmniChannel_ = isOmniChannel;
    }

    bool IsOmniChannel() const
    {
        return toOmniChannel_;
    }
    void SetPersistenceFlag(bool flag)
    {
    }

    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
    {
    }
    bool IsNeedPersistence()
    {
        return false;
    }

private:
    struct RecoveredBufferEntry {
        Buffer* buffer;
        std::shared_ptr<omnistream::Buffer> owner;

        RecoveredBufferEntry(Buffer* b, std::shared_ptr<omnistream::Buffer> o = nullptr)
            : buffer(b),
              owner(std::move(o))
        {
        }
    };

    std::deque<RecoveredBufferEntry> receivedBuffers;
    std::deque<std::shared_ptr<omnistream::Buffer>> consumedRecoveredBufferOwners;

    std::shared_ptr<CompletableFutureV2<void>> stateConsumedFuture;
    std::atomic<bool> stateConsumedFuture1{false};
    std::shared_ptr<BufferManager> bufferManager;
    bool released = false;
    std::shared_ptr<ChannelStateWriter> channelStateWriter;
    int sequenceNumber = std::numeric_limits<int>::min();
    int networkBuffersPerChannel;
    bool exclusiveBuffersAssigned = false;
    long lastStoppedCheckpointId = -1;
    bool toOmniChannel_ = false;
    std::mutex bufferLock;
};

#endif // OMNISTREAM_RECOVEREDINPUTCHANNEL_H
