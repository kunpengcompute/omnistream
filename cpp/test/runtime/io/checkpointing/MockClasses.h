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

#include "runtime/io/checkpointing/CheckpointedInputGate.h"
#include "runtime/io/checkpointing/SingleCheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
#include "runtime/io/checkpointing/BarrierHandlerState.h"
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/partition/consumer/BufferOrEvent.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "runtime/metrics/SystemClock.h"
#include "runtime/metrics/Counter.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/partition/consumer/SingleInputGate.h"

#ifndef OMNISTREAM_MOCKCLASSES_H
#define OMNISTREAM_MOCKCLASSES_H

class TestInput : public CheckpointableInput {
public:
    explicit TestInput(int inputGateIndex, int numChannels = 1)
        : inputGateIndex_(inputGateIndex),
          numChannels_(numChannels)
    {
    }

    void BlockConsumption(const InputChannelInfo& info) override
    {
        blockedChannels_.insert(info.getInputChannelIdx());
    }

    void ResumeConsumption(const InputChannelInfo& info) override
    {
        ++normalResumeCount_;
        resumedChannels_.insert(info.getInputChannelIdx());
    }
    void TimeOutResumeConsumption(const InputChannelInfo& info) override
    {
        ++timeoutResumeCount_;
        resumedChannels_.insert(info.getInputChannelIdx());
    }
    void ConvertToPriorityEvent(int, int) override
    {
    }

    std::vector<InputChannelInfo> GetChannelInfos() override
    {
        std::vector<InputChannelInfo> infos;
        for (int i = 0; i < numChannels_; ++i) {
            infos.emplace_back(inputGateIndex_, i);
        }
        return infos;
    }

    int GetNumberOfInputChannels()
    {
        return numChannels_;
    }

    void CheckpointStarted(const CheckpointBarrier&) override
    {
        started = true;
    }

    void CheckpointStopped(long) override
    {
        stopped = true;
    }

    void notifyDataAvailable() override
    {
    }

    int GetInputGateIndex() override
    {
        return inputGateIndex_;
    }

    // Test state fields
    int inputGateIndex_;
    int numChannels_;
    std::unordered_set<int> blockedChannels_;
    std::unordered_set<int> resumedChannels_;
    int normalResumeCount_ = 0;
    int timeoutResumeCount_ = 0;
    bool started = false;
    bool stopped = false;
};

class MailboxExecutorTest : public omnistream::MailboxExecutor {
public:
    MailboxExecutorTest() = default;

    void execute(std::shared_ptr<omnistream::ThrowingRunnable> command, const std::string& description) override
    {
        command->Run();
    }

    void execute(
        std::shared_ptr<omnistream::ThrowingRunnable> command,
        const std::string& descriptionFormat,
        const std::vector<std::any>& descriptionArgs) override
    {
        // Mock implementation for testing
    }

    void yield() override
    {
    }

    bool tryYield() override
    {
        return true;
    }

    std::string toString() const override
    {
        return "MockMailboxExecutor";
    }
};

using namespace omnistream;

class DummyCounter : public Counter {
public:
    void Inc() override
    {
    }
    void Inc(long) override
    {
    }
    void Dec() override
    {
    }
    void Dec(long) override
    {
    }
    long GetCount() override
    {
        return 0;
    }
    MetricType GetMetricType() const override
    {
        return MetricType::COUNTER;
    }
};

class DummyInputChannel : public InputChannel {
public:
    explicit DummyInputChannel(std::shared_ptr<SingleInputGate> gate, InputChannelInfo info)
        : InputChannel(
              gate,
              0,
              ResultPartitionIDPOD(),
              0,
              0,
              std::make_shared<DummyCounter>(),
              std::make_shared<DummyCounter>()),
          info_(std::move(info))
    {
    }

    InputChannelInfo getChannelInfo() const
    {
        return info_;
    }

    void resumeConsumption() override
    {
    }
    void TimeOutResumeConsumption() override
    {
    }
    void acknowledgeAllRecordsProcessed() override
    {
    }
    void requestSubpartition(int) override
    {
    }
    std::optional<BufferAndAvailability> getNextBuffer() override
    {
        return std::nullopt;
    }
    void sendTaskEvent(std::shared_ptr<TaskEvent>) override
    {
    }
    bool isReleased() override
    {
        return false;
    }
    void releaseAllResources() override
    {
    }
    void announceBufferSize(int) override
    {
    }
    int getBuffersInUseCount() override
    {
        return 0;
    }
    void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) override
    {
    }
    std::string toString() override
    {
        return "DummyInputChannel";
    }
    void SetPersistenceFlag(bool flag) override
    {
    }
    bool IsNeedPersistence() override
    {
        return false;
    }
    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info) override
    {
    }

private:
    InputChannelInfo info_;
};

class SingleChannelDummyInputGate : public IndexedInputGate {
public:
    SingleChannelDummyInputGate(int idx)
        : singleGate_(
              std::make_shared<SingleInputGate>(
                  "dummyTask",
                  idx,
                  IntermediateDataSetIDPOD(),
                  0,
                  0,
                  1,
                  nullptr,
                  std::function<std::shared_ptr<BufferPool>()>(),
                  nullptr,
                  1024))
    {
        dummyChannel_ = std::make_shared<DummyInputChannel>(singleGate_, InputChannelInfo(0, 0));
    }
    BufferOrEvent* PollNext() override
    {
        return GetNext();
    }

    int GetGateIndex() override
    {
        return singleGate_->GetGateIndex();
    }

    std::vector<InputChannelInfo> getUnfinishedChannels() override
    {
        return {InputChannelInfo(0, 0)};
    }

    int getBuffersInUseCount() override
    {
        return singleGate_->getBuffersInUseCount();
    }

    void announceBufferSize(int bufferSize) override
    {
        singleGate_->announceBufferSize(bufferSize);
    }

    BufferOrEvent* GetNext() override
    {
        if (emitted_) {
            finished_ = true;
            return nullptr;
        }

        // Create a dummy checkpoint barrier event
        auto options =
            new CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());

        auto barrier = std::make_shared<CheckpointBarrier>(1L, 999L, options);
        auto boe = std::make_shared<BufferOrEvent>(barrier, InputChannelInfo(0, 0));

        emitted_ = true;
        return boe.get();
    }

    int GetNumberOfInputChannels() override
    {
        return 1;
    }

    bool IsFinished() override
    {
        return finished_;
    }

    bool HasReceivedEndOfData() override
    {
        return finished_;
    }

    void sendTaskEvent(const std::shared_ptr<TaskEvent>&) override
    {
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        return std::make_shared<CompletableFuture>();
    }

    void ResumeConsumption(const InputChannelInfo&) override
    {
    }

    void acknowledgeAllRecordsProcessed(const InputChannelInfo&) override
    {
    }

    std::shared_ptr<InputChannel> getChannel(int) override
    {
        return dummyChannel_;
    }

    std::vector<InputChannelInfo> getChannelInfos()
    {
        return {InputChannelInfo(0, 0)};
    }

    void setup() override
    {
    }

    void RequestPartitions() override
    {
    }

    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture() override
    {
        return std::make_shared<CompletableFutureV2<void>>();
    }

    std::vector<bool> getStateConsumedFuture1() override
    {
        return {};
    }

    void FinishReadRecoveredState() override
    {
    }

private:
    bool emitted_;
    bool finished_;
    std::shared_ptr<InputChannel> dummyChannel_;
    std::shared_ptr<SingleInputGate> singleGate_;
};

// multi-channel DummyInputGate
class MultiChannelDummyInputGate : public InputGate {
public:
    MultiChannelDummyInputGate()
        : singleGate_(
              std::make_shared<SingleInputGate>(
                  "dummyTask",
                  0,
                  IntermediateDataSetIDPOD(),
                  0,
                  0,
                  2,
                  nullptr,
                  std::function<std::shared_ptr<BufferPool>()>(),
                  nullptr,
                  1024))
    {
        // Create 2 channels and their infos
        for (int idx = 0; idx < 2; ++idx) {
            InputChannelInfo info(0, idx);
            infos_.push_back(info);
            channels_.push_back(std::make_shared<DummyInputChannel>(singleGate_, info));
        }
    }

    BufferOrEvent* PollNext() override
    {
        return GetNext();
    }

    int GetNumberOfInputChannels() override
    {
        return 2;
    }

    std::vector<InputChannelInfo> getChannelInfos()
    {
        return infos_;
    }

    std::shared_ptr<InputChannel> getChannel(int idx) override
    {
        return channels_.at(idx);
    }

    BufferOrEvent* GetNext() override
    {
        return nullptr;
    }
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        return {};
    }
    bool IsFinished() override
    {
        return false;
    }
    bool HasReceivedEndOfData() override
    {
        return false;
    }
    void sendTaskEvent(const std::shared_ptr<TaskEvent>&) override
    {
    }
    void ResumeConsumption(const InputChannelInfo&) override
    {
    }
    void acknowledgeAllRecordsProcessed(const InputChannelInfo&) override
    {
    }
    void setup() override
    {
    }
    void RequestPartitions() override
    {
    }
    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture() override
    {
        return {};
    }
    std::vector<bool> getStateConsumedFuture1() override
    {
        return {};
    }
    void FinishReadRecoveredState() override
    {
    }

private:
    std::shared_ptr<SingleInputGate> singleGate_;
    std::vector<InputChannelInfo> infos_;
    std::vector<std::shared_ptr<InputChannel>> channels_;
};

#endif // OMNISTREAM_MOCKCLASSES_H
