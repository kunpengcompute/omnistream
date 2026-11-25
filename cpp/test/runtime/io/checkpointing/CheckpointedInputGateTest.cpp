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

#include <gtest/gtest.h>
#include <memory>

#include "runtime/io/checkpointing/CheckpointedInputGate.h"
#include "runtime/io/checkpointing/SingleCheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/partition/consumer/BufferOrEvent.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "runtime/metrics/SystemClock.h"
#include "MockClasses.h"

using namespace omnistream;
using namespace omnistream::runtime;
class DummyInputGate : public InputGate {
public:
    BufferOrEvent* PollNext() override
    {
        return GetNext();
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
        return nullptr;
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
};

class DummyTask : public CheckpointableTask {
public:
    void TriggerCheckpointOnBarrier(
        std::shared_ptr<CheckpointMetaData> checkpointMetaData,
        std::shared_ptr<CheckpointOptions> checkpointOptions,
        std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics) override
    {
    }

    void abortCheckpointOnBarrier(long, CheckpointException) override
    {
    }
};

class DummyCoordinator : public SubtaskCheckpointCoordinator {
public:
    void InitInputsCheckpoint(long, CheckpointOptions*) override
    {
    }
    std::shared_ptr<ChannelStateWriter> getChannelStateWriter() override
    {
    }
};

TEST(CheckpointedInputGateTest, DISABLED_PollNext_ProcessesCheckpointBarrierEvent)
{
    auto inputGate = std::make_shared<DummyInputGate>();
    auto task = std::make_shared<DummyTask>();
    auto coordinator = std::make_shared<DummyCoordinator>();
    auto mailbox = std::make_shared<MailboxExecutorTest>();
    auto timerService = std::make_shared<SystemProcessingTimeService>();
    auto delayableTimer =
        BarrierAlignmentUtil::createRegisterTimerCallback<std::function<void()>>(mailbox.get(), timerService.get());
    auto mockInput = new TestInput(0);
    std::vector<CheckpointableInput*> inputs = {mockInput};

    auto initialState = new AlternatingWaitingForFirstBarrierUnaligned(false, ChannelState(inputs));

    auto handler = std::make_shared<SingleCheckpointBarrierHandler>(
        "test-task",
        task.get(),
        coordinator.get(),
        SystemClock::GetInstance(),
        1,
        initialState,
        false,
        delayableTimer,
        inputs,
        false);

    CheckpointedInputGate* gate = new CheckpointedInputGate(inputGate, handler, mailbox);

    auto result = gate->PollNext();
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->isEvent());
    EXPECT_EQ(result->getEvent()->GetEventClassName(), "CheckpointBarrier");
    EXPECT_EQ(gate->GetLatestCheckpointId(), 1);
    EXPECT_EQ(gate->AllChannelsRecovered(), true);
    delete result;
    delete gate;
}
