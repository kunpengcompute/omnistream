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
#include "streaming/runtime/io/checkpointing/AbstractAlternatingAlignedBarrierHandlerState.h"
#include "streaming/runtime/io/checkpointing/AlternatingWaitingForFirstBarrier.h"
#include "streaming/runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
#include "runtime/io/network/api/CheckpointBarrier.h"

class TestCheckpointableInput : public CheckpointableInput {
public:
    void BlockConsumption(const InputChannelInfo&) override
    {
        blocked = true;
    }
    void ResumeConsumption(const InputChannelInfo&) override
    {
        resumed = true;
    }
    void TimeOutResumeConsumption(const InputChannelInfo&) override
    {
        resumed = true;
    }
    void ConvertToPriorityEvent(int, int) override
    {
    }
    std::vector<InputChannelInfo> GetChannelInfos() override
    {
        return {InputChannelInfo(GetInputGateIndex(), 0)};
    }
    int GetNumberOfInputChannels()
    {
        return 1;
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
        return 0;
    }
    bool blocked = false;
    bool resumed = false;
    bool started = false;
    bool stopped = false;
};

class TestController : public Controller {
public:
    bool AllBarriersReceived() const override
    {
        return allReceived;
    }
    const CheckpointBarrier* GetPendingCheckpointBarrier() const override
    {
        return nullptr;
    }
    void TriggerGlobalCheckpoint(const CheckpointBarrier&) override
    {
        triggered = true;
    }
    void InitInputsCheckpoint(const CheckpointBarrier&) override
    {
        initialized = true;
    }
    bool IsTimedOut(const CheckpointBarrier&) override
    {
        return timedOut;
    }
    bool allReceived = false;
    bool triggered = false;
    bool initialized = false;
    bool timedOut = false;
};

TEST(BarrierStateTest, AlignedCheckpointInitialization)
{
    TestCheckpointableInput input0, input1;
    std::vector<CheckpointableInput*> inputs = {&input0, &input1};
    ChannelState state(inputs);
    TestController controller;

    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    CheckpointOptions* options = new CheckpointOptions(checkpointType, targetLocation);
    CheckpointBarrier barrier(101, 999999L, options);
    auto* statePtr = new AlternatingWaitingForFirstBarrier(state);
    BarrierHandlerState* nextState = statePtr->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, true);

    EXPECT_TRUE(input0.blocked);
    EXPECT_FALSE(controller.initialized);
    EXPECT_FALSE(controller.triggered);

    controller.allReceived = true;
    BarrierHandlerState* finalState = nextState->BarrierReceived(&controller, InputChannelInfo(1, 0), &barrier, true);

    EXPECT_TRUE(controller.initialized);
    EXPECT_TRUE(controller.triggered);
    EXPECT_TRUE(input0.resumed);
    EXPECT_TRUE(input1.resumed);
    EXPECT_FALSE(input0.stopped);
    EXPECT_FALSE(input1.stopped);

    delete statePtr;
    delete nextState;
    delete finalState;
}

TEST(BarrierStateTest, DISABLED_FallbackToUnalignedOnTimeout)
{
    TestCheckpointableInput input0, input1;
    std::vector<CheckpointableInput*> inputs = {&input0, &input1};
    ChannelState state(inputs);
    TestController controller;

    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    CheckpointOptions* options = new CheckpointOptions(checkpointType, targetLocation);
    CheckpointBarrier barrier(123, 456789L, options);
    BarrierHandlerState* statePtr = new AlternatingWaitingForFirstBarrier(state);
    BarrierHandlerState* nextState = statePtr->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, true);

    EXPECT_TRUE(input0.blocked);
    EXPECT_FALSE(controller.initialized);
    EXPECT_FALSE(controller.triggered);

    controller.timedOut = true;

    BarrierHandlerState* unalignedState =
        nextState->BarrierReceived(&controller, InputChannelInfo(1, 0), &barrier, true);

    EXPECT_TRUE(input0.started);
    EXPECT_TRUE(input1.started);
    EXPECT_TRUE(controller.initialized);
    EXPECT_TRUE(controller.triggered);

    controller.initialized = false;
    controller.triggered = false;
    input0.resumed = input1.resumed = false;
    input0.stopped = input1.stopped = false;
    controller.allReceived = true;

    BarrierHandlerState* finalState =
        unalignedState->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, false);

    EXPECT_TRUE(input0.resumed);
    EXPECT_TRUE(input1.resumed);
    EXPECT_TRUE(input0.stopped);
    EXPECT_TRUE(input1.stopped);

    delete statePtr;
    delete nextState;
    delete unalignedState;
    delete finalState;
}

TEST(BarrierStateTest, DISABLED_WaitingUnalignedOnImmediateTimeout)
{
    TestCheckpointableInput input0, input1;
    std::vector<CheckpointableInput*> inputs = {&input0, &input1};
    ChannelState state(inputs);
    TestController controller;

    controller.timedOut = true;

    CheckpointOptions* options =
        new CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(999, 111L, options);

    BarrierHandlerState* statePtr = new AlternatingWaitingForFirstBarrier(state);
    BarrierHandlerState* unalignedWaiting =
        statePtr->BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, true);

    EXPECT_FALSE(input0.resumed);
    EXPECT_FALSE(input1.resumed);
    EXPECT_TRUE(input0.started);
    EXPECT_TRUE(input1.started);
    EXPECT_TRUE(controller.initialized);
    EXPECT_TRUE(controller.triggered);

    delete statePtr;
    delete unalignedWaiting;
}

TEST(BarrierStateTest, WaitingUnalignedFinishCheckpoint)
{
    TestCheckpointableInput input0, input1;
    std::vector<CheckpointableInput*> inputs = {&input0, &input1};
    ChannelState state(inputs);

    TestController controller;
    controller.allReceived = true;

    CheckpointOptions* options =
        new CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
    CheckpointBarrier barrier(555, 777L, options);

    AlternatingWaitingForFirstBarrierUnaligned unalignedState(/*alternating=*/true, state);

    BarrierHandlerState* next = unalignedState.BarrierReceived(&controller, InputChannelInfo(0, 0), &barrier, true);

    EXPECT_TRUE(input0.started);
    EXPECT_TRUE(input1.started);
    EXPECT_TRUE(input0.stopped);
    EXPECT_TRUE(input1.stopped);
    EXPECT_TRUE(input0.resumed);
    EXPECT_FALSE(input1.resumed);

    auto* asAlignedWaiting = dynamic_cast<AlternatingWaitingForFirstBarrier*>(next);
    EXPECT_NE(asAlignedWaiting, nullptr);

    delete next;
}
