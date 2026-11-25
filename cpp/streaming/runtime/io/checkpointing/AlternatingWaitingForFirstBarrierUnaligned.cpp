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

#include "AlternatingWaitingForFirstBarrierUnaligned.h"
#include "AlternatingCollectingBarriersUnaligned.h"
#include "AlternatingWaitingForFirstBarrier.h"
#include "partition/consumer/IndexedInputGate.h"
#include "streaming/runtime/io/checkpointing/AlternatingCollectingBarriers.h"

AlternatingWaitingForFirstBarrierUnaligned::AlternatingWaitingForFirstBarrierUnaligned(
    bool alternating, ChannelState state)
    : alternating_(alternating),
      state_(std::move(state))
{
}

BarrierHandlerState* AlternatingWaitingForFirstBarrierUnaligned::BarrierReceived(
    Controller* controller, InputChannelInfo channelInfo, CheckpointBarrier* barrier, bool markChannelBlocked)
{
    if (!barrier->IsCheckpoint()) {
        state_.BlockChannel(channelInfo);
        BarrierHandlerState* oldState = new AlternatingCollectingBarriers(std::move(state_));
        BarrierHandlerState* newState = oldState->BarrierReceived(controller, channelInfo, barrier, markChannelBlocked);
        if (newState != oldState) {
            delete oldState;
        }
        return newState;
    }
    LOG_DEBUG("AlternatingWaitingForFirstBarrierUnaligned::BarrierReceived");
    if (markChannelBlocked && !barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        state_.BlockChannel(channelInfo);
    }
    bool isNeedDel = false;
    CheckpointBarrier* unalignedBarrier = barrier->AsUnaligned(isNeedDel);
    controller->InitInputsCheckpoint(*unalignedBarrier);
    for (auto* input : state_.getInputs()) {
        omnistream::IndexedInputGate* inputGate = dynamic_cast<omnistream::IndexedInputGate*>(input);
        if (inputGate) {
            inputGate->CheckpointStarted(*unalignedBarrier);
        } else {
            input->CheckpointStarted(*unalignedBarrier);
        }
    }
    controller->TriggerGlobalCheckpoint(*unalignedBarrier);

    if (controller->AllBarriersReceived()) {
        for (auto* input : state_.getInputs()) {
            input->CheckpointStopped(unalignedBarrier->GetId());
        }
        if (isNeedDel) {
            INFO_RELEASE("unalignedBarrier need del ");
            delete unalignedBarrier;
        }
        return FinishCheckpoint();
    }
    BarrierHandlerState* newState =
        new AlternatingCollectingBarriersUnaligned(alternating_, std::move(state_), unalignedBarrier->GetId());
    if (isNeedDel) {
        INFO_RELEASE("unalignedBarrier need del ");
        delete unalignedBarrier;
    }
    return newState;
}

BarrierHandlerState* AlternatingWaitingForFirstBarrierUnaligned::AlignedCheckpointTimeout(
    Controller*, CheckpointBarrier*)
{
    return this;
}

BarrierHandlerState* AlternatingWaitingForFirstBarrierUnaligned::FinishCheckpoint()
{
    state_.UnblockAllChannels();
    if (alternating_) {
        return new AlternatingWaitingForFirstBarrier(state_.EmptyState());
    } else {
        return new AlternatingWaitingForFirstBarrierUnaligned(false, state_.EmptyState());
    }
}

void AlternatingWaitingForFirstBarrierUnaligned::AbortCheckpoint()
{
    state_.UnblockAllChannels();
}
