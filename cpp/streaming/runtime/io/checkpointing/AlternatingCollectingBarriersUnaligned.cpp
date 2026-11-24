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

#include "AlternatingCollectingBarriersUnaligned.h"
#include "AlternatingWaitingForFirstBarrier.h"
#include "AlternatingWaitingForFirstBarrierUnaligned.h"
#include "partition/consumer/IndexedInputGate.h"

AlternatingCollectingBarriersUnaligned::AlternatingCollectingBarriersUnaligned(
    bool alternating, ChannelState state, long checkpointId)
    : alternating_(alternating),
      state_(std::move(state)),
      checkpointId_(checkpointId)
{
}

BarrierHandlerState* AlternatingCollectingBarriersUnaligned::BarrierReceived(
    Controller* controller, InputChannelInfo channelInfo, CheckpointBarrier* barrier, bool markChannelBlocked)
{
    LOG(">>>>");
    if (markChannelBlocked && !barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        state_.BlockChannel(channelInfo);
    }
    if (alternating_) {
        state_.BlockChannel(channelInfo);
        state_.TimeOutUnblockAllChannels();
    }
    for (auto* input : state_.getInputs()) {
        omnistream::IndexedInputGate* inputGate = dynamic_cast<omnistream::IndexedInputGate*>(input);
        if (inputGate) {
            inputGate->AddInputData(barrier->GetId(), channelInfo);
        }
    }
    if (controller->AllBarriersReceived()) {
        return FinishCheckpoint();
    }
    return this;
}

BarrierHandlerState* AlternatingCollectingBarriersUnaligned::AlignedCheckpointTimeout(Controller*, CheckpointBarrier*)
{
    return this;
}

BarrierHandlerState* AlternatingCollectingBarriersUnaligned::FinishCheckpoint()
{
    for (auto* input : state_.getInputs()) {
        input->CheckpointStopped(checkpointId_);
    }

    if (alternating_) {
        state_.TimeOutUnblockAllChannels();
        return new AlternatingWaitingForFirstBarrier(state_.EmptyState());
    } else {
        state_.UnblockAllChannels();
        return new AlternatingWaitingForFirstBarrierUnaligned(false, state_.EmptyState());
    }
}

void AlternatingCollectingBarriersUnaligned::AbortCheckpoint()
{
    state_.UnblockAllChannels();
}
