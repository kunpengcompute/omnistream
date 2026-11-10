/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "AbstractAlternatingAlignedBarrierHandlerState.h"
#include "AlternatingWaitingForFirstBarrier.h"

AbstractAlternatingAlignedBarrierHandlerState::AbstractAlternatingAlignedBarrierHandlerState(ChannelState state)
    : state(std::move(state))
{
}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::BarrierReceived(
    Controller* controller, InputChannelInfo channelInfo, CheckpointBarrier* barrier, bool markChannelBlocked)
{
    LOG_DEBUG("AbstractAlternatingAlignedBarrierHandlerState::BarrierReceived");
    if (barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        std::unique_ptr<BarrierHandlerState> oldState(AlignedCheckpointTimeout(controller, barrier));
        auto* newState = oldState->BarrierReceived(controller, channelInfo, barrier, markChannelBlocked);
        if (oldState.get() == newState) {
            oldState.release();
        }
        return newState;
    }

    state.removeSeenAnnouncement(channelInfo);

    if (markChannelBlocked) {
        state.BlockChannel(channelInfo);
    }

    if (controller->AllBarriersReceived()) {
        controller->InitInputsCheckpoint(*barrier);
        controller->TriggerGlobalCheckpoint(*barrier);
        if (barrier->IsCheckpoint()) {
            return FinishCheckpoint();
        }
        return FinishSavepoint(barrier->GetId());
    }

    return TransitionAfterBarrierReceived(std::move(state));
}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::AnnouncementReceived(
    Controller* /*controller*/, InputChannelInfo channelInfo, int sequenceNumber)
{
    // Only record the announcement; do NOT prioritize it here.
    // Prioritization happens when the aligned checkpoint times out.
    LOG("start AnnouncementReceived, ");
    state.addSeenAnnouncement(channelInfo, sequenceNumber);
    return this;
}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::FinishCheckpoint()
{
    state.UnblockAllChannels();
    return new AlternatingWaitingForFirstBarrier(state.EmptyState());
}

void AbstractAlternatingAlignedBarrierHandlerState::AbortCheckpoint()
{
    state.UnblockAllChannels();
}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::FinishSavepoint(long id)
{
    state.UnblockAllChannels();
    for (auto* input : state.getInputs()) {
        input->notifyDataAvailable();
        input->CheckpointStopped(id);
    }
    return new AlternatingWaitingForFirstBarrier(state.EmptyState());
}
