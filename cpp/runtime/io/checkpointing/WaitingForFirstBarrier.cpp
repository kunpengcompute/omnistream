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

#include "WaitingForFirstBarrier.h"

#include <stdexcept>
#include "streaming/runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
namespace {

/**
 * Aligned-only collecting state. Similar to Flink's "CollectingBarriers" in
 * aligned checkpoint mode.
 */
class CollectingBarriers : public BarrierHandlerState {
public:
    explicit CollectingBarriers(ChannelState state) : state_(std::move(state))
    {
    }

    BarrierHandlerState* BarrierReceived(
        Controller* controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier* barrier,
        bool markChannelBlocked) override
    {
        // In the aligned-only handler we should never receive an explictly unaligned barrier.
        if (barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
            throw std::runtime_error("Aligned-only barrier handler received an unaligned checkpoint barrier.");
        }

        state_.removeSeenAnnouncement(channelInfo);
        if (markChannelBlocked) {
            state_.BlockChannel(channelInfo);
        }

        if (controller->AllBarriersReceived()) {
            controller->InitInputsCheckpoint(*barrier);
            controller->TriggerGlobalCheckpoint(*barrier);
            return FinishCheckpoint();
        }

        // Stay in collecting state.
        return this;
    }

    BarrierHandlerState* AlignedCheckpointTimeout(Controller* controller, CheckpointBarrier* barrier) override
    {
        // Aligned-only mode:do not switch to unaligned.
        return this;
    }

    BarrierHandlerState* FinishCheckpoint() override
    {
        state_.UnblockAllChannels();

        // Reset internal bookkeeping (sanity-checks that nothing is blocked).
        ChannelState next = std::move(state_);
        next.EmptyState();
        return new WaitingForFirstBarrier(std::move(next));
    }

    void AbortCheckpoint() override
    {
        state_.UnblockAllChannels();
    }

private:
    ChannelState state_;
};

} // namespace

WaitingForFirstBarrier::WaitingForFirstBarrier(ChannelState state) : state_(std::move(state))
{
}

BarrierHandlerState* WaitingForFirstBarrier::BarrierReceived(
    Controller* controller, InputChannelInfo channelInfo, CheckpointBarrier* barrier, bool markChannelBlocked)
{
    // In the aligned-only handler we should never receive an explicitly unaligned barrier.
    if (barrier->IsCheckpoint() && barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        throw std::runtime_error("Aligned-only barrier handler received an unaligned checkpoint barrier.");
    }

    state_.removeSeenAnnouncement(channelInfo);

    if (markChannelBlocked) {
        state_.BlockChannel(channelInfo);
    }

    if (controller->AllBarriersReceived()) {
        controller->InitInputsCheckpoint(*barrier);
        controller->TriggerGlobalCheckpoint(*barrier);
        if (!barrier->IsCheckpoint() && barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
            return FinishSavepoint(barrier->GetId());
        }
        return FinishCheckpoint();
    }

    return new CollectingBarriers(std::move(state_));
}

BarrierHandlerState* WaitingForFirstBarrier::AlignedCheckpointTimeout(
    Controller* /*controller*/, CheckpointBarrier* /*barrier*/)
{
    // Aligned-only mode: do not switch to unaligned.
    return this;
}

BarrierHandlerState* WaitingForFirstBarrier::FinishCheckpoint()
{
    state_.UnblockAllChannels();
    for (auto* input : state_.getInputs()) {
        input->notifyDataAvailable();
    }
    ChannelState next = std::move(state_);
    next.EmptyState();
    return new WaitingForFirstBarrier(std::move(next));
}

void WaitingForFirstBarrier::AbortCheckpoint()
{
    state_.UnblockAllChannels();
}

BarrierHandlerState* WaitingForFirstBarrier::FinishSavepoint(long id)
{
    state_.UnblockAllChannels();
    for (auto* input : state_.getInputs()) {
        input->notifyDataAvailable();
        input->CheckpointStopped(id);
    }
    ChannelState next = std::move(state_);
    next.EmptyState();
    return new AlternatingWaitingForFirstBarrierUnaligned(false, next);
}
