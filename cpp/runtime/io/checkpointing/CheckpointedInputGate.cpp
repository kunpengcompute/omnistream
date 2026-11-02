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
#include "CheckpointedInputGate.h"

#include <utility>
#include "event/EndOfData.h"
#include "event/EndOfPartitionEvent.h"
#include "event/EndOfChannelStateEvent.h"
#include "io/network/api/EventAnnouncement.h"
#include "iostream"

namespace omnistream {

CheckpointedInputGate::CheckpointedInputGate(
    std::shared_ptr<InputGate> inputGate,
    std::shared_ptr<CheckpointBarrierHandler> barrierHandler,
    std::shared_ptr<MailboxExecutor> mailboxExecutor)
    : inputGate_(std::move(inputGate)),
      barrierHandler_(std::move(barrierHandler)),
      mailboxExecutor_(std::move(mailboxExecutor)),
      upstreamRecoveryTracker_(std::make_shared<UpstreamRecoveryTrackerImpl>(inputGate_)),
      isFinished_(false)
{
}

CheckpointedInputGate::CheckpointedInputGate(
    std::shared_ptr<InputGate> inputGate,
    std::shared_ptr<CheckpointBarrierHandler> barrierHandler,
    std::shared_ptr<MailboxExecutor> mailboxExecutor,
    std::shared_ptr<UpstreamRecoveryTracker> upstreamRecoveryTracker)
    : inputGate_(std::move(inputGate)),
      barrierHandler_(std::move(barrierHandler)),
      mailboxExecutor_(std::move(mailboxExecutor)),
      upstreamRecoveryTracker_(std::move(upstreamRecoveryTracker)),
      isFinished_(false)
{
}

CheckpointedInputGate::~CheckpointedInputGate()
{
    Close();
}

std::shared_ptr<CompletableFuture> CheckpointedInputGate::GetAvailableFuture()
{
    return inputGate_->GetAvailableFuture();
}

BufferOrEvent* CheckpointedInputGate::PollNext()
{
    auto bufferOrEvent = inputGate_->PollNext();
    if (!bufferOrEvent) {
        return HandleEmptyBuffer();
    }

    if (bufferOrEvent->isEvent()) {
        return HandleEvent(bufferOrEvent);
    } else if (bufferOrEvent->isBuffer()) {
        barrierHandler_->AddProcessedBytes(bufferOrEvent->getSize());
        return bufferOrEvent;
    }
    return nullptr;
}

BufferOrEvent* CheckpointedInputGate::HandleEvent(BufferOrEvent* bufferOrEvent)
{
    auto eventClassName = bufferOrEvent->getEvent()->GetEventClassName();
    INFO_RELEASE(
        "Start to handle event, eventClassName: " << eventClassName
                                                  << ", channelInfo: " << bufferOrEvent->getChannelInfo().toString());

    if (bufferOrEvent->getEvent()->GetEventClassName() == "CheckpointBarrier") {
        auto checkpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(bufferOrEvent->getEvent());
        if (!checkpointBarrier) {
            INFO_RELEASE("CheckpointedInputGate::HandleEvent checkpointBarrier is nullptr");
            throw std::runtime_error("Failed to cast event to CheckpointBarrier");
        }
        barrierHandler_->ProcessBarrier(*checkpointBarrier, bufferOrEvent->getChannelInfo(), false);
    } else if (bufferOrEvent->getEvent()->GetEventClassName() == "EventAnnouncement") {
        INFO_RELEASE("CheckpointedInputGate::HandleEvent received an announcement event.");
        auto ann = std::dynamic_pointer_cast<EventAnnouncement>(bufferOrEvent->getEvent());
        if (!ann) {
            INFO_RELEASE("CheckpointedInputGate::HandleEvent ann is nullptr!");
            throw std::runtime_error("Failed to cast event to EventAnnouncement");
        }

        auto announced = ann->GetAnnouncedEvent();
        // announcements are used to announce timeoutable aligned checkpoint barriers.
        if (announced && announced->GetEventClassName() == "CheckpointBarrier") {
            INFO_RELEASE("CheckpointedInputGate::HandleEvent event class name is CheckpointBarrier.");
            auto announcedBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(announced);
            if (!announcedBarrier) {
                INFO_RELEASE("CheckpointedInputGate::HandleEvent announcedBarrier is nullptr!");
                throw std::runtime_error("Failed to cast announced event to CheckpointBarrier");
            }
            barrierHandler_->ProcessBarrierAnnouncement(
                *announcedBarrier, ann->GetSequenceNumber(), bufferOrEvent->getChannelInfo());
        }
    } else if (bufferOrEvent->getEvent()->GetEventClassName() == "CancelCheckpointMarker") {
        barrierHandler_->ProcessCancellationBarrier(
            *std::dynamic_pointer_cast<CancelCheckpointMarker>(bufferOrEvent->getEvent()),
            bufferOrEvent->getChannelInfo());
    } else if (bufferOrEvent->getEvent()->GetEventClassName() == "EndOfPartitionEvent") {
        barrierHandler_->ProcessEndOfPartition(bufferOrEvent->getChannelInfo());
    } else if (bufferOrEvent->getEvent()->GetEventClassName() == "EndOfChannelStateEvent") {
        INFO_RELEASE("CheckpointedInputGate::HandleEvent received an EndOfChannelStateEvent.");
        upstreamRecoveryTracker_->handleEndOfRecovery(bufferOrEvent->getChannelInfo());
    } else if (bufferOrEvent->getEvent()->GetEventClassName() == "InnerRecoverEvent") {
        INFO_RELEASE("CheckpointedInputGate::HandleEvent received an InnerRecoverEvent.");
        upstreamRecoveryTracker_->handleInnerRecover(bufferOrEvent->getChannelInfo());
    }

    return bufferOrEvent;
}

BufferOrEvent* CheckpointedInputGate::HandleEmptyBuffer()
{
    if (inputGate_->IsFinished()) {
        isFinished_ = true;
    }
    return nullptr;
}

std::shared_ptr<CompletableFutureV2<void>> CheckpointedInputGate::GetAllBarriersReceivedFuture(long checkpointId)
{
    return barrierHandler_->GetAllBarriersReceivedFuture(checkpointId);
}

bool CheckpointedInputGate::IsFinished()
{
    return inputGate_->IsFinished();
}

int CheckpointedInputGate::GetNumberOfInputChannels() const
{
    return inputGate_->GetNumberOfInputChannels();
}

InputFormat CheckpointedInputGate::getInputFormat() const
{
    return inputGate_->getInputFormat();
}

std::shared_ptr<InputChannel> CheckpointedInputGate::GetChannel(int channelIndex)
{
    return inputGate_->getChannel(channelIndex);
}

std::vector<InputChannelInfo> CheckpointedInputGate::GetChannelInfos() const
{
    return inputGate_->getChannelInfos();
}

bool CheckpointedInputGate::AllChannelsRecovered() const
{
    return upstreamRecoveryTracker_->allChannelsRecovered();
}

bool CheckpointedInputGate::AllInnerChannelsRecovered() const
{
    return upstreamRecoveryTracker_->allInnerRecovered();
}

long CheckpointedInputGate::GetLatestCheckpointId() const
{
    return barrierHandler_->GetLatestCheckpointId();
}

long CheckpointedInputGate::GetAlignmentDurationNanos() const
{
    return barrierHandler_->GetAlignmentDurationNanos();
}

long CheckpointedInputGate::GetCheckpointStartDelayNanos() const
{
    return barrierHandler_->GetCheckpointStartDelayNanos();
}

void CheckpointedInputGate::Close()
{
    barrierHandler_->Close();
}

bool CheckpointedInputGate::HasReceivedEndOfData()
{
    return inputGate_->HasReceivedEndOfData();
}
} // namespace omnistream
