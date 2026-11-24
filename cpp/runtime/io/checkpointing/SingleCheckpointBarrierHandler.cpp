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

#include "SingleCheckpointBarrierHandler.h"
#include <iostream>
#include <algorithm>
#include <stdexcept>
#include "common.h"
#include "io/network/partition/consumer/CheckpointableInput.h"
#include "runtime/jobgraph/tasks/CheckpointableTask.h"
#include "runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
#include "WaitingForFirstBarrier.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrier.h"

namespace omnistream::runtime {

SingleCheckpointBarrierHandler::SingleCheckpointBarrierHandler(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    SubtaskCheckpointCoordinator* subTaskCheckpointCoordinator,
    Clock& clock,
    int numOpenChannels,
    BarrierHandlerState* currentState,
    bool alternating,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    const std::vector<CheckpointableInput*>& inputs,
    bool enableCheckpointAfterTasksFinished)
    : CheckpointBarrierHandler(toNotifyOnCheckpoint, clock, enableCheckpointAfterTasksFinished),
      taskName_(taskName),
      registerTimer_(registerTimer),
      subTaskCheckpointCoordinator_(subTaskCheckpointCoordinator),
      inputs_(inputs),
      currentCheckpointId_(-1L),
      targetChannelCount_(0),
      lastCancelledOrCompletedCheckpointId_(-1L),
      numOpenChannels_(numOpenChannels),
      currentState_(currentState),
      alternating_(alternating)
{
    // allBarriersReceivedFuture_ = CompletableFutureV2<void>();
    LOG_DEBUG("SingleCheckpointBarrierHandler init");
    context_ = new ControllerImpl(this, subTaskCheckpointCoordinator_);
}

SingleCheckpointBarrierHandler::~SingleCheckpointBarrierHandler()
{
    delete currentState_;
    delete context_;
}

// Main ProcessBarrier implementation
void SingleCheckpointBarrierHandler::ProcessBarrier(
    const CheckpointBarrier& barrier, const InputChannelInfo& channelInfo, bool isRpcTriggered)
{
    int64_t barrierId = barrier.GetId();
    LOG(taskName_ + ": Received barrier from channel " + channelInfo.toString() + " @" + std::to_string(barrierId));

    // ignore barriers for checkpoints that are already cancelled/completed.
    // handle cancellation markers that arrive before any barrier of that checkpoint.
    if (barrierId <= lastCancelledOrCompletedCheckpointId_) {
        if (!barrier.GetCheckpointOptions()->IsUnalignedCheckpoint()) {
            inputs_[channelInfo.getGateIdx()]->ResumeConsumption(channelInfo);
        }
        LOG("Func:ProcessBarrier returns, because barrierId <= lastCancelledOrCompletedCheckpointId_");
        return;
    }

    if (currentCheckpointId_ > barrierId || (currentCheckpointId_ == barrierId && !IsCheckpointPending())) {
        if (!barrier.GetCheckpointOptions()->IsUnalignedCheckpoint()) {
            inputs_[channelInfo.getGateIdx()]->ResumeConsumption(channelInfo);
        }
        LOG("Func:ProcessBarrier returns, because urrentCheckpointId_ > barrierId ||(currentCheckpointId_ == barrierId "
            "&& !IsCheckpointPending())");
        return;
    }

    CheckNewCheckpoint(barrier);

    // Verify state consistency
    if (currentCheckpointId_ != barrierId) {
        throw std::runtime_error("Current checkpoint ID mismatch");
    }

    // guard against duplicate barriers from the same channel (can happen after resends or
    // due to priority-event reordering). The handler tracks aligned channels for the current
    // checkpoint; if we've already seen this channel, ignore the duplicate.
    if (alignedChannels_.find(channelInfo) != alignedChannels_.end()) {
        LOG("Func:ProcessBarrier returns, because alignedChannels_.find(channelInfo) != alignedChannels_.end()");
        return;
    }

    // After switching to UC, we must stop blocking channels on late barriers.
    const bool markChannelBlocked =
        (!isRpcTriggered) && !currentCheckpointUnaligned_ && !barrier.GetCheckpointOptions()->IsUnalignedCheckpoint();

    CheckpointBarrier* barrierPtr = const_cast<CheckpointBarrier*>(&barrier);

    // Transform state with barrier received
    auto stateTransformer = [this, &channelInfo, barrierPtr, markChannelBlocked](
                                BarrierHandlerState* state) -> BarrierHandlerState* {
        LOG(">>>>> stateTransformer");
        return state->BarrierReceived(context_, channelInfo, barrierPtr, markChannelBlocked);
    };

    MarkCheckpointAlignedAndTransformState(channelInfo, *barrierPtr, stateTransformer);
}

// Mark checkpoint aligned and transform state
void SingleCheckpointBarrierHandler::MarkCheckpointAlignedAndTransformState(
    const InputChannelInfo& alignedChannel,
    const CheckpointBarrier& barrier,
    const std::function<BarrierHandlerState*(BarrierHandlerState*)>& stateTransformer)
{
    LOG_DEBUG("Func:MarkCheckpointAlignedAndTransformState start");
    alignedChannels_.insert(alignedChannel);
    const bool isAlwaysUnalignedHandler = (!alternating_ && subTaskCheckpointCoordinator_ != nullptr);
    const bool shouldTrackAlignment = !isAlwaysUnalignedHandler && !currentCheckpointUnaligned_ &&
                                      !barrier.GetCheckpointOptions()->IsUnalignedCheckpoint();

    // (Flink 1.16.3 semantics): when alignment completes on the last barrier, make sure the
    // timeout callback cannot concurrently switch this checkpoint to UC and trigger a second time.
    // We do this before executing the state transition (which may trigger the checkpoint), by
    // cancelling the alignment timer and completing the "all barriers received" future.
    const bool isLastBarrierForAlignment =
        shouldTrackAlignment && (alignedChannels_.size() == static_cast<size_t>(targetChannelCount_));
    if (alternating_ && isLastBarrierForAlignment) {
        ResetAlignmentTimer();
        if (!allBarriersReceivedFuture_V2->IsDone()) {
            allBarriersReceivedFuture_V2->Complete();
        }
    }

    if (alignedChannels_.size() == 1 && alternating_) {
        if (targetChannelCount_ == 1) {
            MarkAlignmentStartAndEnd(barrier.GetId(), barrier.GetTimestamp());
        } else {
            MarkAlignmentStart(barrier.GetId(), barrier.GetTimestamp());
        }
    }

    // We must mark alignment end before calling currentState.barrierReceived which might
    // trigger a checkpoint with unfinished future for alignment duration
    if (alternating_ && alignedChannels_.size() == (unsigned int)targetChannelCount_) {
        if (targetChannelCount_ > 1) {
            MarkAlignmentEnd();
        }
        if (!allBarriersReceivedFuture_V2->IsDone()) {
            if (barrier.IsCheckpoint()) {
                ResetAlignmentTimer();
            }
            allBarriersReceivedFuture_V2->Complete();
        }
    }
    try {
        auto* old = currentState_;
        auto* next = stateTransformer(old);
        if (next != old) {
            delete old;
        }
        currentState_ = next;
    } catch (const CheckpointException& e) {
        AbortInternal(currentCheckpointId_, e);
        return;
    } catch (const std::exception& e) {
        CheckpointException checkpointException(CheckpointFailureReason::CHECKPOINT_DECLINED, e);
        AbortInternal(currentCheckpointId_, checkpointException);
        throw std::runtime_error("Error in state transformation: " + std::string(e.what()));
    } catch (...) {
        CheckpointException checkpointException(CheckpointFailureReason::CHECKPOINT_DECLINED);
        AbortInternal(currentCheckpointId_, checkpointException);
        throw std::runtime_error("Error in state transformation: unknown exception");
    }

    if (alternating_ && barrier.IsCheckpoint() && shouldTrackAlignment && alignedChannels_.size() == 1 &&
        targetChannelCount_ > 1 && barrier.GetCheckpointOptions()->IsTimeoutable()) {
        RegisterAlignmentTimer(barrier);
    }

    if (alignedChannels_.size() == (unsigned int)targetChannelCount_) {
        alignedChannels_.clear();
        lastCancelledOrCompletedCheckpointId_ = currentCheckpointId_;
        LOG_DEBUG(taskName_ + ": All the channels are aligned for checkpoint " + std::to_string(currentCheckpointId_));
        if (barrier.IsCheckpoint()) {
            ResetAlignmentTimer();
        }
        if (!allBarriersReceivedFuture_V2->IsDone()) {
            allBarriersReceivedFuture_V2->Complete();
        }
    }
}

// Trigger checkpoint - calls NotifyCheckpoint
void SingleCheckpointBarrierHandler::TriggerCheckpoint(const CheckpointBarrier& trigger)
{
    LOG(taskName_ + " : Triggering checkpoint " + std::to_string(trigger.GetId()) + " on the barrier announcement at " +
        std::to_string(trigger.GetTimestamp()));

    NotifyCheckpoint(trigger);
}

// Process barrier announcement
void SingleCheckpointBarrierHandler::ProcessBarrierAnnouncement(
    const CheckpointBarrier& announcedBarrier, int sequenceNumber, const InputChannelInfo& channelInfo)
{
    LOG_DEBUG("start ProcessBarrierAnnouncement, barrier Id: " << announcedBarrier.GetId());
    // Ignore announcements for checkpoints that are already cancelled/completed.
    if (announcedBarrier.GetId() <= lastCancelledOrCompletedCheckpointId_) {
        LOG_DEBUG(
            "announcedBarrier.GetId() <= lastCancelledOrCompletedCheckpointId_. barrier Id: "
            << announcedBarrier.GetId());
        return;
    }

    // Announcements are only meaningful for timeoutable aligned checkpoints.
    if (announcedBarrier.GetCheckpointOptions() == nullptr ||
        announcedBarrier.GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        LOG_DEBUG(
            "announcedBarrier.GetCheckpointOptions() == nullptr || "
            "announcedBarrier.GetCheckpointOptions()->IsUnalignedCheckpoint(), barrier Id: "
            << announcedBarrier.GetId());
        return;
    }

    // An announcement can arrive before the first barrier. Start tracking the checkpoint now so
    // that we don't lose the announced sequence number when the barrier arrives later.
    CheckNewCheckpoint(announcedBarrier);

    int64_t barrierId = announcedBarrier.GetId();
    if (currentCheckpointId_ > barrierId || (currentCheckpointId_ == barrierId && !IsCheckpointPending())) {
        LOG_DEBUG(
            "scurrentCheckpointId_ > barrierId || (currentCheckpointId_ == barrierId && !IsCheckpointPending()), "
            "barrier Id: "
            << announcedBarrier.GetId());
        std::cout << taskName_ << ": Obsolete announcement of checkpoint " << barrierId << " for channel "
                  << channelInfo.toString() << std::endl;
        return;
    }

    // Record the announcement in the current handler state.
    // NOTE: Do NOT prioritize it here. Prioritization happens when aligned checkpoint times out.
    try {
        auto* oldState = currentState_;
        auto* nextState =
            currentState_->AnnouncementReceived(dynamic_cast<Controller*>(context_), channelInfo, sequenceNumber);
        if (nextState != oldState) {
            delete oldState;
        }
        currentState_ = nextState;
    } catch (...) {
        // best effort; announcement should not fail the task
    }
}

// Process cancellation barrier
void SingleCheckpointBarrierHandler::ProcessCancellationBarrier(
    const CancelCheckpointMarker& cancelBarrier, const InputChannelInfo& channelInfo)
{
    int64_t cancelledId = cancelBarrier.getCheckpointId();

    // Ignore stale cancellations.
    if (cancelledId <= lastCancelledOrCompletedCheckpointId_) {
        return;
    }

    std::cout << taskName_ << ": Received cancellation " << cancelledId << std::endl;
    AbortInternal(cancelledId, CheckpointFailureReason::CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
}

// Process end of partition
void SingleCheckpointBarrierHandler::ProcessEndOfPartition(const InputChannelInfo& channelInfo)
{
    numOpenChannels_--;
}

// Check new checkpoint
void SingleCheckpointBarrierHandler::CheckNewCheckpoint(const CheckpointBarrier& barrier)
{
    LOG_DEBUG(">>>>>" << " id " << barrier.GetId() << " timestamp " << barrier.GetTimestamp());
    int64_t barrierId = barrier.GetId();
    if (currentCheckpointId_ >= barrierId) {
        return; // This barrier is not the first for this checkpoint
    }

    if (IsCheckpointPending()) {
        CancelSubsumedCheckpoint(barrierId);
    }

    currentCheckpointId_ = barrierId;
    pendingCheckpointBarrier_ = const_cast<CheckpointBarrier*>(&barrier);
    alignedChannels_.clear();
    targetChannelCount_ = numOpenChannels_;
    currentCheckpointUnaligned_ = barrier.GetCheckpointOptions()->IsUnalignedCheckpoint();
    LOG("CheckNewCheckpoint cp=" << barrierId << " create future");
    // allBarriersReceivedFuture_ = CompletableFutureV2<void>();
    allBarriersReceivedFuture_V2 = std::make_shared<CompletableFutureV2<void>>();
}

// Register alignment timer
void SingleCheckpointBarrierHandler::RegisterAlignmentTimer(const CheckpointBarrier& announcedBarrier)
{
    LOG("Func:RegisterAlignmentTimer start.");
    if (!announcedBarrier.GetCheckpointOptions()->IsTimeoutable()) {
        return;
    }

    ResetAlignmentTimer();

    int64_t timerDelay = announcedBarrier.GetCheckpointOptions()->GetAlignedCheckpointTimeout();
    const int64_t barrierId = announcedBarrier.GetId();
    currentAlignmentTimerCheckpointId_ = barrierId;

    auto timerTask = [this, announcedBarrier, barrierId]() mutable {
        try {
            // Stale callback protection
            if (currentAlignmentTimerCheckpointId_ != barrierId) {
                return;
            }
            if (currentCheckpointId_ != barrierId || GetAllBarriersReceivedFuture(barrierId)->IsDone()) {
                return;
            }
            if (currentCheckpointUnaligned_) {
                return;
            }

            // Re-check completion right before switching: the last barrier path completes the future
            // early to avoid races, but this callback might have started slightly earlier.
            if (GetAllBarriersReceivedFuture(barrierId)->IsDone()) {
                return;
            }

            LOG("Timeout, start transition.");
            this->currentCheckpointUnaligned_ = true;
            auto* old = this->currentState_;
            auto* next = old->AlignedCheckpointTimeout(
                dynamic_cast<Controller*>(this->context_), const_cast<CheckpointBarrier*>(&announcedBarrier));
            if (next != old) {
                delete old;
            }
            this->currentState_ = next;
        } catch (const CheckpointException& ex) {
            AbortInternal(barrierId, ex);
        } catch (const std::exception& e) {
            throw std::runtime_error("Timer task error: " + std::string(e.what()));
        }

        // Clear timer only if it still belongs to this checkpoint.
        if (currentAlignmentTimerCheckpointId_ == barrierId) {
            currentAlignmentTimer_.reset();
            currentAlignmentTimerCheckpointId_ = -1;
        }
    };

    currentAlignmentTimer_.reset(registerTimer_->RegisterTask(timerTask, std::chrono::milliseconds(timerDelay)));
}

// Abort internal with reason
void SingleCheckpointBarrierHandler::AbortInternal(int64_t cancelledId, CheckpointFailureReason reason)
{
    CheckpointException exception(reason);
    AbortInternal(cancelledId, exception);
}

// Abort internal with exception
void SingleCheckpointBarrierHandler::AbortInternal(int64_t cancelledId, const CheckpointException& exception)
{
    // Ignore stale aborts for already completed newer checkpoints.
    if (cancelledId <= lastCancelledOrCompletedCheckpointId_) {
        return;
    }

    LOG(taskName_ + ": Aborting checkpoint " + std::to_string(cancelledId) + " due to " +
        omnistream::toString(exception.GetCheckpointFailureReason()));

    // Cancel any alignment timer that might still be active.
    ResetAlignmentTimer();

    // The state machine owns the authoritative set of channels blocked by this checkpoint.
    // Resume that set before deleting the old state; scanning every input can send a duplicate
    // resume to an unblocked producer when an unaligned checkpoint is subsumed by a savepoint.
    if (currentState_ != nullptr) {
        currentState_->AbortCheckpoint();
    }

    for (auto* input : inputs_) {
        input->CheckpointStopped(cancelledId);
    }

    alignedChannels_.clear();
    pendingCheckpointBarrier_ = nullptr;
    currentCheckpointUnaligned_ = false;
    lastCancelledOrCompletedCheckpointId_ = cancelledId;

    // Reset the handler state machine so the next checkpoint starts from a clean slate.
    ResetToWaitingState();

    // Notify the task/coordinator about the aborted checkpoint (Flink 1.16.x notifyAbort path).
    // This will ultimately decline the checkpoint and broadcast a CancelCheckpointMarker.
    try {
        NotifyAbort(cancelledId, exception);
    } catch (...) {
        // Best-effort: abort cleanup must not fail due to notification issues.
    }

    if (allBarriersReceivedFuture_V2 && !allBarriersReceivedFuture_V2->IsDone()) {
        //            allBarriersReceivedFuture_V2->CompleteExceptionally(std::make_exception_ptr(exception));
        allBarriersReceivedFuture_V2->Cancel();
    }
}

void SingleCheckpointBarrierHandler::ResetToWaitingState()
{
    // Drop the old state instance to avoid carrying over blocked-channel bookkeeping.
    if (currentState_ != nullptr) {
        delete currentState_;
        currentState_ = nullptr;
    }

    ChannelState channelState(inputs_);
    // Recreate the initial state based on handler mode.
    if (subTaskCheckpointCoordinator_ == nullptr) {
        // Pure aligned handler.
        currentState_ = new WaitingForFirstBarrier(std::move(channelState));
        return;
    }

    if (alternating_) {
        currentState_ = new AlternatingWaitingForFirstBarrier(std::move(channelState));
    } else {
        // Always-unaligned handler.
        currentState_ = new AlternatingWaitingForFirstBarrierUnaligned(false, std::move(channelState));
    }
}

// Cancel subsumed checkpoint
void SingleCheckpointBarrierHandler::CancelSubsumedCheckpoint(int64_t barrierId)
{
    std::cout << taskName_ << ": Received checkpoint barrier for checkpoint " << barrierId
              << " before completing current checkpoint " << currentCheckpointId_ << ". Skipping current checkpoint."
              << std::endl;

    LOG(taskName_ + ": Received checkpoint barrier for checkpoint " + std::to_string(barrierId) +
        " before completing current checkpoint " + std::to_string(currentCheckpointId_) +
        ". Skipping current checkpoint.");

    AbortInternal(currentCheckpointId_, CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED);
}

// Reset alignment timer
void SingleCheckpointBarrierHandler::ResetAlignmentTimer()
{
    LOG("ResetAlignmentTimer");
    if (currentAlignmentTimer_) {
        currentAlignmentTimer_->Cancel();
        currentAlignmentTimer_.reset();
    }

    currentAlignmentTimerCheckpointId_ = -1;
}

/*
// Reset all barriers received future
void SingleCheckpointBarrierHandler::ResetAllBarriersReceivedFuture()
{
    allBarriersReceivedPromise_ = std::promise<void>();
    allBarriersReceivedFuture_ = allBarriersReceivedPromise_.get_future();
}
*/

// Get latest checkpoint ID
int64_t SingleCheckpointBarrierHandler::GetLatestCheckpointId() const
{
    return currentCheckpointId_;
}

BarrierHandlerState* SingleCheckpointBarrierHandler::GetCurrentState() const
{
    return currentState_;
}

// Check if checkpoint is pending
bool SingleCheckpointBarrierHandler::IsCheckpointPending() const
{
    return currentCheckpointId_ != lastCancelledOrCompletedCheckpointId_ && currentCheckpointId_ >= 0;
}

// Get all barriers received future
std::shared_ptr<CompletableFutureV2<void>> SingleCheckpointBarrierHandler::GetAllBarriersReceivedFuture(
    int64_t checkpointId)
{
    if (checkpointId < currentCheckpointId_ || numOpenChannels_ == 0) {
        if (!completed_V2->IsDone()) {
            completed_V2->Complete();
        }

        return completed_V2;
    }

    if (checkpointId > currentCheckpointId_) {
        throw std::runtime_error("Checkpoint " + std::to_string(checkpointId) + " has not been started at all");
    }
    LOG_DEBUG(
        "SingleCheckpointBarrierHandler GetAllBarriersReceivedFuture checkpointId: "
        << checkpointId << ", currentCheckpointId: " << currentCheckpointId_);
    return allBarriersReceivedFuture_V2;
}

// Get number of open channels
int SingleCheckpointBarrierHandler::GetNumOpenChannels() const
{
    return numOpenChannels_;
}

// Close handler
void SingleCheckpointBarrierHandler::Close()
{
    ResetAlignmentTimer();
    if (allBarriersReceivedFuture_V2) {
        allBarriersReceivedFuture_V2->Cancel();
    }
    CheckpointBarrierHandler::Close();
}

// ToString method
std::string SingleCheckpointBarrierHandler::ToString() const
{
    return taskName_ + ": current checkpoint: " + std::to_string(currentCheckpointId_) +
           ", current aligned channels: " + std::to_string(alignedChannels_.size()) +
           ", target channel count: " + std::to_string(targetChannelCount_);
}

// Factory methods - simplified implementations
std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::createUnalignCheckpointBarrierHandler(
    SubtaskCheckpointCoordinator* checkpointCoordinator,
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    Clock& clock,
    bool enableCheckpointsAfterTasksFinish,
    const std::vector<CheckpointableInput*>& inputs)
{
    // Calculate total channel count
    int channelCount = 0;
    for (const auto& input : inputs) {
        channelCount += input->GetChannelInfos().size();
    }

    // Create a simple timer that throws for unaligned checkpoints
    auto timer = std::make_unique<BarrierAlignmentUtil::ThrowingDelayableTimer<std::function<void()>>>();
    return unaligned(
        taskName,
        toNotifyOnCheckpoint,
        checkpointCoordinator,
        clock,
        channelCount,
        timer.get(),
        enableCheckpointsAfterTasksFinish,
        inputs);
}

std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::unaligned(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    SubtaskCheckpointCoordinator* checkpointCoordinator,
    Clock& clock,
    int numOpenChannels,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    bool enableCheckpointAfterTasksFinished,
    const std::vector<CheckpointableInput*>& inputs)
{
    ChannelState channelState(inputs);
    auto state = new AlternatingWaitingForFirstBarrierUnaligned(false, std::move(channelState));

    return std::make_unique<SingleCheckpointBarrierHandler>(
        taskName,
        toNotifyOnCheckpoint,
        checkpointCoordinator,
        clock,
        numOpenChannels,
        state,
        false,
        registerTimer,
        inputs,
        enableCheckpointAfterTasksFinished);
}

std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::aligned(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    Clock& clock,
    int numOpenChannels,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    bool enableCheckpointAfterTasksFinished,
    const std::vector<CheckpointableInput*>& inputs)
{
    auto channelState = ChannelState(inputs);
    auto state = new WaitingForFirstBarrier(std::move(channelState));

    // fix state waitingforfirstbarrier is implemented
    return std::make_unique<SingleCheckpointBarrierHandler>(
        taskName,
        toNotifyOnCheckpoint,
        nullptr,
        clock,
        numOpenChannels,
        state,
        false,
        registerTimer,
        inputs,
        enableCheckpointAfterTasksFinished);
}

std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::alternating(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    SubtaskCheckpointCoordinator* checkpointCoordinator,
    Clock& clock,
    int numOpenChannels,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    bool enableCheckpointAfterTasksFinished,
    const std::vector<CheckpointableInput*>& inputs)
{
    ChannelState channelState(inputs);
    auto state = new AlternatingWaitingForFirstBarrier(std::move(channelState));

    return std::make_unique<SingleCheckpointBarrierHandler>(
        taskName,
        toNotifyOnCheckpoint,
        checkpointCoordinator,
        clock,
        numOpenChannels,
        state,
        true,
        registerTimer,
        inputs,
        enableCheckpointAfterTasksFinished);
}
} // namespace omnistream::runtime
