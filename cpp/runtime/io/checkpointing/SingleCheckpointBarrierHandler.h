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

#ifndef OMNISTREAM_SINGLECHECKPOINTBARRIERHANDLER_H
#define OMNISTREAM_SINGLECHECKPOINTBARRIERHANDLER_H

#pragma once

#include <string>
#include <vector>
#include <unordered_set>
#include <memory>
#include <future>
#include <functional>
#include <chrono>
#include <optional>

#include "CheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/BarrierHandlerState.h"
#include "io/network/partition/consumer/CheckpointableInput.h"
#include "runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinator.h"

namespace omnistream::runtime {

/**
 * SingleCheckpointBarrierHandler is used for triggering checkpoint while reading the first
 * barrier and keeping track of the number of received barriers and consumed barriers.
 */
class SingleCheckpointBarrierHandler : public CheckpointBarrierHandler {
public:
    // Factory methods
    static std::unique_ptr<SingleCheckpointBarrierHandler> createUnalignCheckpointBarrierHandler(
        SubtaskCheckpointCoordinator* checkpointCoordinator,
        const std::string& taskName,
        CheckpointableTask* toNotifyOnCheckpoint,
        Clock& clock,
        bool enableCheckpointsAfterTasksFinish,
        const std::vector<CheckpointableInput*>& inputs);

    static std::unique_ptr<SingleCheckpointBarrierHandler> unaligned(
        const std::string& taskName,
        CheckpointableTask* toNotifyOnCheckpoint,
        SubtaskCheckpointCoordinator* checkpointCoordinator,
        Clock& clock,
        int numOpenChannels,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
        bool enableCheckpointAfterTasksFinished,
        const std::vector<CheckpointableInput*>& inputs);

    static std::unique_ptr<SingleCheckpointBarrierHandler> aligned(
        const std::string& taskName,
        CheckpointableTask* toNotifyOnCheckpoint,
        Clock& clock,
        int numOpenChannels,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
        bool enableCheckpointAfterTasksFinished,
        const std::vector<CheckpointableInput*>& inputs);

    static std::unique_ptr<SingleCheckpointBarrierHandler> alternating(
        const std::string& taskName,
        CheckpointableTask* toNotifyOnCheckpoint,
        SubtaskCheckpointCoordinator* checkpointCoordinator,
        Clock& clock,
        int numOpenChannels,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
        bool enableCheckpointAfterTasksFinished,
        const std::vector<CheckpointableInput*>& inputs);

    // Constructor
    SingleCheckpointBarrierHandler(
        const std::string& taskName,
        CheckpointableTask* toNotifyOnCheckpoint,
        SubtaskCheckpointCoordinator* subTaskCheckpointCoordinator,
        Clock& clock,
        int numOpenChannels,
        BarrierHandlerState* currentState,
        bool alternating,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
        const std::vector<CheckpointableInput*>& inputs,
        bool enableCheckpointAfterTasksFinished);

    // Destructor
    ~SingleCheckpointBarrierHandler() override;

    // Override virtual methods from base class
    void ProcessBarrier(
        const CheckpointBarrier& barrier, const InputChannelInfo& channelInfo, bool isRpcTriggered) override;

    void ProcessBarrierAnnouncement(
        const CheckpointBarrier& announcedBarrier, int sequenceNumber, const InputChannelInfo& channelInfo) override;

    void ProcessCancellationBarrier(
        const CancelCheckpointMarker& cancelBarrier, const InputChannelInfo& channelInfo) override;

    void ProcessEndOfPartition(const InputChannelInfo& channelInfo) override;

    int64_t GetLatestCheckpointId() const override;
    BarrierHandlerState* GetCurrentState() const;

    void Close() override;
    bool IsCheckpointPending() const override;
    // Additional public methods
    // Be very careful when using this function. It returns a refernce because future can't be copied
    std::shared_ptr<CompletableFutureV2<void>> GetAllBarriersReceivedFuture(int64_t checkpointId) override;
    int GetNumOpenChannels() const;
    std::string ToString() const;
    Controller* GetContext() const
    {
        return context_;
    }

    BarrierHandlerState* GetCurrentState()
    {
        return currentState_;
    }

protected:
    void TriggerCheckpoint(const CheckpointBarrier& trigger);
    void RegisterAlignmentTimer(const CheckpointBarrier& announcedBarrier);
    void CheckNewCheckpoint(const CheckpointBarrier& barrier);
    void AbortInternal(int64_t cancelledId, CheckpointFailureReason reason);
    void AbortInternal(int64_t cancelledId, const CheckpointException& exception);
    void ResetAlignmentTimer();
    // Reset the barrier-handling state machine to the initial "waiting" state.
    // This is required for correctness after abort/subsume to avoid leaking the previous
    // checkpoint's state into the next one.
    void ResetToWaitingState();
    void CancelSubsumedCheckpoint(int64_t barrierId);
    void MarkCheckpointAlignedAndTransformState(
        const InputChannelInfo& alignedChannel,
        const CheckpointBarrier& barrier,
        const std::function<BarrierHandlerState*(BarrierHandlerState*)>& stateTransformer);

private:
    class ControllerImpl : public Controller {
    public:
        ControllerImpl(
            SingleCheckpointBarrierHandler* parent, SubtaskCheckpointCoordinator* subtaskCheckpointCoordinator)
            : parent_(parent),
              subTaskCheckpointCoordinator_(subtaskCheckpointCoordinator)
        {
        }

        void TriggerGlobalCheckpoint(const CheckpointBarrier& checkpointBarrier) override
        {
            LOG_DEBUG(parent_->taskName_ << "TriggerGlobalCheckpoint checkpointId=" << checkpointBarrier.GetId());
            parent_->TriggerCheckpoint(checkpointBarrier);
        }

        bool IsTimedOut(const CheckpointBarrier& barrier) override
        {
            if (!barrier.GetCheckpointOptions()->IsTimeoutable()) {
                return false;
            }

            return barrier.GetId() <= parent_->currentCheckpointId_ &&
                   barrier.GetCheckpointOptions()->GetAlignedCheckpointTimeout() <
                       (parent_->clock.RelativeTimeMillis() - barrier.GetTimestamp());
        }

        bool AllBarriersReceived() const override
        {
            return parent_->alignedChannels_.size() == static_cast<size_t>(parent_->targetChannelCount_);
        }

        const CheckpointBarrier* GetPendingCheckpointBarrier() const override
        {
            return parent_->pendingCheckpointBarrier_;
        }

        void InitInputsCheckpoint(const CheckpointBarrier& checkpointBarrier) override
        {
            if (UNLIKELY(subTaskCheckpointCoordinator_ == nullptr)) {
                return;
            }
            long barrierId = checkpointBarrier.GetId();
            subTaskCheckpointCoordinator_->InitInputsCheckpoint(
                barrierId, checkpointBarrier.GetCheckpointOptions().get());
        }

    private:
        SingleCheckpointBarrierHandler* parent_ = nullptr;
        SubtaskCheckpointCoordinator* subTaskCheckpointCoordinator_ = nullptr;
    };

    // Member variables
    std::string taskName_;
    Controller* context_ = nullptr;
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer_ = nullptr;
    SubtaskCheckpointCoordinator* subTaskCheckpointCoordinator_ = nullptr;
    std::vector<CheckpointableInput*> inputs_;

    // Checkpoint tracking
    int64_t currentCheckpointId_;
    CheckpointBarrier* pendingCheckpointBarrier_ = nullptr;
    std::unordered_set<InputChannelInfo> alignedChannels_;
    int targetChannelCount_;
    int64_t lastCancelledOrCompletedCheckpointId_;
    int numOpenChannels_;
    CompletableFutureV2<void> allBarriersReceivedFuture_;
    std::shared_ptr<CompletableFutureV2<void>> allBarriersReceivedFuture_V2;
    CompletableFutureV2<void> completed;
    std::shared_ptr<CompletableFutureV2<void>> completed_V2 = std::make_shared<CompletableFutureV2<void>>();

    // State management
    BarrierHandlerState* currentState_;
    std::unique_ptr<BarrierAlignmentUtil::Cancellable> currentAlignmentTimer_;
    // Tracks whether the current checkpoint has been switched to Unaligned mode (timeout -> UC).
    bool currentCheckpointUnaligned_{false};
    // Guard against stale timer callbacks clearing a newer timer instance.
    int64_t currentAlignmentTimerCheckpointId_{-1};
    bool alternating_;
};
} // namespace omnistream::runtime

#endif // OMNISTREAM_SINGLECHECKPOINTBARRIERHANDLER_H
