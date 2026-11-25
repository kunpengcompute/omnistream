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

#include "CheckpointBarrierHandler.h"
#include "metrics/Clock.h"

namespace omnistream {
CheckpointBarrierHandler::CheckpointBarrierHandler(
    CheckpointableTask* toNotifyOnCheckpoint, Clock& clock, bool enableCheckpointAfterTasksFinished)
    : clock(clock),
      toNotifyOnCheckpoint(toNotifyOnCheckpoint),
      enableCheckpointAfterTasksFinished(enableCheckpointAfterTasksFinished)
{
    // Initialize
    latestAlignmentDurationNanos = std::make_shared<CompletableFutureV2<int64_t>>();
    latestBytesProcessedDuringAlignment = std::make_shared<CompletableFutureV2<int64_t>>();
}

CheckpointBarrierHandler::~CheckpointBarrierHandler() = default;

bool CheckpointBarrierHandler::IsCheckpointAfterTasksFinishedEnabled() const
{
    return enableCheckpointAfterTasksFinished;
}

int64_t CheckpointBarrierHandler::GetAlignmentDurationNanos()
{
    if (IsDuringAlignment()) {
        return clock.RelativeTimeNanos() - startOfAlignmentTimestamp;
    }
    try {
        return latestAlignmentDurationNanos->Get();
    } catch (...) {
        return 0;
    }
}

int64_t CheckpointBarrierHandler::GetCheckpointStartDelayNanos() const
{
    return latestCheckpointStartDelayNanos;
}

std::shared_ptr<CompletableFutureV2<void>> CheckpointBarrierHandler::GetAllBarriersReceivedFuture(int64_t checkpointId)
{
    if (!completed_V2->IsDone()) {
        completed_V2->Complete();
    }
    return completed_V2;
}

void CheckpointBarrierHandler::NotifyCheckpoint(const CheckpointBarrier& checkpointBarrier)
{
    LOG(">>>>>>>> startAlignmentCheckpointId " << startAlignmentCheckpointId);
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    auto checkpointMetadata =
        std::make_shared<CheckpointMetaData>(checkpointBarrier.GetId(), checkpointBarrier.GetTimestamp(), ms);
    auto checkpointMetrics = std::make_shared<CheckpointMetricsBuilder>();
    if (checkpointBarrier.GetId() == startAlignmentCheckpointId) {
        checkpointMetrics->SetAlignmentDurationNanos(latestAlignmentDurationNanos);
        checkpointMetrics->SetBytesProcessedDuringAlignment(latestBytesProcessedDuringAlignment);
        checkpointMetrics->SetCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);
    } else {
        checkpointMetrics->SetAlignmentDurationNanos(0L);
        checkpointMetrics->SetBytesProcessedDuringAlignment(0L);
        checkpointMetrics->SetCheckpointStartDelayNanos(0);
    }

    toNotifyOnCheckpoint->TriggerCheckpointOnBarrier(
        checkpointMetadata, checkpointBarrier.GetCheckpointOptions(), checkpointMetrics);
}

void CheckpointBarrierHandler::NotifyAbortOnCancellationBarrier(int64_t checkpointId)
{
    NotifyAbort(
        checkpointId, CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
}

void CheckpointBarrierHandler::NotifyAbort(int64_t checkpointId, const CheckpointException& cause)
{
    toNotifyOnCheckpoint->abortCheckpointOnBarrier(checkpointId, cause);
}

void CheckpointBarrierHandler::MarkAlignmentStartAndEnd(int64_t checkpointId, int64_t checkpointCreationTimestamp)
{
    MarkAlignmentStart(checkpointId, checkpointCreationTimestamp);
    MarkAlignmentEnd(0);
}

void CheckpointBarrierHandler::MarkAlignmentStart(int64_t checkpointId, int64_t checkpointCreationTimestamp)
{
    const auto i = 1000000;
    latestCheckpointStartDelayNanos =
        i * std::max(int64_t(0), clock.AbsoluteTimeMillis() - checkpointCreationTimestamp);
    ResetAlignment();
    startOfAlignmentTimestamp = clock.RelativeTimeNanos();
    startAlignmentCheckpointId = checkpointId;
}

void CheckpointBarrierHandler::MarkAlignmentEnd()
{
    int64_t end = clock.RelativeTimeNanos();
    MarkAlignmentEnd(end - startOfAlignmentTimestamp);
}

void CheckpointBarrierHandler::MarkAlignmentEnd(int64_t alignmentDuration)
{
    if (alignmentDuration < 0) {
        throw std::runtime_error(
            "Alignment time is less than zero(" + std::to_string(alignmentDuration) + "). Is the time monotonic?");
    }
    latestAlignmentDurationNanos->Complete(alignmentDuration);
    latestBytesProcessedDuringAlignment->Complete(bytesProcessedDuringAlignment);
    startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;
    bytesProcessedDuringAlignment = 0;
}

void CheckpointBarrierHandler::ResetAlignment()
{
    MarkAlignmentEnd(0);
    latestAlignmentDurationNanos = std::make_shared<CompletableFutureV2<int64_t>>();
    latestBytesProcessedDuringAlignment = std::make_shared<CompletableFutureV2<int64_t>>();
}

void CheckpointBarrierHandler::AddProcessedBytes(int bytes)
{
    if (IsDuringAlignment()) {
        bytesProcessedDuringAlignment += bytes;
    }
}

bool CheckpointBarrierHandler::IsDuringAlignment() const
{
    return startOfAlignmentTimestamp > OUTSIDE_OF_ALIGNMENT;
}

Clock& CheckpointBarrierHandler::GetClock() const
{
    return clock;
}

} // namespace omnistream
