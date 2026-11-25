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

#ifndef OMNISTREAM_CHECKPOINTBARRIERHANDLE_H
#define OMNISTREAM_CHECKPOINTBARRIERHANDLE_H
#pragma once

#include <memory>
#include <chrono>
#include <string>
#include <stdexcept>
#include "metrics/Clock.h"
#include "io/network/api/CheckpointBarrier.h"
#include "partition/consumer/InputChannelInfo.h"
#include "runtime/jobgraph/tasks/CheckpointableTask.h"
#include "CheckpointException.h"
#include "io/network/api/CancelCheckpointMarker.h"
#include "core/utils/threads/CompletableFutureV2.h"

namespace omnistream {

class CheckpointBarrierHandler {
public:
    static constexpr int64_t OUTSIDE_OF_ALIGNMENT = INT64_MIN;

    virtual ~CheckpointBarrierHandler();

    bool IsCheckpointAfterTasksFinishedEnabled() const;
    virtual void Close() {};

    virtual void ProcessBarrier(
        const CheckpointBarrier& receivedBarrier, const InputChannelInfo& channelInfo, bool isRpcTriggered) = 0;

    virtual void ProcessBarrierAnnouncement(
        const CheckpointBarrier& announcedBarrier, int sequenceNumber, const InputChannelInfo& channelInfo) = 0;

    virtual void ProcessCancellationBarrier(
        const CancelCheckpointMarker& cancelBarrier, const InputChannelInfo& channelInfo) = 0;

    virtual void ProcessEndOfPartition(const InputChannelInfo& channelInfo) = 0;

    virtual int64_t GetLatestCheckpointId() const = 0;

    int64_t GetAlignmentDurationNanos();
    int64_t GetCheckpointStartDelayNanos() const;

    virtual std::shared_ptr<CompletableFutureV2<void>> GetAllBarriersReceivedFuture(int64_t checkpointId);

    virtual bool IsCheckpointPending() const = 0;
    void AddProcessedBytes(int bytes);

protected:
    CheckpointBarrierHandler(
        CheckpointableTask* toNotifyOnCheckpoint, Clock& clock, bool enableCheckpointAfterTasksFinished);
    void NotifyCheckpoint(const CheckpointBarrier& checkpointBarrier);
    void NotifyAbortOnCancellationBarrier(int64_t checkpointId);
    void NotifyAbort(int64_t checkpointId, const CheckpointException& cause);

    void MarkAlignmentStartAndEnd(int64_t checkpointId, int64_t checkpointCreationTimestamp);
    void MarkAlignmentStart(int64_t checkpointId, int64_t checkpointCreationTimestamp);
    void MarkAlignmentEnd();
    void MarkAlignmentEnd(int64_t alignmentDuration);
    void ResetAlignment();
    bool IsDuringAlignment() const;
    Clock& GetClock() const;
    Clock& clock;

private:
    CheckpointableTask* toNotifyOnCheckpoint;
    std::shared_ptr<CompletableFutureV2<int64_t>> latestAlignmentDurationNanos;
    int64_t latestCheckpointStartDelayNanos = 0;
    int64_t startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;
    int64_t startAlignmentCheckpointId = -1;
    int64_t bytesProcessedDuringAlignment = 0;
    std::shared_ptr<CompletableFutureV2<int64_t>> latestBytesProcessedDuringAlignment;
    const bool enableCheckpointAfterTasksFinished;
    CompletableFutureV2<void> completed;
    std::shared_ptr<CompletableFutureV2<void>> completed_V2 = std::make_shared<CompletableFutureV2<void>>();

    // Disable copying
    CheckpointBarrierHandler(const CheckpointBarrierHandler&) = delete;
    CheckpointBarrierHandler& operator=(const CheckpointBarrierHandler&) = delete;
};
} // namespace omnistream

#endif // OMNISTREAM_CHECKPOINTBARRIERHANDLE_H
