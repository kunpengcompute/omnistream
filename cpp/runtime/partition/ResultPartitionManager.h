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

// ResultPartitionManager.h
#ifndef OMNISTREAM_RESULTPARTITIONMANAGER_H
#define OMNISTREAM_RESULTPARTITIONMANAGER_H

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ResultPartition.h"
#include "ResultSubpartitionView.h"
#include "BufferAvailabilityListener.h"

#include "ResultPartitionProvider.h"
// check
namespace omnistream {

class OmniTask;

class ResultPartitionManager : public ResultPartitionProvider {
public:
    ResultPartitionManager();
    ~ResultPartitionManager() override;

    void registerResultPartition(std::shared_ptr<ResultPartition> partition);

    /**
     * Binds the native task that produces `partitionId`. The manager deletes the task once both of
     * these have happened, whichever comes last:
     *   - every partition the task produces has been consumed by all downstream tasks, and
     *   - the task's run loop has returned (reported through onTaskRunFinished).
     */
    void bindOwningTask(const ResultPartitionIDPOD& partitionId, OmniTask* owningTask);

    /**
     * Reports that a task's run loop has returned. Deletes the task when all of its partitions were
     * already consumed. Returns true when the task was deleted, in which case the caller must not
     * touch it anymore.
     */
    bool onTaskRunFinished(OmniTask* task);

    std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
        const ResultPartitionIDPOD& partitionId,
        int subpartitionIndex,
        BufferAvailabilityListener* availabilityListener) override;

    void releasePartition(const ResultPartitionIDPOD& partitionId, std::optional<std::exception_ptr> cause);

    void shutdown();

    void onConsumedPartition(std::shared_ptr<ResultPartition> partition);

    std::vector<ResultPartitionIDPOD> getUnreleasedPartitions();

    std::string toString();

private:
    /**
     * Drops the binding of `partitionId` to its owning task. Returns the task when this was its last
     * unconsumed partition (the caller then owns the task and must delete it), nullptr otherwise.
     * Must be called with mutex_ held.
     */
    OmniTask* unbindOwningTask(const ResultPartitionIDPOD& partitionId);

    /**
     * Deletes a task and logs on both sides of the destructor, so the log proves the teardown ran
     * rather than only that it was started. Must be called without mutex_ held.
     */
    void deleteOwningTask(OmniTask* task, const std::string& reason);

    std::unordered_map<ResultPartitionIDPOD, std::shared_ptr<ResultPartition>> registeredPartitions;
    // Which task produced a partition, and how many of that task's partitions are still unconsumed.
    std::unordered_map<ResultPartitionIDPOD, OmniTask*> partitionToOwningTask_;
    std::unordered_map<OmniTask*, int> unconsumedPartitionsPerTask_;
    // Tasks waiting on the other half of the deletion condition.
    std::unordered_set<OmniTask*> tasksAwaitingRunFinish_; // all partitions consumed, still running
    std::unordered_set<OmniTask*> finishedTasks_;          // run loop returned, still being consumed
    // Accounting so a leak shows up as a mismatch at shutdown rather than needing log arithmetic.
    long boundTaskCount_ = 0;
    long deletedTaskCount_ = 0;
    bool isShutdown;
    std::recursive_mutex mutex_;
};

} // namespace omnistream

#endif // OMNISTREAM_RESULTPARTITIONMANAGER_H
