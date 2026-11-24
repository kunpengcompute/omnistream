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

#ifndef OMNISTREAM_ASYNCCHECKPOINTRUNNABLE_H
#define OMNISTREAM_ASYNCCHECKPOINTRUNNABLE_H
#include <string>
#include <atomic>
#include <chrono>
#include "runtime/jobgraph/OperatorID.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/execution/Environment.h"
#include "runtime/checkpoint/TaskStateSnapshot.h"
#include "streaming/api/operators/OperatorSnapshotFinalizer.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "core/fs/FileSystemSafetyNet.h"
#include "runtime/checkpoint/CheckpointMetricsBuilder.h"
#include "runtime/checkpoint/CheckpointMetaData.h"
#include "core/utils/function/Supplier.h"
#include "runtime/execution/OmniEnvironment.h"

class SnapshotsFinalizeResult {
public:
    std::shared_ptr<TaskStateSnapshot> jobManagerTaskOperatorSubtaskStates;
    std::shared_ptr<TaskStateSnapshot> localTaskOperatorSubtaskStates;
    long bytesPersistedDuringAlignment;

    SnapshotsFinalizeResult(
        std::shared_ptr<TaskStateSnapshot> jobManagerStates,
        std::shared_ptr<TaskStateSnapshot> localStates,
        long bytesPersisted)
        : jobManagerTaskOperatorSubtaskStates(std::move(jobManagerStates)),
          localTaskOperatorSubtaskStates(std::move(localStates)),
          bytesPersistedDuringAlignment(bytesPersisted)
    {
    }
};

class AsyncCheckpointRunnable {
public:
    AsyncCheckpointRunnable(
        std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress,
        const CheckpointMetaData& checkpointMetaData,
        const CheckpointMetricsBuilder& checkpointMetrics,
        long asyncConstructionNanos,
        const std::string& taskName,
        std::function<void(AsyncCheckpointRunnable*)>* unregister,
        std::shared_ptr<omnistream::EnvironmentV2> taskEnvironment,
        std::function<void(std::string, std::exception)>* asyncExceptionHandler,
        bool isTaskDeployedAsFinished,
        bool isTaskFinished,
        std::shared_ptr<omnistream::Supplier<bool>> isTaskRunning)
        : operatorSnapshotsInProgress(operatorSnapshotsInProgress),
          checkpointMetaData(checkpointMetaData),
          checkpointMetric(checkpointMetrics),
          asyncConstructionNanos(asyncConstructionNanos),
          taskName(taskName),
          consumer(unregister),
          taskEnvironment(taskEnvironment),
          asyncExceptionHandler(asyncExceptionHandler),
          isTaskDeployedAsFinished(isTaskDeployedAsFinished),
          isTaskFinished(isTaskFinished),
          isTaskRunning(isTaskRunning),
          asyncCheckpointState(AsyncCheckpointState::RUNNING),
          finishedFuture()
    {
    }

    ~AsyncCheckpointRunnable()
    {
        if (operatorSnapshotsInProgress) {
            const bool shouldCancel = asyncCheckpointState.load() == AsyncCheckpointState::RUNNING;
            for (auto& entry : *operatorSnapshotsInProgress) {
                if (entry.second == nullptr) {
                    continue;
                }
                if (shouldCancel) {
                    try {
                        entry.second->cancel();
                    } catch (...) {
                    }
                }
                delete entry.second;
                entry.second = nullptr;
            }
            delete operatorSnapshotsInProgress;
            operatorSnapshotsInProgress = nullptr;
        }
        if (consumer) {
            delete consumer;
            consumer = nullptr;
        }
        if (asyncExceptionHandler) {
            delete asyncExceptionHandler;
            asyncExceptionHandler = nullptr;
        }
    }

    enum class AsyncCheckpointState {
        RUNNING,
        DSICARDED,
        COMPLETED
    };

    bool IsRunning() const;

    void Run();

    bool IsFinished() const;

    long GetCheckpointId() const;

    void Close();

private:
    SnapshotsFinalizeResult* FinalizeNonFinishedSnapshots();
    SnapshotsFinalizeResult* FinalizedFinishedSnapshots();
    void ReportCompletedSnapshotStates(
        std::shared_ptr<TaskStateSnapshot> acknowledgedTaskStateSnapshot,
        std::shared_ptr<TaskStateSnapshot> localTaskStateSnapshot,
        long asyncDurationMillis);
    void HandleExecutionException(std::__exception_ptr::exception_ptr e);
    std::pair<long, long> Cleanup();
    void ReportAbortedSnapshotStats(long stateSize, long checkpointedSize);
    void LogFailedCleanupAttempt();

    std::unordered_map<OperatorID, OperatorSnapshotFutures*>* operatorSnapshotsInProgress;
    CheckpointMetaData checkpointMetaData;
    CheckpointMetricsBuilder checkpointMetric;
    long asyncConstructionNanos;
    std::string taskName;
    std::function<void(AsyncCheckpointRunnable*)>* consumer;
    std::shared_ptr<omnistream::EnvironmentV2> taskEnvironment;
    std::function<void(std::string, std::exception)>* asyncExceptionHandler;
    bool isTaskDeployedAsFinished;
    bool isTaskFinished;
    std::shared_ptr<omnistream::Supplier<bool>> isTaskRunning;
    std::atomic<AsyncCheckpointState> asyncCheckpointState;
    CompletableFutureV2<void> finishedFuture;
};
#endif // OMNISTREAM_ASYNCCHECKPOINTRUNNABLE_H
