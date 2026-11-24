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

#include "TaskStateManager.h"

#include "io/checkpointing/CheckpointException.h"

namespace omnistream {
TaskStateManager::TaskStateManager(
    JobIDPOD jobId,
    ExecutionAttemptIDPOD executionAttemptId,
    TaskLocalStateStore* localStateStore,
    CheckpointResponder* checkpointResponder,
    std::shared_ptr<TaskStateManagerBridge> bridge,
    std::shared_ptr<OmniTaskBridge> omniTaskBridge,
    std::shared_ptr<JobManagerTaskRestore> jobManagerTaskRestore)
    : localStateStore_(localStateStore),
      checkpointResponder_(checkpointResponder),
      jobId_(jobId),
      executionAttempId_(executionAttemptId),
      bridge_(bridge),
      jobManagerTaskRestore_(jobManagerTaskRestore),
      omniTaskBridge_(omniTaskBridge)
{
    std::shared_ptr<TaskStateSnapshot> taskStateSnapshot = jobManagerTaskRestore == nullptr
                                                               ? std::make_shared<TaskStateSnapshot>()
                                                               : jobManagerTaskRestore->getTaskStateSnapshot();
    sequentialChannelStateReader_ =
        std::make_shared<SequentialChannelStateReaderImpl>(taskStateSnapshot, omniTaskBridge);
}

TaskStateManager::~TaskStateManager()
{
    delete localStateStore_;
    delete checkpointResponder_;
}
void TaskStateManager::NotifyCheckpointCompleteV2(long checkpointId)
{
    std::string checkpointIdstr = std::to_string(checkpointId);
    bridge_->NotifyCheckpointComplete(checkpointIdstr);
}
void TaskStateManager::NotifyCheckpointAbortedV2(long checkpointId)
{
    std::string checkpointIdstr = std::to_string(checkpointId);
    bridge_->notifyCheckpointAborted(checkpointIdstr);
}

void TaskStateManager::NotifyCheckpointComplete(long checkpointId)
{
    localStateStore_->confirmCheckpoint(checkpointId);
}

void TaskStateManager::NotifyCheckpointAborted(long checkpointId)
{
    localStateStore_->abortCheckpoint(checkpointId);
}

void TaskStateManager::ReportTaskStateSnapshots(
    CheckpointMetaData* checkpointMetaData,
    CheckpointMetrics* checkpointMetrics,
    std::shared_ptr<TaskStateSnapshot> acknowledgedState,
    std::shared_ptr<TaskStateSnapshot> localState)
{
    long checkpointId = checkpointMetaData->GetCheckpointId();

    localStateStore_->storeLocalState(checkpointId, localState);

    checkpointResponder_->acknowledgeCheckpoint(
        jobId_, executionAttempId_, checkpointId, checkpointMetrics, acknowledgedState);
}

void TaskStateManager::ReportTaskStateSnapshotsV2(
    CheckpointMetaData* checkpointMetaData,
    CheckpointMetrics* checkpointMetrics,
    std::shared_ptr<TaskStateSnapshot> acknowledgedState,
    std::shared_ptr<TaskStateSnapshot> localState)
{
    std::string checkpointMetaDataJson = checkpointMetaData->ToString();
    std::string checkpointMetricsJson = checkpointMetrics->ToString();
    std::string acknowledgedStateJson = acknowledgedState == nullptr ? "" : acknowledgedState->ToString();
    std::string localStateJson = localState == nullptr ? "" : localState->ToString();
    if (bridge_ != nullptr) {
        bridge_->ReportTaskStateSnapshots(
            checkpointMetaDataJson, checkpointMetricsJson, acknowledgedStateJson, localStateJson);
    }
}

void TaskStateManager::ReportIncompleteTaskStateSnapshots(
    CheckpointMetaData* checkpointMetaData, CheckpointMetrics* checkpointMetrics)
{
    checkpointResponder_->reportCheckpointMetrics(
        jobId_, executionAttempId_, checkpointMetaData->GetCheckpointId(), checkpointMetrics);
}

PrioritizedOperatorSubtaskState TaskStateManager::prioritizedOperatorState(const OperatorID& operatorID)
{
    if (jobManagerTaskRestore_ == nullptr) {
        return PrioritizedOperatorSubtaskState();
    }

    std::shared_ptr<TaskStateSnapshot> jobManagerStateSnapshot = jobManagerTaskRestore_->getTaskStateSnapshot();

    auto jobManagerSubtaskState = jobManagerStateSnapshot->GetSubtaskStateByOperatorID(operatorID);
    if (jobManagerSubtaskState == nullptr) {
        return PrioritizedOperatorSubtaskState(jobManagerTaskRestore_->getRestoreCheckpointId());
    }

    long restoreCheckpointId = jobManagerTaskRestore_->getRestoreCheckpointId();

    std::shared_ptr<TaskStateSnapshot> localStateSnapshot = bridge_->RetrieveLocalState(restoreCheckpointId);
    if (localStateSnapshot == nullptr) {
        LOG("load local snapshot failed!");
    }

    localStateStore_->pruneMatchingCheckpoints(
        [restoreCheckpointId](long checkpointId) -> bool { return checkpointId != restoreCheckpointId; });

    std::vector<OperatorSubtaskState> alternativesByPriority;

    if (localStateSnapshot != nullptr) {
        auto localSubtaskState = localStateSnapshot->GetSubtaskStateByOperatorID(operatorID);
        if (localSubtaskState != nullptr) {
            alternativesByPriority.push_back(*localSubtaskState);
        }
    }

    PrioritizedOperatorSubtaskState::Builder builder(
        *jobManagerSubtaskState, alternativesByPriority, jobManagerTaskRestore_->getRestoreCheckpointId());

    return builder.build();
}

} // namespace omnistream
