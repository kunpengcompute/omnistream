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

#ifndef OMNISTREAM_CHECKPOINTRESPONDER
#define OMNISTREAM_CHECKPOINTRESPONDER

#include "runtime/checkpoint/CheckpointMetrics.h"
#include "runtime/executiongraph/descriptor/ExecutionAttemptIDPOD.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/checkpoint/TaskStateSnapshot.h"
namespace omnistream {
class CheckpointResponder {
public:
    virtual void acknowledgeCheckpoint(
        const JobIDPOD& jobID,
        const ExecutionAttemptIDPOD& executionAttemptID,
        long checkpointId,
        CheckpointMetrics* checkpointMetrics,
        std::shared_ptr<TaskStateSnapshot> subtaskState) = 0;

    virtual void reportCheckpointMetrics(
        const JobIDPOD& jobId,
        const ExecutionAttemptIDPOD& executionAttemptID,
        long checkpointId,
        CheckpointMetrics* checkpointMetrics) = 0;

    virtual void DeclineCheckpoint(
        const JobIDPOD& jobID, const ExecutionAttemptIDPOD& executionAttemptID, long checkpointId) = 0;
    virtual ~CheckpointResponder() = default;
};

class NoOpCheckpoingResponder : public CheckpointResponder {
public:
    void acknowledgeCheckpoint(
        const JobIDPOD& jobID,
        const ExecutionAttemptIDPOD& executionAttemptID,
        long checkpointId,
        CheckpointMetrics* checkpointMetrics,
        std::shared_ptr<TaskStateSnapshot> subtaskState) override {};

    void reportCheckpointMetrics(
        const JobIDPOD& jobId,
        const ExecutionAttemptIDPOD& executionAttemptID,
        long checkpointId,
        CheckpointMetrics* checkpointMetrics) override {};

    void DeclineCheckpoint(
        const JobIDPOD& jobID, const ExecutionAttemptIDPOD& executionAttemptID, long checkpointId) override {};
};
} // namespace omnistream
#endif // OMNISTREAM_CHECKPOINTRESPONDER
