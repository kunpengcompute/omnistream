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

#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_H

#include <memory>
#include <functional>
#include <atomic>
#include <vector>
#include <stdexcept>
#include "runtime/jobgraph/JobVertexID.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/state/CheckpointStorageLocationReference.h"
#include "ChannelStateWriter.h"
#include "ChannelStateCheckpointWriter.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/buffer/ObjectBuffer.h"

namespace omnistream {

enum class CheckpointInProgressRequestState {
    NEW,
    EXECUTING,
    COMPLETED,
    FAILED,
    CANCELLED
};

class ChannelStateWriteRequest {
public:
    ChannelStateWriteRequest() = default;
    ChannelStateWriteRequest(JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::string& name);
    virtual ~ChannelStateWriteRequest() = default;

    JobVertexID getJobVertexID() const;
    int getSubtaskIndex() const;
    long getCheckpointId() const;
    std::string getName() const;

    virtual std::shared_ptr<CompletableFutureV2<void>> getReadyFuture();
    virtual void cancel(const std::exception_ptr& cause) = 0;
    virtual void execute(std::shared_ptr<ChannelStateCheckpointWriter> writer) = 0;

    static std::shared_ptr<ChannelStateWriteRequest> completeInput(
        JobVertexID jobVertexID, int subtaskIndex, long checkpointId);

    static std::shared_ptr<ChannelStateWriteRequest> completeOutput(
        JobVertexID jobVertexID, int subtaskIndex, long checkpointId);

    static std::shared_ptr<ChannelStateWriteRequest> writeInput(
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        InputChannelInfo info,
        std::vector<Buffer*> buffers);

    static std::shared_ptr<ChannelStateWriteRequest> writeOutput(
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        ResultSubpartitionInfoPOD info,
        std::vector<Buffer*> buffers);

    static std::shared_ptr<ChannelStateWriteRequest> writeOutputFuture(
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        ResultSubpartitionInfoPOD info,
        std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> dataFuture);

    static std::shared_ptr<ChannelStateWriteRequest> start(
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
        const std::string name);

    static std::shared_ptr<ChannelStateWriteRequest> terminate(
        JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::exception_ptr& cause);

    static std::shared_ptr<ChannelStateWriteRequest> registerSubtask(JobVertexID jobVertexID, int subtaskIndex);

    static std::shared_ptr<ChannelStateWriteRequest> releaseSubtask(JobVertexID jobVertexID, int subtaskIndex);

private:
    JobVertexID jobVertexID_;
    int subtaskIndex_;
    long checkpointId_;
    std::string name_;
};

class CheckpointStartRequest : public ChannelStateWriteRequest {
public:
    CheckpointStartRequest() = default;
    virtual ~CheckpointStartRequest() = default;
    CheckpointStartRequest(
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
        std::shared_ptr<CheckpointStorageLocationReference> locationReference);

    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> getTargetResult();
    std::shared_ptr<CheckpointStorageLocationReference> getLocationReference();

    void cancel(const std::exception_ptr& cause) override;
    void execute(std::shared_ptr<ChannelStateCheckpointWriter> writer) override;

private:
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult_;
    std::shared_ptr<CheckpointStorageLocationReference> locationReference_;
};

class CheckpointInProgressRequest : public ChannelStateWriteRequest {
public:
    using Action = std::function<void(std::shared_ptr<ChannelStateCheckpointWriter>&)>;
    using DiscardAction = std::function<void(const std::exception_ptr&)>;

    CheckpointInProgressRequest(
        const std::string& name,
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        Action action,
        DiscardAction discardAction,
        std::shared_ptr<CompletableFutureV2<void>> readyFuture = nullptr);

    CheckpointInProgressRequest(
        const std::string& name, JobVertexID jobVertexID, int subtaskIndex, long checkpointId, Action action);

    CheckpointInProgressRequest(
        const std::string& name,
        JobVertexID jobVertexID,
        int subtaskIndex,
        long checkpointId,
        Action action,
        DiscardAction discardAction,
        std::shared_ptr<CompletableFutureV2<std::vector<ObjectBuffer>>> dataFuture);

    std::shared_ptr<CompletableFutureV2<void>> getReadyFuture() override;
    void cancel(const std::exception_ptr& cause) override;
    void execute(std::shared_ptr<ChannelStateCheckpointWriter> writer) override;

private:
    Action action_;
    DiscardAction discardAction_;
    std::shared_ptr<CompletableFutureV2<void>> readyFuture_;
    std::shared_ptr<CompletableFutureV2<std::vector<ObjectBuffer>>> dataFuture_;
    std::atomic<CheckpointInProgressRequestState> state_{CheckpointInProgressRequestState::NEW};
};

class CheckpointAbortRequest : public ChannelStateWriteRequest {
public:
    CheckpointAbortRequest(
        JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::exception_ptr& cause);

    const std::exception_ptr& getCause() const;
    void cancel(const std::exception_ptr&) override;
    void execute(std::shared_ptr<ChannelStateCheckpointWriter> writer) override;

private:
    std::exception_ptr cause_;
};

class SubtaskRegisterRequest : public ChannelStateWriteRequest {
public:
    SubtaskRegisterRequest(JobVertexID jobVertexID, int subtaskIndex);
    void cancel(const std::exception_ptr&) override;
    void execute(std::shared_ptr<ChannelStateCheckpointWriter> writer) override;
};

class SubtaskReleaseRequest : public ChannelStateWriteRequest {
public:
    SubtaskReleaseRequest(JobVertexID jobVertexID, int subtaskIndex);
    void cancel(const std::exception_ptr&) override;
    void execute(std::shared_ptr<ChannelStateCheckpointWriter> writer) override;
};

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_H
