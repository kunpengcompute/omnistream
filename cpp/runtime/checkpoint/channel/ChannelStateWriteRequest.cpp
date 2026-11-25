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

#include "ChannelStateWriteRequest.h"
#include "CheckpointBufferUtils.h"

namespace omnistream {

ChannelStateWriteRequest::ChannelStateWriteRequest(
    JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::string& name)
    : jobVertexID_(jobVertexID),
      subtaskIndex_(subtaskIndex),
      checkpointId_(checkpointId),
      name_(name)
{
}

JobVertexID ChannelStateWriteRequest::getJobVertexID() const
{
    return jobVertexID_;
}
int ChannelStateWriteRequest::getSubtaskIndex() const
{
    return subtaskIndex_;
}
long ChannelStateWriteRequest::getCheckpointId() const
{
    return checkpointId_;
}
std::string ChannelStateWriteRequest::getName() const
{
    return name_;
}

std::shared_ptr<CompletableFutureV2<void>> ChannelStateWriteRequest::getReadyFuture()
{
    auto f = std::make_shared<CompletableFutureV2<void>>();
    f->Complete();
    return f;
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::completeInput(
    JobVertexID jobVertexID, int subtaskIndex, long checkpointId)
{
    auto ready = std::make_shared<CompletableFutureV2<void>>();
    ready->Complete();

    return std::make_shared<CheckpointInProgressRequest>(
        "CheckpointCompleteInput",
        jobVertexID,
        subtaskIndex,
        checkpointId,
        [jobVertexID, subtaskIndex](std::shared_ptr<ChannelStateCheckpointWriter>& w) {
            w->CompleteInput(jobVertexID, subtaskIndex);
        },
        [](const std::exception_ptr&) {},
        std::move(ready));
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::completeOutput(
    JobVertexID jobVertexID, int subtaskIndex, long checkpointId)
{
    auto ready = std::make_shared<CompletableFutureV2<void>>();
    ready->Complete();

    return std::make_shared<CheckpointInProgressRequest>(
        "CheckpointCompleteOutput",
        jobVertexID,
        subtaskIndex,
        checkpointId,
        [jobVertexID, subtaskIndex](std::shared_ptr<ChannelStateCheckpointWriter>& w) {
            w->CompleteOutput(jobVertexID, subtaskIndex);
        },
        [](const std::exception_ptr&) {},
        std::move(ready));
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::writeInput(
    JobVertexID jobVertexID, int subtaskIndex, long checkpointId, InputChannelInfo info, std::vector<Buffer*> buffers)
{
    auto pendingBuffers = std::make_shared<std::vector<Buffer*>>(std::move(buffers));
    return std::make_shared<CheckpointInProgressRequest>(
        "WriteInput",
        jobVertexID,
        subtaskIndex,
        checkpointId,
        [jobVertexID, subtaskIndex, info, pendingBuffers](std::shared_ptr<ChannelStateCheckpointWriter>& writer) {
            for (Buffer*& buffer : *pendingBuffers) {
                if (buffer != nullptr) {
                    Buffer* bufferToWrite = buffer;
                    // From this point ChannelStateCheckpointWriter owns this reference and recycles it even if the
                    // serialization or stream write throws.
                    buffer = nullptr;
                    writer->WriteInput(jobVertexID, subtaskIndex, info, bufferToWrite);
                }
            }
        },
        [pendingBuffers](const std::exception_ptr&) {
            for (Buffer*& buffer : *pendingBuffers) {
                if (buffer != nullptr) {
                    Buffer* bufferToRecycle = buffer;
                    buffer = nullptr;
                    try {
                        ReleaseCheckpointBuffer(bufferToRecycle);
                    } catch (const std::exception& e) {
                        ERROR_RELEASE("Failed to recycle a pending channel-state input buffer: " << e.what());
                    } catch (...) {
                        ERROR_RELEASE(
                            "Failed to recycle a pending channel-state input buffer due to an unknown "
                            "exception");
                    }
                }
            }
        });
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::writeOutput(
    JobVertexID jobVertexID,
    int subtaskIndex,
    long checkpointId,
    ResultSubpartitionInfoPOD info,
    std::vector<Buffer*> buffers)
{
    auto pendingBuffers = std::make_shared<std::vector<Buffer*>>(std::move(buffers));
    return std::make_shared<CheckpointInProgressRequest>(
        "writeOutput",
        jobVertexID,
        subtaskIndex,
        checkpointId,
        [jobVertexID, subtaskIndex, info, pendingBuffers](std::shared_ptr<ChannelStateCheckpointWriter>& writer) {
            for (Buffer*& buffer : *pendingBuffers) {
                if (buffer != nullptr) {
                    Buffer* bufferToWrite = buffer;
                    // From this point ChannelStateCheckpointWriter owns this reference and recycles it even if the
                    // serialization or stream write throws.
                    buffer = nullptr;
                    writer->WriteOutput(jobVertexID, subtaskIndex, info, bufferToWrite);
                }
            }
        },
        [pendingBuffers](const std::exception_ptr&) {
            for (Buffer*& buffer : *pendingBuffers) {
                if (buffer != nullptr) {
                    Buffer* bufferToRecycle = buffer;
                    buffer = nullptr;
                    try {
                        ReleaseCheckpointBuffer(bufferToRecycle);
                    } catch (const std::exception& e) {
                        ERROR_RELEASE("Failed to recycle a pending channel-state output buffer: " << e.what());
                    } catch (...) {
                        ERROR_RELEASE(
                            "Failed to recycle a pending channel-state output buffer due to an unknown "
                            "exception");
                    }
                }
            }
        });
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::writeOutputFuture(
    JobVertexID jobVertexID,
    int subtaskIndex,
    long checkpointId,
    ResultSubpartitionInfoPOD info,
    std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> dataFuture)
{
    auto voidFuture = std::make_shared<CompletableFutureV2<void>>();

    dataFuture->ThenRun([voidFuture]() { voidFuture->Complete(); });

    return std::make_shared<CheckpointInProgressRequest>(
        "writeOutputFuture",
        jobVertexID,
        subtaskIndex,
        checkpointId,
        [jobVertexID, subtaskIndex, info, dataFuture](std::shared_ptr<ChannelStateCheckpointWriter>& writer) {
            auto buffers = dataFuture->Get();
            for (Buffer* buffer : buffers) {
                if (buffer) {
                    writer->WriteOutput(jobVertexID, subtaskIndex, info, buffer);
                }
            }
        },
        [dataFuture](const std::exception_ptr&) {
            if (dataFuture->IsDone()) {
                auto buffers = dataFuture->GetNow({});
                for (auto* buffer : buffers) {
                    if (buffer) {
                        ReleaseCheckpointBuffer(buffer);
                    }
                }
            }
        },
        voidFuture);
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::start(
    JobVertexID jobVertexID,
    int subtaskIndex,
    long checkpointId,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
    const std::string name)
{
    std::shared_ptr<CheckpointStorageLocationReference> locationReference =
        std::make_shared<CheckpointStorageLocationReference>();
    return std::make_shared<CheckpointStartRequest>(
        jobVertexID, subtaskIndex, checkpointId, targetResult, locationReference);
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::terminate(
    JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::exception_ptr& cause)
{
    return std::make_shared<CheckpointAbortRequest>(jobVertexID, subtaskIndex, checkpointId, cause);
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::registerSubtask(
    JobVertexID jobVertexID, int subtaskIndex)
{
    return std::make_shared<SubtaskRegisterRequest>(jobVertexID, subtaskIndex);
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequest::releaseSubtask(
    JobVertexID jobVertexID, int subtaskIndex)
{
    return std::make_shared<SubtaskReleaseRequest>(jobVertexID, subtaskIndex);
}

CheckpointStartRequest::CheckpointStartRequest(
    JobVertexID jobVertexID,
    int subtaskIndex,
    long checkpointId,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
    std::shared_ptr<CheckpointStorageLocationReference> locationReference)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, checkpointId, "Start"),
      targetResult_(targetResult),
      locationReference_(locationReference)
{
}

std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> CheckpointStartRequest::getTargetResult()
{
    return targetResult_;
}

std::shared_ptr<CheckpointStorageLocationReference> CheckpointStartRequest::getLocationReference()
{
    return locationReference_;
}

void CheckpointStartRequest::cancel(const std::exception_ptr& cause)
{
    targetResult_->Fail(cause);
}

void CheckpointStartRequest::execute(std::shared_ptr<ChannelStateCheckpointWriter> writer)
{
    writer->Start(getJobVertexID(), getSubtaskIndex(), targetResult_, locationReference_);
}

CheckpointInProgressRequest::CheckpointInProgressRequest(
    const std::string& name,
    JobVertexID jobVertexID,
    int subtaskIndex,
    long checkpointId,
    Action action,
    DiscardAction discardAction,
    std::shared_ptr<CompletableFutureV2<void>> readyFuture)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, checkpointId, name),
      action_(std::move(action)),
      discardAction_(std::move(discardAction)),
      readyFuture_(readyFuture)
{
}

CheckpointInProgressRequest::CheckpointInProgressRequest(
    const std::string& name,
    JobVertexID jobVertexID,
    int subtaskIndex,
    long checkpointId,
    Action action,
    DiscardAction discardAction,
    std::shared_ptr<CompletableFutureV2<std::vector<ObjectBuffer>>> dataFuture)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, checkpointId, name),
      action_(std::move(action)),
      discardAction_(std::move(discardAction)),
      readyFuture_(nullptr),
      dataFuture_(dataFuture)
{
}

CheckpointInProgressRequest::CheckpointInProgressRequest(
    const std::string& name, JobVertexID jobVertexID, int subtaskIndex, long checkpointId, Action action)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, checkpointId, name),
      action_(std::move(action))
{
}

std::shared_ptr<CompletableFutureV2<void>> CheckpointInProgressRequest::getReadyFuture()
{
    return readyFuture_ ? readyFuture_ : ChannelStateWriteRequest::getReadyFuture();
}

void CheckpointInProgressRequest::cancel(const std::exception_ptr& cause)
{
    CheckpointInProgressRequestState expected = CheckpointInProgressRequestState::NEW;
    if (state_.compare_exchange_strong(expected, CheckpointInProgressRequestState::CANCELLED) ||
        (state_.load() == CheckpointInProgressRequestState::FAILED &&
         state_.compare_exchange_strong(expected, CheckpointInProgressRequestState::CANCELLED))) {
        if (discardAction_) {
            discardAction_(cause);
        }
        if (readyFuture_) {
            readyFuture_->Cancel();
        }
        if (dataFuture_) {
            dataFuture_->Cancel();
        }
    }
}

void CheckpointInProgressRequest::execute(std::shared_ptr<ChannelStateCheckpointWriter> writer)
{
    CheckpointInProgressRequestState expected = CheckpointInProgressRequestState::NEW;
    if (!state_.compare_exchange_strong(expected, CheckpointInProgressRequestState::EXECUTING)) {
        throw std::runtime_error("Request not in NEW state");
    }

    try {
        action_(writer);
        state_ = CheckpointInProgressRequestState::COMPLETED;
    } catch (const std::exception& e) {
        state_ = CheckpointInProgressRequestState::FAILED;
        INFO_RELEASE("ERROR: Failed to execute a channel-state checkpoint request: " << e.what());
        throw;
    } catch (...) {
        state_ = CheckpointInProgressRequestState::FAILED;
        INFO_RELEASE("ERROR: Failed to execute a channel-state checkpoint request due to an unknown exception");
        throw;
    }
}

CheckpointAbortRequest::CheckpointAbortRequest(
    JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::exception_ptr& cause)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, checkpointId, "Abort"),
      cause_(cause)
{
}

const std::exception_ptr& CheckpointAbortRequest::getCause() const
{
    return cause_;
}

void CheckpointAbortRequest::cancel(const std::exception_ptr&)
{
}

void CheckpointAbortRequest::execute(std::shared_ptr<ChannelStateCheckpointWriter> writer)
{
    writer->Abort(getJobVertexID(), getSubtaskIndex(), cause_);
}

SubtaskRegisterRequest::SubtaskRegisterRequest(JobVertexID jobVertexID, int subtaskIndex)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, 0, "Register")
{
}

void SubtaskRegisterRequest::cancel(const std::exception_ptr&)
{
    // No-op for register requests
}

void SubtaskRegisterRequest::execute(std::shared_ptr<ChannelStateCheckpointWriter> writer)
{
    writer->RegisterSubtask(getJobVertexID(), getSubtaskIndex());
}

SubtaskReleaseRequest::SubtaskReleaseRequest(JobVertexID jobVertexID, int subtaskIndex)
    : ChannelStateWriteRequest(jobVertexID, subtaskIndex, 0, "Release")
{
}

void SubtaskReleaseRequest::cancel(const std::exception_ptr&)
{
}

void SubtaskReleaseRequest::execute(std::shared_ptr<ChannelStateCheckpointWriter> writer)
{
    writer->ReleaseSubtask(SubtaskID(getJobVertexID(), getSubtaskIndex()));
}

} // namespace omnistream
