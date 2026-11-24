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

#include "ChannelStateWriteRequestDispatcherImpl.h"

namespace omnistream {

ChannelStateWriteRequestDispatcherImpl::ChannelStateWriteRequestDispatcherImpl(
    std::shared_ptr<CheckpointStorage> checkpointStorage,
    const JobIDPOD& jobID,
    std::shared_ptr<ChannelStateSerializer> serializer,
    std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver)
    : checkpointStorage(checkpointStorage),
      jobID(jobID),
      serializer(serializer),
      ongoingCheckpointId(-1),
      maxAbortedCheckpointId(-1),
      abortedSubtaskID(JobVertexID(-1, -1), -1),
      streamFactoryResolver(streamFactoryResolver)
{
}

void ChannelStateWriteRequestDispatcherImpl::dispatch(std::shared_ptr<ChannelStateWriteRequest> request)
{
    try {
        dispatchInternal(request);
    } catch (...) {
        request->cancel(std::current_exception());
    }
}

void ChannelStateWriteRequestDispatcherImpl::fail(const std::exception_ptr& cause)
{
    std::lock_guard<std::mutex> lock(mutex);
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    auto it = writers.find(ongoingCheckpointId);
    if (it != writers.end()) {
        writer = it->second;
    }
    if (writer) {
        writer->Fail(cause);
        writer.reset();
    }
}

void ChannelStateWriteRequestDispatcherImpl::dispatchInternal(std::shared_ptr<ChannelStateWriteRequest> request)
{
    std::lock_guard<std::mutex> lock(mutex);
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    auto it = writers.find(request->getCheckpointId());
    LOG_DEBUG(
        " dispatchInternal " << request->getName() << " checkpoint " << request->getCheckpointId()
                             << " ongoingCheckpointId " << ongoingCheckpointId);
    if (it != writers.end()) {
        writer = it->second;
    }
    if (auto req = std::dynamic_pointer_cast<SubtaskRegisterRequest>(request)) {
        registeredSubtasks.insert(SubtaskID::Of(req->getJobVertexID(), req->getSubtaskIndex()));
        if (writer) {
            req->execute(writer);
        }
    } else if (auto req = std::dynamic_pointer_cast<CheckpointStartRequest>(request)) {
        handleCheckpointStartRequest(req);
    } else if (auto req = std::dynamic_pointer_cast<CheckpointInProgressRequest>(request)) {
        if (writer && ongoingCheckpointId == request->getCheckpointId()) {
            LOG_DEBUG(" dispatchInternal " << request->getName() << " checkpoint " << request->getCheckpointId());
            req->execute(writer);
        } else {
            req->cancel(
                std::make_exception_ptr(
                    std::runtime_error(
                        "Channel state writer not found for checkpoint " +
                        std::to_string(request->getCheckpointId()))));
        }
    } else if (auto req = std::dynamic_pointer_cast<SubtaskReleaseRequest>(request)) {
        SubtaskID sid = SubtaskID::Of(req->getJobVertexID(), req->getSubtaskIndex());
        registeredSubtasks.erase(sid);
        if (writer) {
            req->execute(writer);
        }
    } else if (std::dynamic_pointer_cast<CheckpointAbortRequest>(request)) {
        // Handled below after the aborted-checkpoint check updates the shared writer state.
    } else {
        LOG_DEBUG("ChannelStateWriteRequestDispatcherImpl::dispatchInternal");
        throw std::invalid_argument("Unknown request type");
    }
    if (isAbortedCheckpoint(request->getCheckpointId())) {
        handleAbortedRequest(request);
    } else if (auto req = std::dynamic_pointer_cast<CheckpointAbortRequest>(request)) {
        handleCheckpointAbortRequest(req);
    }
}

bool ChannelStateWriteRequestDispatcherImpl::isAbortedCheckpoint(long checkpointId)
{
    return checkpointId < ongoingCheckpointId || checkpointId <= maxAbortedCheckpointId;
}

void ChannelStateWriteRequestDispatcherImpl::handleAbortedRequest(std::shared_ptr<ChannelStateWriteRequest> request)
{
    if (request->getCheckpointId() != maxAbortedCheckpointId) {
        request->cancel(
            std::make_exception_ptr(CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED)));
        return;
    }

    if (SubtaskID::Of(request->getJobVertexID(), request->getSubtaskIndex()) == abortedSubtaskID) {
        request->cancel(std::make_exception_ptr(abortedCause));
    } else {
        request->cancel(
            std::make_exception_ptr(CheckpointException(
                CheckpointFailureReason::CHANNEL_STATE_SHARED_STREAM_EXCEPTION,
                std::runtime_error("Aborted by another subtask"))));
    }
}

void ChannelStateWriteRequestDispatcherImpl::handleCheckpointStartRequest(
    std::shared_ptr<CheckpointStartRequest> request)
{
    if (request->getCheckpointId() < ongoingCheckpointId) {
        throw std::logic_error(
            "Checkpoint ID must be incremental. Ongoing: " + std::to_string(ongoingCheckpointId) +
            ", Requested: " + std::to_string(request->getCheckpointId()));
    }

    if (ongoingCheckpointId >= 0 && request->getCheckpointId() > ongoingCheckpointId) {
        failAndClearWriter(
            std::make_exception_ptr(CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED)));
    }

    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    auto it = writers.find(request->getCheckpointId());
    if (it != writers.end()) {
        writer = it->second;
    }
    if (!writer) {
        writer = buildWriter(request);
        if (writer) {
            writers[request->getCheckpointId()] = writer;
        }
        ongoingCheckpointId = request->getCheckpointId();
    }

    writer->RegisterSubtaskResult(
        SubtaskID::Of(request->getJobVertexID(), request->getSubtaskIndex()), request->getTargetResult());
}

void ChannelStateWriteRequestDispatcherImpl::handleCheckpointInProgressRequest(
    std::shared_ptr<CheckpointInProgressRequest> request)
{
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    auto it = writers.find(request->getCheckpointId());
    if (it != writers.end()) {
        writer = it->second;
    }
    if (!writer || ongoingCheckpointId != request->getCheckpointId()) {
        throw std::logic_error("Writer not found for request");
    }
    request->execute(writer);
}

void ChannelStateWriteRequestDispatcherImpl::handleCheckpointAbortRequest(
    std::shared_ptr<CheckpointAbortRequest> request)
{
    if (request->getCheckpointId() > maxAbortedCheckpointId) {
        maxAbortedCheckpointId = request->getCheckpointId();
        abortedCause = request->getCause();
        abortedSubtaskID = SubtaskID::Of(request->getJobVertexID(), request->getSubtaskIndex());
    }

    if (request->getCheckpointId() == ongoingCheckpointId) {
        try {
            std::rethrow_exception(request->getCause());
        } catch (const std::exception& e) {
            failAndClearWriter(request->getJobVertexID(), request->getSubtaskIndex(), request->getCause());
        }
    } else if (request->getCheckpointId() > ongoingCheckpointId) {
        failAndClearWriter(
            std::make_exception_ptr(CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED)));
    }
}

void ChannelStateWriteRequestDispatcherImpl::failAndClearWriter(const std::exception_ptr& e)
{
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    auto it = writers.find(ongoingCheckpointId);
    if (it != writers.end()) {
        writer = it->second;
    }
    if (writer) {
        writer->Fail(e);
        writers.erase(ongoingCheckpointId);
    } else {
        registeredSubtasks.clear();
    }
}

void ChannelStateWriteRequestDispatcherImpl::failAndClearWriter(
    const JobVertexID& jvid, int idx, const std::exception_ptr& e)
{
    std::shared_ptr<ChannelStateCheckpointWriter> writer = nullptr;
    auto it = writers.find(ongoingCheckpointId);
    if (it != writers.end()) {
        writer = it->second;
    }
    if (writer) {
        writer->Fail(jvid, idx, e);
        writers.erase(ongoingCheckpointId);
    } else {
        registeredSubtasks.erase(SubtaskID::Of(jvid, idx));
    }
}

std::shared_ptr<ChannelStateCheckpointWriter> ChannelStateWriteRequestDispatcherImpl::buildWriter(
    std::shared_ptr<CheckpointStartRequest> request)
{
    long id = request->getCheckpointId();
    auto it = writers.find(id);
    if (it != writers.end()) {
        return nullptr;
    }
    SubtaskID sid = SubtaskID::Of(request->getJobVertexID(), request->getSubtaskIndex());
    registeredSubtasks.insert(sid);
    return std::make_shared<ChannelStateCheckpointWriter>(
        registeredSubtasks,
        id,
        getStreamFactoryResolver()->resolveCheckpointStorageLocation(id, request->getLocationReference()),
        serializer,
        [this, id]() { this->RemoveWriter(id); });
}

std::shared_ptr<CheckpointStorageWorkerView> ChannelStateWriteRequestDispatcherImpl::getStreamFactoryResolver()
{
    if (!streamFactoryResolver) {
        streamFactoryResolver = checkpointStorage->createCheckpointStorage(jobID);
    }
    return streamFactoryResolver;
}

} // namespace omnistream
