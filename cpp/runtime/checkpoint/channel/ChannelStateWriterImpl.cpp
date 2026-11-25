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

#include "ChannelStateWriterImpl.h"
#include "CheckpointBufferUtils.h"

namespace omnistream {
ChannelStateWriterImpl::ChannelStateWriterImpl(
    const JobVertexID& jobVertexID,
    const std::string& taskName,
    int subtaskIndex,
    std::shared_ptr<CheckpointStorage> checkpointStorage,
    std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver,
    int maxCheckpoints,
    int maxSubtasksPerChannelStateFile)
    : jobVertexID_(jobVertexID),
      taskName_(taskName),
      subtaskIndex_(subtaskIndex),
      maxCheckpoints_(maxCheckpoints),
      wasClosed_(false)
{
    serializer_ = std::make_shared<ChannelStateSerializerImpl>();
    auto dispatcher = std::make_shared<ChannelStateWriteRequestDispatcherImpl>(
        checkpointStorage, JobIDPOD{}, serializer_, streamFactoryResolver);
    executor_ = std::make_shared<ChannelStateWriteRequestExecutorImpl>(dispatcher);
}

void ChannelStateWriterImpl::Start(long checkpointId, const CheckpointOptions& options)
{
    std::lock_guard<std::mutex> lock(resultsMutex_);
    if (results_.find(checkpointId) != results_.end()) {
        return;
    }

    if (results_.size() >= static_cast<size_t>(maxCheckpoints_)) {
        throw std::runtime_error(taskName_ + " exceeded max checkpoints");
    }
    auto channelStateWriteResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    channelStateWriteResult->setIsNeedsChannelState(true);
    results_.emplace(checkpointId, channelStateWriteResult);
    auto& result = results_.at(checkpointId);

    enqueue(
        ChannelStateWriteRequest::start(jobVertexID_, subtaskIndex_, checkpointId, channelStateWriteResult, "Start"),
        false);
}

void ChannelStateWriterImpl::AddInputData(
    long checkpointId, const InputChannelInfo& info, int startSeqNum, std::vector<Buffer*> data)
{
    try {
        validateCheckpointId(checkpointId);
        enqueue(ChannelStateWriteRequest::writeInput(jobVertexID_, subtaskIndex_, checkpointId, info, data), false);
    } catch (...) {
        for (auto* buffer : data) {
            if (buffer != nullptr) {
                ReleaseCheckpointBuffer(buffer);
            }
        }
        throw;
    }
}

void ChannelStateWriterImpl::AddOutputData(
    long checkpointId, const ResultSubpartitionInfoPOD& info, int startSeqNum, std::vector<Buffer*>& data)
{
    std::vector<Buffer*> ownedData;
    ownedData.swap(data);
    try {
        validateCheckpointId(checkpointId);
        enqueue(
            ChannelStateWriteRequest::writeOutput(jobVertexID_, subtaskIndex_, checkpointId, info, ownedData), false);
    } catch (...) {
        for (auto* buffer : ownedData) {
            if (buffer != nullptr) {
                ReleaseCheckpointBuffer(buffer);
            }
        }
        throw;
    }
}

void ChannelStateWriterImpl::AddOutputDataFuture(
    long checkpointId,
    const ResultSubpartitionInfoPOD& info,
    int startSeqNum,
    std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> data)
{
    validateCheckpointId(checkpointId);
    enqueue(ChannelStateWriteRequest::writeOutputFuture(jobVertexID_, subtaskIndex_, checkpointId, info, data), false);
}

void ChannelStateWriterImpl::FinishInput(long checkpointId)
{
    validateCheckpointId(checkpointId);
    enqueue(ChannelStateWriteRequest::completeInput(jobVertexID_, subtaskIndex_, checkpointId), false);
}

void ChannelStateWriterImpl::FinishOutput(long checkpointId)
{
    validateCheckpointId(checkpointId);
    enqueue(ChannelStateWriteRequest::completeOutput(jobVertexID_, subtaskIndex_, checkpointId), false);
}

void ChannelStateWriterImpl::Abort(long checkpointId, const std::exception_ptr& cause, bool cleanup)
{
    // Priority Abort
    enqueue(ChannelStateWriteRequest::terminate(jobVertexID_, subtaskIndex_, checkpointId, cause), true);

    // Normal Abort
    enqueue(ChannelStateWriteRequest::terminate(jobVertexID_, subtaskIndex_, checkpointId, cause), false);

    if (cleanup) {
        std::lock_guard<std::mutex> lock(resultsMutex_);
        results_.erase(checkpointId);
    }
}

std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> ChannelStateWriterImpl::GetAndRemoveWriteResult(
    long checkpointId)
{
    std::lock_guard<std::mutex> lock(resultsMutex_);
    auto it = results_.find(checkpointId);
    if (it == results_.end()) {
        throw std::invalid_argument("Checkpoint result not found");
    }
    auto result = it->second;
    results_.erase(it);
    return result;
}

void ChannelStateWriterImpl::Close()
{
    if (wasClosed_.exchange(true)) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(resultsMutex_);
        results_.clear();
    }

    enqueue(ChannelStateWriteRequest::releaseSubtask(jobVertexID_, subtaskIndex_), false);
}

void ChannelStateWriterImpl::open()
{
    executor_->start();
}

void ChannelStateWriterImpl::validateCheckpointId(long checkpointId)
{
}

void ChannelStateWriterImpl::enqueue(std::shared_ptr<ChannelStateWriteRequest> request, bool priority)
{
    LOG_DEBUG(" enqueue " << request->getName() << " checkpointid " << request->getCheckpointId());
    if (priority) {
        executor_->submitPriority(request);
    } else {
        executor_->submit(request);
    }
}

} // namespace omnistream
