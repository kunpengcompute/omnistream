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

#include <cstdlib>
#include <memory>

#include "ChannelStateCheckpointWriter.h"
#include "CheckpointBufferUtils.h"
#include "ChannelStateWriter.h"
#include "state/filesystem/FileStateHandle.h"
namespace omnistream {
namespace {
class BufferRecycleGuard {
public:
    explicit BufferRecycleGuard(Buffer* buffer) : buffer_(buffer)
    {
    }

    ~BufferRecycleGuard()
    {
        if (buffer_ == nullptr) {
            return;
        }
        try {
            ReleaseCheckpointBuffer(buffer_);
        } catch (const std::exception& e) {
            INFO_RELEASE("ERROR: Failed to recycle a channel-state input buffer: " << e.what());
        } catch (...) {
            INFO_RELEASE("ERROR: Failed to recycle a channel-state input buffer due to an unknown exception");
        }
    }

private:
    Buffer* buffer_;
};
} // namespace

ChannelStateCheckpointWriter::ChannelStateCheckpointWriter(
    const std::set<SubtaskID>& subtasks,
    int64_t checkpointId,
    std::shared_ptr<CheckpointStreamFactory> streamFactory,
    std::shared_ptr<ChannelStateSerializer> serializer,
    std::function<void()> onComplete)
    : checkpointId(checkpointId),
      serializer(serializer),
      onComplete(onComplete),
      checkpointStream(nullptr),
      dataStream(nullptr),
      streamOffset(0)
{
    if (subtasks.empty()) {
        throw std::invalid_argument("subtasks cannot be empty");
    }
    subtasksToRegister = subtasks;
    std::unique_ptr<CheckpointStateOutputStream> checkpointStreamGuard(
        streamFactory->createCheckpointStateOutputStream(CheckpointedStateScope::EXCLUSIVE));
    size_t memSize = static_cast<size_t>(serializer->GetHeaderLength());
    std::unique_ptr<char, void (*)(void*)> dataStreamGuard(static_cast<char*>(std::malloc(memSize)), std::free);
    if (dataStreamGuard == nullptr) {
        INFO_RELEASE("Exception: Failed to allocate channel state data stream.");
        throw std::runtime_error("Failed to allocate channel state data stream");
    }
    (void)memset_s(dataStreamGuard.get(), memSize, 0, memSize);
    serializer->WriteHeader(dataStreamGuard.get());
    checkpointStream = checkpointStreamGuard.get();
    checkpointStream->Write(dataStreamGuard.get(), memSize);
    streamOffset = static_cast<int64_t>(memSize);
    checkpointStreamGuard.release();
    dataStream = dataStreamGuard.release();
}

ChannelStateCheckpointWriter::~ChannelStateCheckpointWriter()
{
    std::free(dataStream);
    delete checkpointStream;
    for (auto& kv : pendingResults) {
        delete kv.second;
    }
}

void ChannelStateCheckpointWriter::RegisterSubtaskResult(
    const SubtaskID& id, std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> result)
{
    if (IsDone()) {
        throw std::logic_error("Write is already done");
    }
    if (pendingResults.count(id)) {
        throw std::logic_error("Subtask already registered");
    }
    subtasksToRegister.erase(id);
    pendingResults[id] = new ChannelStatePendingResult(id.GetSubtaskIndex(), checkpointId, result, serializer);
}

void ChannelStateCheckpointWriter::ReleaseSubtask(const SubtaskID& id)
{
    subtasksToRegister.erase(id);
    TryFinishResult(nullptr);
}

void ChannelStateCheckpointWriter::WriteInput(
    const JobVertexID& jvid, int subtaskIndex, const InputChannelInfo& info, Buffer* buffer)
{
    BufferRecycleGuard recycleGuard(buffer);
    if (IsDone()) {
        return;
    }

    ChannelStatePendingResult* pending = GetChannelStatePendingResult(jvid, subtaskIndex);
    Write(pending->GetInputChannelOffsets(), info, buffer, !pending->IsAllInputsReceived(), "ChannelState#WriteInput");
}

void ChannelStateCheckpointWriter::WriteOutput(
    const JobVertexID& jvid, int subtaskIndex, const ResultSubpartitionInfoPOD& info, Buffer* buffer)
{
    BufferRecycleGuard recycleGuard(buffer);
    if (IsDone()) {
        return;
    }

    ChannelStatePendingResult* pending = GetChannelStatePendingResult(jvid, subtaskIndex);
    Write(
        pending->GetResultSubpartitionOffsets(),
        info,
        buffer,
        !pending->IsAllOutputsReceived(),
        "ChannelState#WriteOutput");
}

void ChannelStateCheckpointWriter::CompleteInput(const JobVertexID& jvid, int subtaskIndex)
{
    if (IsDone()) {
        return;
    }

    ChannelStatePendingResult* pending = GetChannelStatePendingResult(jvid, subtaskIndex);
    pending->CompleteInput();
    TryFinishResult(pending);
}

void ChannelStateCheckpointWriter::CompleteOutput(const JobVertexID& jvid, int subtaskIndex)
{
    if (IsDone()) {
        return;
    }

    ChannelStatePendingResult* pending = GetChannelStatePendingResult(jvid, subtaskIndex);
    pending->CompleteOutput();
    TryFinishResult(pending);
}

void ChannelStateCheckpointWriter::Fail(const JobVertexID& jvid, int subtaskIndex, const std::exception_ptr& e)
{
    if (IsDone()) {
        return;
    }
    throwable = e;
    auto id = SubtaskID::Of(jvid, subtaskIndex);
    if (pendingResults.count(id)) {
        pendingResults[id]->Fail(e);
    }
    failResultAndCloseStream(e);
}

void ChannelStateCheckpointWriter::Fail(const std::exception_ptr& e)
{
    if (IsDone()) {
        return;
    }
    throwable = e;
    failResultAndCloseStream(e);
}
void ChannelStateCheckpointWriter::Reset()
{
}
void ChannelStateCheckpointWriter::Start(
    const JobVertexID& jobVertexID,
    int subtaskIndex,
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
    std::shared_ptr<CheckpointStorageLocationReference> locationReference)
{
    SubtaskID id = SubtaskID::Of(jobVertexID, subtaskIndex);
    RegisterSubtaskResult(id, targetResult);
}

void ChannelStateCheckpointWriter::Abort(
    const JobVertexID& jobVertexID, int subtaskIndex, const std::exception_ptr& cause)
{
    Fail(jobVertexID, subtaskIndex, cause);
}

void ChannelStateCheckpointWriter::RegisterSubtask(const JobVertexID& jobVertexID, int subtaskIndex)
{
}

bool ChannelStateCheckpointWriter::IsDone() const
{
    if (throwable) {
        return true;
    }
    for (auto& kv : pendingResults)
        if (kv.second->IsDone()) {
            return true;
        }
    return false;
}

void ChannelStateCheckpointWriter::TryFinishResult(ChannelStatePendingResult* pending)
{
    if (!subtasksToRegister.empty()) {
        return;
    }

    for (auto& kv : pendingResults) {
        if (!kv.second->IsAllInputsReceived() || !kv.second->IsAllOutputsReceived()) {
            return;
        }
    }

    if (IsDone()) {
        onComplete();
    } else {
        FinishWriteAndResult(pending);
        onComplete();
    }
}

void ChannelStateCheckpointWriter::FinishWriteAndResult(ChannelStatePendingResult* pending)
{
    std::shared_ptr<StreamStateHandle> handle = nullptr;

    checkpointStream->Flush();
    handle = checkpointStream->CloseAndGetHandle(dataStream);

    if (handle) {
        auto channel = pending->GetInputChannelOffsets();
        using InputChannelStateHandleVecPtr = std::shared_ptr<std::vector<std::shared_ptr<InputChannelStateHandle>>>;
        InputChannelStateHandleVecPtr channelHandles =
            std::make_shared<std::vector<std::shared_ptr<InputChannelStateHandle>>>();
        for (auto it = channel.begin(); it != channel.end(); ++it) {
            auto inputChannelStateHandle =
                std::make_shared<InputChannelStateHandle>(pending->GetSubtaskIndex(), it->first, handle, it->second);
            channelHandles->push_back(inputChannelStateHandle);
        }
        pending->GetResult()->GetInputChannelStateHandles()->Complete(channelHandles);
        auto parition = pending->GetResultSubpartitionOffsets();
        using ResultSubpartitionStateVecPtr =
            std::shared_ptr<std::vector<std::shared_ptr<ResultSubpartitionStateHandle>>>;
        ResultSubpartitionStateVecPtr paritionHandles =
            std::make_shared<std::vector<std::shared_ptr<ResultSubpartitionStateHandle>>>();
        for (auto it = parition.begin(); it != parition.end(); ++it) {
            auto resultSubpartitionStateHandle = std::make_shared<ResultSubpartitionStateHandle>(
                pending->GetSubtaskIndex(), it->first, handle, it->second);
            paritionHandles->push_back(resultSubpartitionStateHandle);
        }
        pending->GetResult()->GetResultSubpartitionStateHandles()->Complete(paritionHandles);
    } else {
        throw std::runtime_error("Failed to close channel state stream: null state handle");
    }
}

void ChannelStateCheckpointWriter::failResultAndCloseStream(const std::exception_ptr& e)
{
    throwable = e;
    for (auto& kv : pendingResults) kv.second->Fail(e);
    try {
        checkpointStream->Close();
    } catch (const std::exception& ex) {
        INFO_RELEASE("ERROR: Failed to close checkpointStream: " << ex.what());
    }
}

ChannelStatePendingResult* ChannelStateCheckpointWriter::GetChannelStatePendingResult(
    const JobVertexID& jvid, int subtaskIndex)
{
    auto id = SubtaskID::Of(jvid, subtaskIndex);
    auto it = pendingResults.find(id);
    if (it == pendingResults.end()) {
        LOG_DEBUG("ChannelStateCheckpointWriter::GetChannelStatePendingResult");
        throw std::runtime_error("Subtask not registered: " + id.ToString());
    }
    return it->second;
}

} // namespace omnistream
