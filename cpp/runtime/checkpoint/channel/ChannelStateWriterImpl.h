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

#ifndef OMNISTREAM_CHANNEL_STATE_WRITER_IMPL_H
#define OMNISTREAM_CHANNEL_STATE_WRITER_IMPL_H

#include <map>
#include <mutex>
#include <atomic>
#include <memory>
#include <stdexcept>
#include "ChannelStateWriter.h"
#include "ChannelStateWriteRequest.h"
#include "ChannelStateWriteRequestExecutor.h"
#include "ChannelStateWriteRequestExecutorImpl.h"
#include "ChannelStateWriteRequestDispatcherImpl.h"
#include "runtime/buffer/ObjectBuffer.h"

namespace omnistream {

class ChannelStateWriterImpl : public ChannelStateWriter {
public:
    ChannelStateWriterImpl() = default;
    ChannelStateWriterImpl(
        const JobVertexID& jobVertexID,
        const std::string& taskName,
        int subtaskIndex,
        std::shared_ptr<CheckpointStorage> checkpointStorage,
        std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver,
        int maxCheckpoints = 1000,
        int maxSubtasksPerChannelStateFile = 10);

    void Start(long checkpointId, const CheckpointOptions& options) override;
    void AddInputData(
        long checkpointId, const InputChannelInfo& info, int startSeqNum, std::vector<Buffer*> data) override;
    void AddOutputData(
        long checkpointId, const ResultSubpartitionInfoPOD& info, int startSeqNum, std::vector<Buffer*>& data) override;
    void AddOutputDataFuture(
        long checkpointId,
        const ResultSubpartitionInfoPOD& info,
        int startSeqNum,
        std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> data) override;
    void FinishInput(long checkpointId) override;
    void FinishOutput(long checkpointId) override;
    void Abort(long checkpointId, const std::exception_ptr& cause, bool cleanup) override;
    std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> GetAndRemoveWriteResult(long checkpointId) override;
    void Close();
    void open() override;

private:
    JobVertexID jobVertexID_;
    std::string taskName_;
    int subtaskIndex_;
    int maxCheckpoints_;
    std::atomic<bool> wasClosed_;
    std::mutex resultsMutex_;
    std::shared_ptr<ChannelStateWriteRequestExecutor> executor_;
    std::map<long, std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult>> results_;
    std::shared_ptr<ChannelStateSerializer> serializer_;

    void validateCheckpointId(long checkpointId);
    void enqueue(std::shared_ptr<ChannelStateWriteRequest> request, bool priority);
};

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITER_IMPL_H
