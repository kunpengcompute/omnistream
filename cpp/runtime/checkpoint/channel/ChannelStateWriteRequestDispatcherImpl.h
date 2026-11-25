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

#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_IMPL_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_IMPL_H

#include <map>
#include <mutex>
#include <set>
#include <exception>
#include <stdexcept>
#include "ChannelStateCheckpointWriter.h"
#include "ChannelStateWriteRequest.h"
#include "ChannelStateWriteRequestDispatcher.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/CheckpointStorageWorkerView.h"
#include "runtime/io/checkpointing/CheckpointException.h"

namespace omnistream {

class ChannelStateWriteRequestDispatcherImpl : public ChannelStateWriteRequestDispatcher {
public:
    ChannelStateWriteRequestDispatcherImpl(
        std::shared_ptr<CheckpointStorage> checkpointStorage,
        const JobIDPOD& jobID,
        std::shared_ptr<ChannelStateSerializer> serializer,
        std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver);

    void dispatch(std::shared_ptr<ChannelStateWriteRequest> request) override;
    void fail(const std::exception_ptr& cause) override;

private:
    std::shared_ptr<CheckpointStorage> checkpointStorage;
    JobIDPOD jobID;
    std::shared_ptr<ChannelStateSerializer> serializer;
    std::set<SubtaskID> registeredSubtasks;
    std::mutex mutex;
    long ongoingCheckpointId;
    long maxAbortedCheckpointId;
    SubtaskID abortedSubtaskID;
    std::exception_ptr abortedCause;
    std::unordered_map<uint64_t, std::shared_ptr<ChannelStateCheckpointWriter>> writers;
    void RemoveWriter(long checkpointId)
    {
        writers.erase(checkpointId);
    }
    std::shared_ptr<CheckpointStorageWorkerView> streamFactoryResolver;

    void dispatchInternal(std::shared_ptr<ChannelStateWriteRequest> request);
    bool isAbortedCheckpoint(long checkpointId);
    void handleAbortedRequest(std::shared_ptr<ChannelStateWriteRequest> request);
    void handleCheckpointStartRequest(std::shared_ptr<CheckpointStartRequest> request);
    void handleCheckpointInProgressRequest(std::shared_ptr<CheckpointInProgressRequest> request);
    void handleCheckpointAbortRequest(std::shared_ptr<CheckpointAbortRequest> request);
    void failAndClearWriter(const std::exception_ptr& e);
    void failAndClearWriter(const JobVertexID& jvid, int idx, const std::exception_ptr& e);
    std::shared_ptr<ChannelStateCheckpointWriter> buildWriter(std::shared_ptr<CheckpointStartRequest> request);
    std::shared_ptr<CheckpointStorageWorkerView> getStreamFactoryResolver();
};

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_IMPL_H
