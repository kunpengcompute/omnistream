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

#ifndef OMNISTREAM_CHANNEL_STATE_CHECKPOINT_WRITER_H
#define OMNISTREAM_CHANNEL_STATE_CHECKPOINT_WRITER_H

#include <map>
#include <set>
#include <vector>
#include <functional>
#include <stdexcept>
#include "runtime/jobgraph/JobVertexID.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "runtime/state/AbstractChannelStateHandle.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "runtime/state/StreamStateHandle.h"
#include "ChannelStateSerializer.h"
#include "ChannelStatePendingResult.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"

namespace omnistream {

class SubtaskID {
public:
    SubtaskID() : jobVertexID(JobVertexID(-1, -1)), subtaskIndex(-1)
    {
    }

    SubtaskID(const JobVertexID& jvid, int subtaskIndex) : jobVertexID(jvid), subtaskIndex(subtaskIndex)
    {
    }

    int GetSubtaskIndex() const
    {
        return subtaskIndex;
    }

    const JobVertexID& GetJobVertexID() const
    {
        return jobVertexID;
    }

    static SubtaskID Of(const JobVertexID& jvid, int subtaskIdx)
    {
        return SubtaskID(jvid, subtaskIdx);
    }

    bool operator<(const SubtaskID& other) const
    {
        return jobVertexID < other.jobVertexID ||
               (jobVertexID == other.jobVertexID && subtaskIndex < other.subtaskIndex);
    }

    bool operator==(const SubtaskID& other) const
    {
        return jobVertexID == other.jobVertexID && subtaskIndex == other.subtaskIndex;
    }

    std::string ToString() const
    {
        std::ostringstream oss;
        oss << "SubtaskID{jobVertexID=" << jobVertexID.toString() << ", subtaskIndex=" << subtaskIndex << "}";
        return oss.str();
    }

private:
    JobVertexID jobVertexID;
    int subtaskIndex;
};

class ChannelStateCheckpointWriter {
public:
    ChannelStateCheckpointWriter(
        const std::set<SubtaskID>& subtasks,
        int64_t checkpointId,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        std::shared_ptr<ChannelStateSerializer> serializer,
        std::function<void()> onComplete);

    ~ChannelStateCheckpointWriter();

    void RegisterSubtaskResult(
        const SubtaskID& id, std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> result);
    void ReleaseSubtask(const SubtaskID& id);
    void WriteInput(const JobVertexID& jvid, int subtaskIndex, const InputChannelInfo& info, Buffer* buffer);
    void WriteOutput(const JobVertexID& jvid, int subtaskIndex, const ResultSubpartitionInfoPOD& info, Buffer* buffer);
    void CompleteInput(const JobVertexID& jvid, int subtaskIndex);
    void CompleteOutput(const JobVertexID& jvid, int subtaskIndex);
    void Fail(const JobVertexID& jvid, int subtaskIndex, const std::exception_ptr& e);
    void Fail(const std::exception_ptr& e);
    void Reset();
    void Start(
        const JobVertexID& jobVertexID,
        int subtaskIndex,
        std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
        std::shared_ptr<CheckpointStorageLocationReference> locationReference);
    void Abort(const JobVertexID& jobVertexID, int subtaskIndex, const std::exception_ptr& cause);
    void RegisterSubtask(const JobVertexID& jobVertexID, int subtaskIndex);

private:
    int64_t checkpointId;
    std::shared_ptr<ChannelStateSerializer> serializer;
    std::function<void()> onComplete;
    std::map<SubtaskID, ChannelStatePendingResult*> pendingResults;
    std::set<SubtaskID> subtasksToRegister;
    CheckpointStateOutputStream* checkpointStream;
    char* dataStream;
    int64_t streamOffset;
    std::exception_ptr throwable;

    bool IsDone() const;
    void TryFinishResult(ChannelStatePendingResult* pending);
    void FinishWriteAndResult(ChannelStatePendingResult* pending);
    void failResultAndCloseStream(const std::exception_ptr& e);

    template <typename K>
    void Write(
        std::map<K, typename AbstractChannelStateHandle<K>::StateContentMetaInfo>& offsets,
        const K& key,
        Buffer* buffer,
        bool precondition,
        const std::string& action)
    {
        LOG_DEBUG("ChannelStateCheckpointWriter write start! action: " << action);
        if (!precondition) {
            LOG_DEBUG("void Write.");
            throw std::logic_error("Precondition failed for " + action);
        }
        std::vector<char> serializedData = serializer->SerializeData(buffer);
        int64_t offset = streamOffset;
        checkpointStream->Write(serializedData.data(), serializedData.size());
        int64_t size = static_cast<int64_t>(serializedData.size());
        streamOffset += size;
        auto it = offsets.find(key);
        if (it != offsets.end()) {
            it->second.WithDataAdded(offset, size);
        } else {
            offsets[key].WithDataAdded(offset, size);
        }
    }

    ChannelStatePendingResult* GetChannelStatePendingResult(const JobVertexID& jvid, int subtaskIndex);
};
} // namespace omnistream

namespace std {
template <>
struct hash<omnistream::SubtaskID> {
    size_t operator()(const omnistream::SubtaskID& id) const
    {
        size_t h1 = hash<omnistream::JobVertexID>()(id.GetJobVertexID());
        size_t h2 = hash<int>()(id.GetSubtaskIndex());
        return h1 ^ (h2 << 1);
    }
};
} // namespace std

#endif // OMNISTREAM_CHANNEL_STATE_CHECKPOINT_WRITER_H
