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

#pragma once
#include <atomic>
#include "LocalInputChannel.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "table/utils/BinaryRowDataSerializationUtils.h"
#include <optional>
#include <memory>
#include <queue>

#include "RemoteDataFetcherBridge.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"

namespace omnistream {
class RemoteInputChannel : public LocalInputChannel {
public:
    RemoteInputChannel(
        std::shared_ptr<SingleInputGate> inputGate,
        int channelIndex,
        ResultPartitionIDPOD partitionId,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        int initialBackoff,
        int maxBackoff,
        int networkBuffersPerChannel,
        std::shared_ptr<Counter> numBytesIn,
        std::shared_ptr<Counter> numBuffersIn,
        std::shared_ptr<ChannelStateWriter> stateWriter);
    void requestSubpartition(int subpartitionIndex) override;
    void notifyRemoteDataAvailableForVectorBatch(
        long bufferAddress,
        int bufferLength,
        int sequenceNumber,
        const std::shared_ptr<OriginalNetworkBufferRecycler>& originalNetworkBufferRecycle);
    std::optional<BufferAndAvailability> getNextBuffer() override;
    std::shared_ptr<ObjectSegment> DoDataDeserializationResult(uint8_t*& buffer);
    void notifyRemoteDataAvailableForNetworkBuffer(
        long bufferAddress,
        int bufferLength,
        int readIndex,
        int sequenceNumber,
        std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler,
        bool isBuffer,
        int bufferType);

    void SetRemoteDataFetcherBridge(std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);
    void releaseAllResources() override;
    void resumeConsumption() override;
    void TimeOutResumeConsumption() override;
    void CheckpointStarted(
        const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter) override;
    void CheckpointStopped(long checkpointId) override;
    std::vector<Buffer*> GetInflightBuffersUnsafe(long checkpointId);
    std::vector<Buffer*> GetInflightVectorBatchBuffersUnsafe(long checkpointId);
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    int unsynchronizedGetSizeOfQueuedBuffers() override;

    void ResetLastBarrier()
    {
        lastBarrierId_ = 1;
    }

    void SetPersistenceFlag(bool flag)
    {
        isNeedPersistence_ = flag;
    }

    bool IsNeedPersistence() override;

    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info);

private:
    std::queue<Buffer*> dataQueue;
    Buffer* delayData = nullptr;
    int lastSequenceNumber = 0;
    int expectSequenceNumber = 0;
    int initialCredit;
    std::recursive_mutex queueMutex;
    std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge;
    long lastBarrierId_ = -1;
    size_t insize = 0;
    size_t outsize = 0;
    size_t startSize_ = 0;
    bool isNeedPersistence_ = false;
    std::atomic_bool isBlockedByCheckpoint_{false};
    bool isNeedExpansion = false;
    std::vector<Buffer*> inflightBuffers_;
    int taskType = 0;
};
}; // namespace omnistream
