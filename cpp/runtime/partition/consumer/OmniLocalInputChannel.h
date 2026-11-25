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

#ifndef OMNILOCALINPUTCHANNEL_H
#define OMNILOCALINPUTCHANNEL_H
#include <atomic>
#include "LocalInputChannel.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"
#include "runtime/state/bridge/OmniLocalInputChannelBridge.h"

namespace omnistream {
class OmniLocalInputChannel : public LocalInputChannel {
public:
    OmniLocalInputChannel(
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

    void notifyOriginalDataAvailable(
        long bufferAddress,
        int bufferLength,
        int readIndex,
        int sequenceNumber,
        int memorySegmentOffset,
        int bufferType);

    std::optional<BufferAndAvailability> getNextBuffer() override;
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    int unsynchronizedGetSizeOfQueuedBuffers() override;
    size_t getConsumedBufferSize() const override;

    std::shared_ptr<ObjectSegment> DoDataDeserializationResult(uint8_t*& buffer, int bufferLength);

    long GetRecycleBufferAddress();

    void releaseAllResources() override;

    void resumeConsumption() override;

    void TimeOutResumeConsumption() override;

    void SetOmniLocalInputChannelBridge(std::shared_ptr<OmniLocalInputChannelBridge> omniLocalInputChannelBridge);

    void CheckpointStarted(const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter);

    void CheckpointStopped(long checkpointId);

    std::vector<Buffer*> GetInflightBuffersUnsafe(long checkpointId);

    void SetPersistenceFlag(bool flag)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        isNeedPersistence_ = flag;
    }
    bool IsNeedPersistence()
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (startSize_ != 0) {
            return outsize < startSize_;
        }
        return true;
    }
    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info);

private:
    size_t insize = 0;
    size_t outsize = 0;
    size_t startSize_ = 0;
    bool isNeedPersistence_ = false;
    std::atomic_bool isBlockedByCheckpoint_{false};
    int expectSequenceNumber = 0;
    int initialCredit;
    std::recursive_mutex queueMutex;
    std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler_;
    std::queue<std::shared_ptr<BufferAndAvailability>> dataQueue;
    std::shared_ptr<OmniLocalInputChannelBridge> omniLocalInputChannelBridge;
    std::vector<Buffer*> inflightBuffers_;
};
} // namespace omnistream
#endif // OMNILOCALINPUTCHANNEL_H
