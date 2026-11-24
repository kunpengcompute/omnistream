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

// LocalInputChannel.h
#ifndef LOCALINPUTCHANNEL_H
#define LOCALINPUTCHANNEL_H

#include <memory>
#include <vector>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <partition/BufferAvailabilityListener.h>
#include <partition/ResultPartitionManager.h>
#include "partition/virtual_enable_shared_from_this_base.h"

#include "InputChannel.h"

#include "SingleInputGate.h"
#include "checkpoint/channel/ChannelStateWriter.h"
#include "runtime/io/channelutil/ChannelStatePersister.h"

namespace omnistream {

class LocalInputChannel : public InputChannel, public BufferAvailabilityListener {
public:
    LocalInputChannel(
        std::shared_ptr<SingleInputGate> inputGate,
        int channelIndex,
        ResultPartitionIDPOD partitionId,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        //   std::shared_ptr<TaskEventPublisher> taskEventPublisher,
        int initialBackoff,
        int maxBackoff,
        std::shared_ptr<Counter> numBytesIn,
        std::shared_ptr<Counter> numBuffersIn
        //  std::shared_ptr<ChannelStateWriter> stateWriter
    );

    LocalInputChannel(
        std::shared_ptr<SingleInputGate> inputGate,
        int channelIndex,
        ResultPartitionIDPOD partitionId,
        std::shared_ptr<ResultPartitionManager> _partitionManager,
        // std::shared_ptr<TaskEventPublisher> taskEventPublisher,
        int initialBackoff,
        int maxBackoff,
        std::shared_ptr<Counter> numBytesIn,
        std::shared_ptr<Counter> numBuffersIn,
        std::shared_ptr<ChannelStateWriter> stateWriter);

    void CheckpointStarted(
        const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter) override;
    void CheckpointStopped(long checkpointId) override;
    void ConvertToPriorityEvent(int sequenceNumber) override;
    void notifyDataAvailable() override;
    void notifyPriorityEvent(int prioritySequenceNumber) override;
    void resumeConsumption() override;
    void TimeOutResumeConsumption() override;
    void acknowledgeAllRecordsProcessed() override;
    bool isReleased() override;
    void releaseAllResources() override;
    void announceBufferSize(int newBufferSize) override;
    int getBuffersInUseCount() override;
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    size_t getConsumedBufferSize() const override;
    std::string toString() override;
    std::shared_ptr<ResultSubpartitionView> getSubpartitionView();
    void notifyBufferAvailable(int subpartitionId) override;
    void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) override;
    void SetPersistenceFlag(bool flag) override;
    bool IsNeedPersistence() override;
    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info) override;

public:
    void retriggerSubpartitionRequest(
        std::shared_ptr<std::chrono::steady_clock::time_point> timer, int subpartitionIndex);
    std::shared_ptr<ResultSubpartitionView> checkAndWaitForSubpartitionView();

protected:
    void requestSubpartition(int subpartitionIndex) override;
    std::optional<BufferAndAvailability> getNextBuffer() override;
    void sendTaskEvent(std::shared_ptr<TaskEvent> event) override;

protected:
    static void ReleaseInflightBuffers(std::vector<Buffer*>& buffers);

    std::recursive_mutex requestLock;
    std::shared_ptr<ResultPartitionManager> partitionManager;
    //  std::shared_ptr<TaskEventPublisher> taskEventPublisher;
    std::shared_ptr<ChannelStatePersister> channelStatePersister;
    std::shared_ptr<ResultSubpartitionView> subpartitionView;
    std::vector<Buffer*> inflightBuffers_;
    bool isNeedPersistence_ = false;
    size_t insize = 0;
    size_t outsize = 0;
    size_t startSize_ = 0;
    std::atomic<bool> isReleased_{false};
};

} // namespace omnistream

#endif // LOCALINPUTCHANNEL_H
