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

// PipelinedSubpartition.h
#ifndef PIPELINEDSUBPARTITION_H
#define PIPELINEDSUBPARTITION_H

#include <deque>
#include <memory>
#include <mutex>
#include <vector>
#include <string>

#include "PipelinedSubpartitionView.h"
#include "PrioritizedDeque.h"
#include "ResultSubpartition.h"
#include "buffer/BufferConsumerWithPartialRecordLength.h"
#include "CheckpointedResultSubpartition.h"
#include "ChannelStateHolder.h"
#include "checkpoint/channel/ChannelStateWriter.h"

namespace omnistream {

class PipelinedSubpartition : public ResultSubpartition,
                              public CheckpointedResultSubpartition,
                              public ChannelStateHolder,
                              public std::enable_shared_from_this<PipelinedSubpartition> {
public:
    PipelinedSubpartition(int index, int receiverExclusiveBuffersPerChannel, std::shared_ptr<ResultPartition> parent);
    ~PipelinedSubpartition() override;

    // int add(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength) override;
    const ResultSubpartitionInfoPOD& getSubpartitionInfo() override;
    BufferBuilder* requestBufferBuilderBlocking() override;
    void addRecovered(std::shared_ptr<BufferConsumer> bufferConsumer) override;
    void finishReadRecoveredState(bool notifyAndBlockOnCompletion) override;

    void setChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) override;

    void alignedBarrierTimeout(long checkpointId) override;
    void abortCheckpoint(long checkpointId, std::optional<std::exception_ptr> throwable) override;

    int add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength) override;

    void finish() override;
    void release() override;
    BufferAndBacklog* pollBuffer();
    // Convert the announced timeoutable aligned barrier in this subpartition into a priority event
    // so it can overtake buffered data when alignment times out.
    void ConvertToPriorityEvent(int announcedSequenceNumber);
    void resumeConsumption();
    void acknowledgeAllDataProcessed();
    bool isReleased() override;

    std::shared_ptr<ResultSubpartitionView> createReadView(BufferAvailabilityListener* availabilityListener) override;
    AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable);
    int getNumberOfQueuedBuffers() override;
    void bufferSize(int desirableNewBufferSize) override;
    std::string toString() override;
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    void flush() override;
    long getTotalNumberOfBuffersUnsafe() override;
    long getTotalNumberOfBytesUnsafe() override;
    void decreaseBuffersInBacklogUnsafe(bool isBuffer);
    void increaseBuffersInBacklog(std::shared_ptr<BufferConsumer> buffer);
    // std::shared_ptr<VectorBatchBuffer> buildSliceBuffer(std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength>
    // buffer);
    Buffer* buildSliceBuffer(
        std::shared_ptr<BufferConsumerWithPartialRecordLength> bufferConsumerWithPartialRecordLength);
    // std::shared_ptr<ObjectBufferConsumerWithPartialRecordLength> getNextBuffer();
    std::shared_ptr<BufferConsumerWithPartialRecordLength> getNextBuffer();

    int getBuffersInBacklogUnsafe() const override;

private:
    int receiverExclusiveBuffersPerChannel;
    // PrioritizedDeque<ObjectBufferConsumerWithPartialRecordLength> buffers;
    PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers;
    std::recursive_mutex buffersMutex;
    std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> channelStateFuture_;
    long channelStateCheckpointId_ = 0;

    int buffersInBacklog;
    std::shared_ptr<PipelinedSubpartitionView> readView;
    bool isFinished;
    bool flushRequested;
    volatile bool isReleased_;
    long totalNumberOfBuffers;
    long totalNumberOfBytes;
    int bufferSize_ = INT_MAX;
    std::shared_ptr<CompletableFuture> channelStateFuture;
    long channelStateCheckpointId;
    bool isBlocked;
    int sequenceNumber;
    std::shared_ptr<ChannelStateWriter> channelStateWriter_;

    int add(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength, bool finish);
    bool addBuffer(std::shared_ptr<BufferConsumer> bufferConsumer, int partialRecordLength);
    std::shared_ptr<CheckpointBarrier> ParseCheckpointBarrier(const std::shared_ptr<BufferConsumer>& bufferConsumer);
    bool ProcessPriorityBuffer(
        std::shared_ptr<BufferConsumer> bufferConsumer,
        int partialRecordLength,
        std::shared_ptr<CheckpointBarrier> barrier);
    void ProcessTimeoutableCheckpointBarrier(
        std::shared_ptr<BufferConsumer> bufferConsumer, std::shared_ptr<CheckpointBarrier> barrier);
    std::shared_ptr<CheckpointBarrier> ParseAndCheckTimeoutableCheckpointBarrier(
        const std::shared_ptr<BufferConsumer>& bufferConsumer, std::shared_ptr<CheckpointBarrier> barrier);
    std::shared_ptr<CompletableFutureV2<std::vector<Buffer*>>> CreateChannelStateFuture(long checkpointId);
    void CompleteChannelStateFuture(std::vector<Buffer*>& channelResult, std::exception_ptr e);

    bool isDataAvailableUnsafe();
    ObjectBufferDataType getNextBufferTypeUnsafe();

    bool shouldNotifyDataAvailable();
    void notifyDataAvailable();
    void notifyPriorityEvent(int prioritySequenceNumber);
    int getNumberOfFinishedBuffers();
};

} // namespace omnistream

#endif // PIPELINEDSUBPARTITION_H
