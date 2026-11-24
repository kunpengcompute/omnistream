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

#ifndef BUFFER_WRITING_RESULT_PARTITION_H
#define BUFFER_WRITING_RESULT_PARTITION_H

#include <memory>
#include <vector>

#include "ResultPartition.h"
#include "ResultSubpartition.h"

#include "BufferAvailabilityListener.h"
#include "ResultSubpartitionView.h"
#include "checkpoint/channel/ChannelStateWriter.h"

#include <iostream>

#include "metrics/TimerGauge.h"

namespace omnistream {

class BufferWritingResultPartition : public ResultPartition {
public:
    BufferWritingResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        // std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory);
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory);

    BufferWritingResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        int numSubpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
        int taskType);

    ~BufferWritingResultPartition()
    {
        for (auto bufferBuilder : unicastBufferBuilders) {
            if (bufferBuilder) {
                bufferBuilder->close();
                delete bufferBuilder;
            }
        }
        unicastBufferBuilders.clear();
        if (broadcastBufferBuilder) {
            delete broadcastBufferBuilder;
        }
    }

    void setup() override;
    int getNumberOfQueuedBuffers() override;
    int getNumberOfQueuedBuffers(int targetSubpartition) override;
    void emitRecord(void* record, int targetSubpartition) override;
    void broadcastRecord(void* record) override;
    void broadcastEvent(std::shared_ptr<AbstractEvent> event, bool isPriorityEvent) override;
    BufferBuilder* appendUnicastDataForRecordContinuation(void* record, int targetSubpartition);

    std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
        int subpartitionIndex, BufferAvailabilityListener* availabilityListener) override;
    void finish() override;
    void cancel() override;
    void close() override;

    virtual void setSubpartitions(const std::vector<std::shared_ptr<ResultSubpartition>>& subpartitions);

    std::vector<std::shared_ptr<ResultSubpartition>> getAllPartitions();
    void SetChannelStateWriter(const std::shared_ptr<ChannelStateWriter>& channelStateWriter);
    std::shared_ptr<TimerGauge> getBackPressuredTimeMsPerSecond() const;

protected:
    void releaseInternal() override;
    void flushSubpartition(int targetSubpartition, bool finishProducers);
    void flushAllSubpartitions(bool finishProducers);

    // The subpartitions of this partition. At least one.
    std::vector<std::shared_ptr<ResultSubpartition>> subpartitions_;

    // For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be null.
    std::vector<BufferBuilder*> unicastBufferBuilders;

    // For broadcast mode, a single BufferBuilder is shared by all subpartitions
    BufferBuilder* broadcastBufferBuilder = nullptr;

    int64_t totalWrittenBytes;

    void createBroadcastBufferConsumers(std::shared_ptr<ObjectBufferBuilder> buffer, int partialRecordBytes);

    void finishUnicastBufferBuilder(int targetSubpartition);

    void finishUnicastBufferBuilders();

    void finishBroadcastBufferBuilder();

    void ensureUnicastMode();

    void ensureBroadcastMode();
    void SetMetricGroup(std::shared_ptr<AbstractMetricGroup> metricGroup) override;

private:
    BufferBuilder* requestNewUnicastBufferBuilder(int targetSubpartition, uint64_t bytes = 0);

    BufferBuilder* requestNewBroadcastBufferBuilder();

    BufferBuilder* requestNewBufferBuilderFromPool(int targetSubpartition, uint64_t bytes = 0);

    void addToSubpartition(BufferBuilder* buffer, int targetSubpartition, int i);

    void addToSubpartition(
        BufferBuilder* buffer, int targetSubpartition, int partialRecordLength, int minDesirableBufferSize);

    void resizeBuffer(BufferBuilder* buffer, int desirableBufferSize, int minDesirableBufferSize);

    BufferBuilder* appendUnicastDataForNewRecord(void* record, int targetSubpartition);

    void requestMemoryForVectorBatch(int targetSubpartition, uint64_t bytes);

    std::shared_ptr<TimerGauge> hardBackPressuredTimeMsPerSecond = std::make_shared<TimerGauge>();
};

} // namespace omnistream

#endif // BUFFER_WRITING_RESULT_PARTITION_H
