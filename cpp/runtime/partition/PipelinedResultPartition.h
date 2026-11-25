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

// PipelinedResultPartition.h
#ifndef PIPELINED_RESULT_PARTITION_H
#define PIPELINED_RESULT_PARTITION_H

#include <memory>
#include <vector>
#include <mutex>
#include <future>
#include <buffer/ObjectBufferPool.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>

#include "BufferWritingResultPartition.h"
#include "ResultPartition.h"
#include "ResultSubpartition.h"

#include "ResultPartitionManager.h"
#include "CheckpointedResultPartition.h"
#include "ChannelStateHolder.h"
namespace omnistream {

class PipelinedResultPartition : public BufferWritingResultPartition,
                                 public CheckpointedResultPartition,
                                 public ChannelStateHolder {
public:
    PipelinedResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        // std::shared_ptr<Supplier<ObjectBufferPool>> bufferPool);
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory);

    PipelinedResultPartition(
        const std::string& owningTaskName,
        int partitionIndex,
        const ResultPartitionIDPOD& partitionId,
        int partitionType,
        int numSubpartitions,
        int numTargetKeyGroups,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
        int taskType);

    void flushAll() override;
    void flush(int targetSubpartition) override;
    void NotifyEndOfData(StopMode mode) override;
    std::shared_ptr<CompletableFuture> getAllDataProcessedFuture() override;
    void onSubpartitionAllDataProcessed(int subpartition) override;

    void close() override;
    std::string toString() override;
    void OnConsumedSubpartition(int subpartitionIndex) override;

    void setChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) override;
    std::shared_ptr<CheckpointedResultSubpartition> getCheckpointedSubpartition(int subpartitionIndex) override;
    void finishReadRecoveredState(bool notifyAndBlockOnCompletion) override;

private:
    void decrementNumberOfUsers(int subpartitionIndex);
    static int checkResultPartitionType(int type);

    static const int PIPELINED_RESULT_PARTITION_ITSELF = -42;
    std::recursive_mutex lock;
    std::vector<bool> allRecordsProcessedSubpartitions;
    int numNotAllRecordsProcessedSubpartitions;
    bool hasNotifiedEndOfUserRecords = false;
    std::shared_ptr<CompletableFuture> allRecordsProcessedFuture;
    std::vector<bool> consumedSubpartitions;
    int numberOfUsers;
};

} // namespace omnistream

#endif // PIPELINED_RESULT_PARTITION_H
