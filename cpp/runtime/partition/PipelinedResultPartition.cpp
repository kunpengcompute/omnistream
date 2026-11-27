/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// PipelinedResultPartition.cpp
#include "PipelinedResultPartition.h"
#include <iostream>
#include <stdexcept>

#include "ResultPartitionType.h"
#include "event/EndOfData.h"
#include "PipelinedSubpartition.h"

namespace omnistream {

PipelinedResultPartition::PipelinedResultPartition(
    const std::string& owningTaskName,
    int partitionIndex,
    const ResultPartitionIDPOD& partitionId,
    int partitionType,
    std::vector<std::shared_ptr<ResultSubpartition>> subpartitions,
    int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory)
    : BufferWritingResultPartition(
          owningTaskName,
          partitionIndex,
          partitionId,
          checkResultPartitionType(partitionType),
          subpartitions,
          numTargetKeyGroups,
          partitionManager,
          bufferPoolFactory),
      allRecordsProcessedSubpartitions(subpartitions.size(), false),
      numNotAllRecordsProcessedSubpartitions(subpartitions.size()),
      hasNotifiedEndOfUserRecords(false),
      allRecordsProcessedFuture(std::make_shared<CompletableFuture>()),
      consumedSubpartitions(subpartitions.size(), false),
      numberOfUsers(subpartitions.size() + 1)
{
}

PipelinedResultPartition::PipelinedResultPartition(
    const std::string& owningTaskName,
    int partitionIndex,
    const ResultPartitionIDPOD& partitionId,
    int partitionType,
    int numSubpartitions,
    int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
    int taskType)
    : BufferWritingResultPartition(
          owningTaskName,
          partitionIndex,
          partitionId,
          checkResultPartitionType(partitionType),
          numSubpartitions,
          numTargetKeyGroups,
          partitionManager,
          bufferPoolFactory,
          taskType),
      allRecordsProcessedSubpartitions(numSubpartitions, false),
      numNotAllRecordsProcessedSubpartitions(numSubpartitions),
      hasNotifiedEndOfUserRecords(false),
      allRecordsProcessedFuture(std::make_shared<CompletableFuture>()),
      consumedSubpartitions(numSubpartitions, false),
      numberOfUsers(numSubpartitions + 1)
{
    LOG_PART("Body of PipelinedResultPartition constructor");
}

void PipelinedResultPartition::setChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    for (const auto& subpartition : subpartitions_) {
        auto pipelinedSubpartition = std::dynamic_pointer_cast<PipelinedSubpartition>(subpartition);
        if (pipelinedSubpartition) {
            pipelinedSubpartition->setChannelStateWriter(channelStateWriter);
        }
    }
}

// std::shared_ptr<CheckpointedResultSubpartition> PipelinedResultPartition::getCheckpointedSubpartition(int
// subpartitionIndex) {
//     return std::reinterpret_pointer_cast<CheckpointedResultSubpartition>(subpartitions_[subpartitionIndex]);
// }

std::shared_ptr<CheckpointedResultSubpartition> PipelinedResultPartition::getCheckpointedSubpartition(
    int subpartitionIndex)
{
    auto pipelinedSubpartition = std::dynamic_pointer_cast<PipelinedSubpartition>(subpartitions_[subpartitionIndex]);
    if (!pipelinedSubpartition) {
        throw std::runtime_error(
            "subpartition is not a PipelinedSubpartition, subpartitionIndex=" + std::to_string(subpartitionIndex));
    }
    return std::static_pointer_cast<CheckpointedResultSubpartition>(pipelinedSubpartition);
}

// void PipelinedResultPartition::finishReadRecoveredState(bool notifyAndBlockOnCompletion) {
//         for (auto subpartition : subpartitions_) {
//             std::reinterpret_pointer_cast<CheckpointedResultSubpartition>(subpartition)->finishReadRecoveredState(notifyAndBlockOnCompletion);
//         }
// }

void PipelinedResultPartition::finishReadRecoveredState(bool notifyAndBlockOnCompletion)
{
    for (const auto& subpartition : subpartitions_) {
        auto pipelinedSubpartition = std::dynamic_pointer_cast<PipelinedSubpartition>(subpartition);
        if (!pipelinedSubpartition) {
            throw std::runtime_error("subpartition is not a PipelinedSubpartition");
        }
        pipelinedSubpartition->finishReadRecoveredState(notifyAndBlockOnCompletion);
    }
}

/**
 * The pipelined partition releases automatically once all subpartition readers are released.
 * That is because pipelined partitions cannot be consumed multiple times, or reconnect.
 */
void PipelinedResultPartition::OnConsumedSubpartition(int subpartitionIndex)
{
    // std::cout<<"you are in PipelinedResultPartition::OnConsumedSubpartition"<<std::endl;
    decrementNumberOfUsers(subpartitionIndex);
}

void PipelinedResultPartition::decrementNumberOfUsers(int subpartitionIndex)
{
    if (isReleased()) {
        return;
    }

    int remainingUnconsumed;

    {
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        //        if (subpartitionIndex < -1 || subpartitionIndex >= static_cast<int>(consumedSubpartitions.size())) {
        //            throw std::runtime_error("Invalid subpartition index received in consume notification: " +
        //                                     std::to_string(subpartitionIndex));
        //        }

        if (subpartitionIndex != PIPELINED_RESULT_PARTITION_ITSELF) {
            if (consumedSubpartitions[subpartitionIndex]) {
                return;
            }
            consumedSubpartitions[subpartitionIndex] = true;
        }
        remainingUnconsumed = (--numberOfUsers);
    }

    // std::cout << "PipelinedResultPartition: Received consumed notification for subpartition: " << subpartitionIndex
    // <<" "<<remainingUnconsumed<< std::endl;

    if (remainingUnconsumed == 0) {
        partitionManager->onConsumedPartition(shared_from_this());
    } else if (remainingUnconsumed < 0) {
        throw std::runtime_error("Received consume notification even though all subpartitions are already consumed.");
    }
}

void PipelinedResultPartition::flushAll()
{
    flushAllSubpartitions(false);
}

void PipelinedResultPartition::flush(int targetSubpartition)
{
    flushSubpartition(targetSubpartition, false);
}

void PipelinedResultPartition::NotifyEndOfData(StopMode mode)
{
    std::lock_guard<std::recursive_mutex> lockGuard(lock);
    if (!hasNotifiedEndOfUserRecords) {
        broadcastEvent(std::make_shared<EndOfData>(mode), false);
        hasNotifiedEndOfUserRecords = true;
    }
}

std::shared_ptr<CompletableFuture> PipelinedResultPartition::getAllDataProcessedFuture()
{
    return allRecordsProcessedFuture;
}

void PipelinedResultPartition::onSubpartitionAllDataProcessed(int subpartition)
{
    std::lock_guard<std::recursive_mutex> lockGuard(lock);
    if (allRecordsProcessedSubpartitions[subpartition]) {
        return;
    }

    allRecordsProcessedSubpartitions[subpartition] = true;
    numNotAllRecordsProcessedSubpartitions--;

    if (numNotAllRecordsProcessedSubpartitions == 0) {
        allRecordsProcessedFuture->complete();
    }
}

std::string PipelinedResultPartition::toString()
{
    return "PipelinedResultPartition " + partitionId.toString() + " [" + std::to_string(partitionType) + ", " +
           std::to_string(subpartitions_.size()) + " subpartitions, " + std::to_string(numberOfUsers) +
           " pending consumptions]";
}

int PipelinedResultPartition::checkResultPartitionType(int type)
{
    if (type != ResultPartitionType::PIPELINED && type != ResultPartitionType::PIPELINED_BOUNDED &&
        type != ResultPartitionType::PIPELINED_APPROXIMATE) {
        throw std::invalid_argument("Invalid ResultPartitionType for PipelinedResultPartition.");
    }
    return type;
}

void PipelinedResultPartition::close()
{
    decrementNumberOfUsers(PIPELINED_RESULT_PARTITION_ITSELF);
    BufferWritingResultPartition::close();
}

} // namespace omnistream
