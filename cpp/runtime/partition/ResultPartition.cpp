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

#include "ResultPartition.h"
#include <iostream>
#include <buffer/ObjectBufferPoolFactory.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include <partition/ResultPartitionManager.h>

namespace omnistream {

const std::string ResultPartition::LOG_NAME = "ResultPartition";

ResultPartition::ResultPartition(
    const std::string& owningTaskName,
    int partitionIndex,
    const ResultPartitionIDPOD& partitionId,
    int partitionType,
    int numSubpartitions,
    int numTargetKeyGroups,
    std::shared_ptr<ResultPartitionManager> partitionManager,
    std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
    int taskType)
    : owningTaskName(owningTaskName),
      partitionIndex(partitionIndex),
      partitionId(partitionId),
      partitionType(partitionType),
      partitionManager(partitionManager),
      numSubpartitions(numSubpartitions),
      numTargetKeyGroups(numTargetKeyGroups),
      bufferPoolFactory_(bufferPoolFactory),
      taskType(taskType)
{
    LOG_PART("Inside constructor");
}

void ResultPartition::setup()
{
    if (bufferPool != nullptr) {
        THROW_RUNTIME_ERROR("Bug in result partition setup logic: Already registered buffer pool.");
    }

    LOG_PART("Before get buffer pool");
    bufferPool = bufferPoolFactory_->get();
    LOG_PART("before register partition");
    this->partitionManager->registerResultPartition(shared_from_this());

    LOG_PART(" after register partition");
}

std::string ResultPartition::getOwningTaskName() const
{
    return owningTaskName;
};

ResultPartitionIDPOD ResultPartition::getPartitionId()
{
    return partitionId;
}

int ResultPartition::getPartitionIndex() const
{
    return partitionIndex;
}

int ResultPartition::getNumberOfSubpartitions()
{
    return numSubpartitions;
}

std::shared_ptr<BufferPool> ResultPartition::getBufferPool()
{
    return bufferPool;
}

int ResultPartition::getPartitionType() const
{
    return partitionType;
}

std::shared_ptr<CompletableFuture> ResultPartition::getAllDataProcessedFuture()
{
    throw std::runtime_error("UnsupportedOperationException");
}

void ResultPartition::onSubpartitionAllDataProcessed(int subpartition)
{
}

void ResultPartition::finish()
{
    checkInProduceState();
    isFinished_ = true;
}

bool ResultPartition::isFinished()
{
    return isFinished_;
}

void ResultPartition::release()
{
    release(nullptr);
}

void ResultPartition::release(std::optional<std::exception_ptr> cause)
{
    bool expected = false;
    if (isReleased_.compare_exchange_strong(expected, true)) {
        std::cout << owningTaskName << ": Releasing " << toString() << std::endl;

        if (cause != nullptr) {
            this->cause = cause;
        }

        releaseInternal();
    }
}
void ResultPartition::closeBufferPool()
{
    if (bufferPool != nullptr) {
        bufferPool->lazyDestroy();
    }
}
void ResultPartition::close()
{
    this->closeBufferPool();
}

void ResultPartition::fail(std::optional<std::exception_ptr> throwable)
{
    if (bufferPool != nullptr) {
        bufferPool->lazyDestroy();
    }
    partitionManager->releasePartition(partitionId, throwable);
}

std::optional<std::exception_ptr> ResultPartition::getFailureCause()
{
    return cause;
}

int ResultPartition::getNumTargetKeyGroups()
{
    return numTargetKeyGroups;
}

bool ResultPartition::isReleased()
{
    return isReleased_.load();
}

std::shared_ptr<CompletableFuture> ResultPartition::GetAvailableFuture()
{
    return bufferPool->GetAvailableFuture();
}

std::string ResultPartition::toString() const
{
    return "ResultPartition " + partitionId.toString() + " [" + std::to_string(partitionType) + ", " +
           std::to_string(numSubpartitions) + " subpartitions]";
}

std::shared_ptr<ResultPartitionManager> ResultPartition::getPartitionManager()
{
    return partitionManager;
}

void ResultPartition::checkInProduceState() const
{
    if (isFinished_) {
        throw std::runtime_error("Partition already finished.");
    }
}

void ResultPartition::OnConsumedSubpartition(int subpartitionIndex)
{
    std::cout << "you are in ResultPartition::OnConsumedSubpartition" << std::endl;
    if (isReleased_.load()) {
        return;
    }
    std::cout << toString() << ": Received release notification for subpartition " << subpartitionIndex << "."
              << std::endl;
}
} // namespace omnistream
