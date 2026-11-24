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

// ResultPartitionManager.cpp
#include "ResultPartitionManager.h"
#include <iostream>
#include "PartitionNotFoundException.h"
#include "taskmanager/OmniTask.h"

namespace omnistream {

ResultPartitionManager::ResultPartitionManager() : registeredPartitions(), isShutdown(false)
{
}

ResultPartitionManager::~ResultPartitionManager()
{
    shutdown();
}

void ResultPartitionManager::registerResultPartition(std::shared_ptr<ResultPartition> partition)
{
    std::cout << "ResultPartitionManager::registerResultPartition: " << partition.use_count() << std::endl;
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (isShutdown) {
        throw std::runtime_error("Result partition manager already shut down.");
    }

    auto result = registeredPartitions.insert({partition->getPartitionId(), partition});
    if (!result.second) {
        THROW_RUNTIME_ERROR("Result partition already registered.");
    }

    LOG_PART("Registered " << partition->toString() << std::endl);
}

void ResultPartitionManager::bindOwningTask(const ResultPartitionIDPOD& partitionId, OmniTask* owningTask)
{
    if (owningTask == nullptr) {
        return;
    }

    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto result = partitionToOwningTask_.insert({partitionId, owningTask});
    if (!result.second) {
        THROW_RUNTIME_ERROR("Result partition already bound to a task.");
    }
    if (unconsumedPartitionsPerTask_[owningTask]++ == 0) {
        // First partition of this task, so this is a task newly handed to the manager.
        boundTaskCount_++;
    }

    LOG_PART("Bound " << partitionId.toString() << " to task " << owningTask << std::endl);
}

OmniTask* ResultPartitionManager::unbindOwningTask(const ResultPartitionIDPOD& partitionId)
{
    auto binding = partitionToOwningTask_.find(partitionId);
    if (binding == partitionToOwningTask_.end()) {
        return nullptr;
    }

    OmniTask* owningTask = binding->second;
    partitionToOwningTask_.erase(binding);

    auto unconsumed = unconsumedPartitionsPerTask_.find(owningTask);
    if (unconsumed == unconsumedPartitionsPerTask_.end() || --unconsumed->second > 0) {
        return nullptr;
    }

    unconsumedPartitionsPerTask_.erase(unconsumed);
    return owningTask;
}

std::shared_ptr<ResultSubpartitionView> ResultPartitionManager::createSubpartitionView(
    const ResultPartitionIDPOD& partitionId, int subpartitionIndex, BufferAvailabilityListener* availabilityListener)
{
    LOG("Requesting subpartition " << subpartitionIndex << " of " << partitionId.toString() << std::endl);

    LOCK_BEFORE();
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    LOCK_AFTER();

    auto it = registeredPartitions.find(partitionId);
    if (it == registeredPartitions.end()) {
        throw PartitionNotFoundException("Result partition not found: " + partitionId.toString());
    }

    std::shared_ptr<ResultPartition> partition = it->second;
    LOG_PART("Requesting subpartition " << subpartitionIndex << " of " << partition->toString() << std::endl);

    return partition->createSubpartitionView(subpartitionIndex, availabilityListener);
}

void ResultPartitionManager::releasePartition(
    const ResultPartitionIDPOD& partitionId, std::optional<std::exception_ptr> cause)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = registeredPartitions.find(partitionId);
    if (it != registeredPartitions.end()) {
        std::shared_ptr<ResultPartition> resultPartition = it->second;
        registeredPartitions.erase(it);
        resultPartition->release(cause);
        // The partition was released instead of consumed (cancel/fail); stop tracking the task but leave
        // it alone, its cleanup is driven by the Java side on that path.
        OmniTask* unboundTask = unbindOwningTask(partitionId);
        if (unboundTask != nullptr) {
            tasksAwaitingRunFinish_.erase(unboundTask);
            finishedTasks_.erase(unboundTask);
        }
        std::cout << "Released partition " << partitionId.toString() << " produced by " << partitionId.toString()
                  << std::endl;
    }
}

void ResultPartitionManager::shutdown()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::cout << "Releasing " << registeredPartitions.size() << " partitions because of shutdown." << std::endl;

    for (auto& pair : registeredPartitions) {
        pair.second->release();
    }

    registeredPartitions.clear();
    partitionToOwningTask_.clear();
    unconsumedPartitionsPerTask_.clear();
    tasksAwaitingRunFinish_.clear();
    finishedTasks_.clear();
    isShutdown = true;
    // One line that answers "were they all freed" without any log arithmetic.
    std::cout << "OmniTask accounting at shutdown: " << deletedTaskCount_ << " deleted of " << boundTaskCount_
              << " bound" << (deletedTaskCount_ == boundTaskCount_ ? "" : "  <-- LEAK") << std::endl;
    std::cout << "Successful shutdown." << std::endl;
}

void ResultPartitionManager::onConsumedPartition(std::shared_ptr<ResultPartition> partition)
{
    OmniTask* taskToDelete = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        auto it = registeredPartitions.find(partition->getPartitionId());
        if (it == registeredPartitions.end() || it->second != partition) {
            return;
        }
        registeredPartitions.erase(it);
        partition->release();

        OmniTask* consumedTask = unbindOwningTask(partition->getPartitionId());
        if (consumedTask != nullptr) {
            if (finishedTasks_.erase(consumedTask) > 0) {
                taskToDelete = consumedTask;
            } else {
                // The run loop is still active, it deletes the task through onTaskRunFinished().
                tasksAwaitingRunFinish_.insert(consumedTask);
            }
        }
    }

    // Deleted outside the lock: ~OmniTask closes its partitions and gates, which calls back into the manager.
    if (taskToDelete != nullptr) {
        deleteOwningTask(taskToDelete, "all partitions consumed");
    }
}

void ResultPartitionManager::deleteOwningTask(OmniTask* task, const std::string& reason)
{
    void* taskAddress = task;
    std::cout << "Deleting upstream task " << taskAddress << " (" << reason << ")" << std::endl;
    delete task;

    long deleted;
    long bound;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        deleted = ++deletedTaskCount_;
        bound = boundTaskCount_;
    }
    // Printed only after the destructor has returned, so this line is the proof that the task was
    // actually torn down rather than merely scheduled for deletion.
    std::cout << "Deleted upstream task " << taskAddress << " (" << deleted << " of " << bound
              << " bound tasks deleted so far)" << std::endl;
}

bool ResultPartitionManager::onTaskRunFinished(OmniTask* task)
{
    if (task == nullptr) {
        return false;
    }

    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (tasksAwaitingRunFinish_.erase(task) == 0) {
            if (unconsumedPartitionsPerTask_.count(task) == 0) {
                // Never bound (a task without produced partitions); the Java side owns its deletion.
                return false;
            }
            // Some partitions are still being consumed, onConsumedPartition() deletes the task later.
            finishedTasks_.insert(task);
            return false;
        }
    }

    deleteOwningTask(task, "run loop returned with all partitions consumed");
    return true;
}

std::vector<ResultPartitionIDPOD> ResultPartitionManager::getUnreleasedPartitions()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<ResultPartitionIDPOD> result;
    for (const auto& pair : registeredPartitions) {
        result.push_back(pair.first);
    }
    return result;
}

std::string ResultPartitionManager::toString()
{
    return "ResultPartitionManager";
}
} // namespace omnistream
