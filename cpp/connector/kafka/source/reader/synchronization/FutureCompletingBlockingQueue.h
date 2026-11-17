/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_FUTURECOMPLETINGBLOCKINGQUEUE_H
#define FLINK_TNEL_FUTURECOMPLETINGBLOCKINGQUEUE_H

#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <stdexcept>
#include <vector>
#include <future>
#include "connector/kafka/source/reader/KafkaPartitionSplitReader.h"
#include "runtime/io/AvailabilityProvider.h"
#include "connector/kafka/bind_core_manager.h"

template <typename E>
class FutureCompletingBlockingQueue {
public:
    inline static int DEFAULT_CAPACITY = 5;
    inline static std::shared_ptr<omnistream::CompletableFuture> AVAILABLE =
        omnistream::AvailabilityProvider::AVAILABLE;
    explicit FutureCompletingBlockingQueue(int32_t subIndex) : FutureCompletingBlockingQueue(subIndex, DEFAULT_CAPACITY)
    {
    }

    explicit FutureCompletingBlockingQueue(int32_t subIndex, int capacity) : capacity(capacity)
    {
        if (omnistream::BindCoreManager::GetInstance()->NeedBindSource()) {
            this->coreId = omnistream::BindCoreManager::GetInstance()->GetSourceCore(subIndex);
        }
        if (capacity <= 0) {
            throw std::invalid_argument("capacity must be > 0");
        }
        putConditionAndFlags.emplace_back(std::make_unique<ConditionAndFlag>());
        currentFuture = std::make_shared<omnistream::CompletableFuture>();
    }

    std::shared_ptr<omnistream::CompletableFuture> getAvailabilityFuture() const
    {
        std::lock_guard<std::mutex> guard(lock);
        return currentFuture;
    }

    void notifyAvailable()
    {
        std::lock_guard<std::mutex> guard(lock);
        moveToAvailable();
    }

    bool put(int threadIndex, RecordsWithSplitIds<E>* element)
    {
        std::unique_lock<std::mutex> lk(lock);
        while (queue.size() >= capacity) {
            if (getAndResetWakeUpFlag(threadIndex)) {
                return false;
            }
            waitIfQueueFull(threadIndex, lk);
        }
        enqueue(element);
        return true;
    }

    RecordsWithSplitIds<E>* poll()
    {
        std::lock_guard<std::mutex> guard(lock);
        if (queue.empty()) {
            moveToUnAvailable();
            return nullptr;
        }
        return dequeue();
    }

    RecordsWithSplitIds<E>* peek()
    {
        std::lock_guard<std::mutex> guard(lock);
        if (queue.empty()) {
            return nullptr;
        }
        return queue.front();
    }

    int size()
    {
        std::lock_guard<std::mutex> guard(lock);
        return queue.size();
    }

    bool isEmpty()
    {
        std::lock_guard<std::mutex> guard(lock);
        return queue.empty();
    }

    int remainingCapacity()
    {
        std::lock_guard<std::mutex> guard(lock);
        return capacity - queue.size();
    }

    void wakeUpPuttingThread(int threadIndex)
    {
        std::lock_guard<std::mutex> guard(lock);
        maybeCreateCondition(threadIndex);
        putConditionAndFlags[threadIndex]->wakeUp = true;
        putConditionAndFlags[threadIndex]->cond.notify_one();
    }

    int32_t getCoreId() const
    {
        return coreId;
    }

private:
    const uint64_t capacity;
    std::shared_ptr<omnistream::CompletableFuture> currentFuture;
    mutable std::mutex lock;
    std::queue<RecordsWithSplitIds<E>*> queue;
    std::queue<std::condition_variable*> notFull;
    int32_t coreId = -1;

private:
    struct ConditionAndFlag {
        std::condition_variable cond;
        bool wakeUp;
        ConditionAndFlag() : wakeUp(false)
        {
        }
    };
    std::vector<std::unique_ptr<ConditionAndFlag>> putConditionAndFlags;

    void moveToAvailable()
    {
        std::shared_ptr<omnistream::CompletableFuture> current = currentFuture;
        if (current != AVAILABLE) {
            currentFuture = AVAILABLE;
            current->complete();
        }
    }

    void moveToUnAvailable()
    {
        if (currentFuture == AVAILABLE) {
            currentFuture = std::make_shared<omnistream::CompletableFuture>();
        }
    }

    void enqueue(RecordsWithSplitIds<E>* element)
    {
        uint64_t sizeBefore = queue.size();
        queue.push(element);
        if (sizeBefore == 0) {
            moveToAvailable();
        }
        if (sizeBefore < capacity - 1 && !notFull.empty()) {
            signalNextPutter();
        }
    }

    RecordsWithSplitIds<E>* dequeue()
    {
        uint64_t sizeBefore = queue.size();
        RecordsWithSplitIds<E>* element = queue.front();
        queue.pop();
        if (sizeBefore == capacity && !notFull.empty()) {
            // std::cout << "full queue" << std::endl;
            signalNextPutter();
        }
        if (queue.empty()) {
            moveToUnAvailable();
        }
        return element;
    }

    void signalNextPutter()
    {
        if (!notFull.empty()) {
            auto cond = notFull.front();
            notFull.pop();
            cond->notify_one();
        }
    }

    void maybeCreateCondition(int threadIndex)
    {
        if (putConditionAndFlags.size() < static_cast<size_t>(threadIndex + 1)) {
            putConditionAndFlags.resize(threadIndex + 1);
            for (size_t i = putConditionAndFlags.size(); i < static_cast<size_t>(threadIndex + 1); ++i) {
                putConditionAndFlags[i] = std::make_unique<ConditionAndFlag>();
            }
        }
    }

    bool getAndResetWakeUpFlag(int threadIndex)
    {
        maybeCreateCondition(threadIndex);
        if (putConditionAndFlags[threadIndex]->wakeUp) {
            putConditionAndFlags[threadIndex]->wakeUp = false;
            return true;
        }
        return false;
    }

    void waitIfQueueFull(int threadIndex, std::unique_lock<std::mutex>& lk)
    {
        maybeCreateCondition(threadIndex);
        notFull.push(&putConditionAndFlags[threadIndex]->cond);
        // Use wait with predicate to handle spurious wakeups
        putConditionAndFlags[threadIndex]->cond.wait(lk);
    }
};

#endif // FLINK_TNEL_FUTURECOMPLETINGBLOCKINGQUEUE_H
