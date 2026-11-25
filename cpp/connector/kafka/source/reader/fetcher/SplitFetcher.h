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

#ifndef FLINK_TNEL_SPLITFETCHER_H
#define FLINK_TNEL_SPLITFETCHER_H

#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <stdexcept>
#include <chrono>
#include "AddSplitsTask.h"
#include "core/include/common.h"
#include "FetchTask.h"
#include "DummySplitFetcherTask.h"

class BlockingDeque {
public:
    void offer(const std::shared_ptr<SplitFetcherTask>& item)
    {
        offerLast(item);
    }

    // 插入到队列的头部
    void offerFirst(const std::shared_ptr<SplitFetcherTask>& item)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        deque_.push_front(item);
        notEmpty_.notify_one(); // 通知等待的消费者
    }

    // 插入到队列的尾部
    void offerLast(const std::shared_ptr<SplitFetcherTask>& item)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        deque_.push_back(item);
        notEmpty_.notify_one(); // 通知等待的消费者
    }

    std::shared_ptr<SplitFetcherTask> take()
    {
        return takeFirst();
    }

    // 从队列头部移除元素，如果队列为空则阻塞
    std::shared_ptr<SplitFetcherTask> takeFirst()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        notEmpty_.wait(lock, [this] { return !deque_.empty(); }); // 等待直到队列不为空
        std::shared_ptr<SplitFetcherTask> item = deque_.front();
        deque_.pop_front();
        return item;
    }

    // 获取队列大小
    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return deque_.size();
    }

    bool isEmpty() const
    {
        return size() == 0;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::deque<std::shared_ptr<SplitFetcherTask>> deque_;
};

template <typename E, typename SplitT>
class SplitFetcher {
public:
    SplitFetcher(
        int id,
        FutureCompletingBlockingQueue<E>* elementsQueue,
        SplitReader<E, SplitT>* splitReader,
        std::function<void(const std::exception_ptr&)> errorHandler,
        std::function<void()> shutdownHook)
        : id(id),
          elementsQueue(elementsQueue),
          splitReader(splitReader),
          errorHandler(errorHandler),
          shutdownHook(shutdownHook)
    {
        this->fetchTask = std::make_shared<FetchTask<E, SplitT>>(
            splitReader,
            elementsQueue,
            [this](const std::set<std::string>& ids) {
                for (const auto& id : ids) {
                    assignedSplitsMap.erase(id);
                }
            },
            id);
    }

    ~SplitFetcher()
    {
        // Release an interrupted fetch batch before its consumer is destroyed.
        runningTask.reset();
        fetchTask.reset();
        delete splitReader;
    }

    void run()
    {
        LOG("Starting split fetcher " + std::to_string(id));
        try {
            while (!closed.load()) {
                runOnce();
            }
        } catch (const std::exception& e) {
            auto exceptionPtr = std::current_exception();
            errorHandler(exceptionPtr);
        }
        try {
            splitReader->close();
        } catch (const std::exception& e) {
            auto exceptionPtr = std::current_exception();
            errorHandler(exceptionPtr);
        }
        LOG("Split fetcher " + std::to_string(id) + " exited");
        // Keep the callable alive until the shutdown hook returns.
        auto onShutdown = shutdownHook;
        onShutdown();
    }

    void runOnce()
    {
        try {
            if (shouldRunFetchTask()) {
                runningTask = fetchTask;
            } else {
                runningTask = taskQueue.take();
            }
            LOG("Prepare to run " + (runningTask ? runningTask->ToString() : "null"));
            if (!wakeUp.load() && runningTask && runningTask->Run()) {
                LOG("Finished running task " + runningTask->ToString());
                // the task has finished running. Set it to null so it won't be enqueued.
                runningTask = nullptr;
                checkAndSetIdle();
            }
        } catch (const std::exception& e) {
            throw std::runtime_error(
                std::string("SplitFetcher thread ") + std::to_string(id) +
                " received unexpected exception while polling the records: " + e.what());
        }
        maybeEnqueueTask(runningTask);
        {
            std::lock_guard<std::mutex> wakeUpLock(
                wakeUpMutex); // 假设存在一个互斥量 wakeUpMutex 用于保护 wakeUp 相关操作
            runningTask = nullptr;
            wakeUp.store(false);
            LOG("Cleaned wakeup flag.");
        }
    }

    void addSplits(std::vector<SplitT*>& splitsToAdd)
    {
        auto addTask = std::make_shared<AddSplitsTask<E, SplitT>>(splitReader, splitsToAdd, assignedSplitsMap);
        enqueueTask(addTask);
        wake(true);
    }

    void enqueueTask(const std::shared_ptr<SplitFetcherTask>& task)
    {
        std::lock_guard<std::mutex> lockGuard(lock);
        taskQueue.offer(task);
        idle = false;
    }

    KafkaPartitionSplitReader* getSplitReader()
    {
        return splitReader;
    }

    // 获取 fetcher 的 ID
    int fetcherId()
    {
        return id;
    }

    // 关闭 split fetcher
    void shutdown()
    {
        bool expected = false;
        bool desired = true;
        if (closed.compare_exchange_strong(expected, desired)) {
            LOG("Shutting down split fetcher " + std::to_string(id));
            wake(false);
        }
    }

    // 获取已分配的分片，供单元测试使用
    const std::unordered_map<std::string, SplitT*>& assignedSplits() const
    {
        return assignedSplitsMap;
    }

    // 判断是否空闲
    bool isIdle() const
    {
        return idle;
    }

    // 检查是否应该运行 fetch 任务
    bool shouldRunFetchTask() const
    {
        return taskQueue.isEmpty() && !assignedSplitsMap.empty();
    }

    void wake(bool taskOnly)
    {
        std::lock_guard<std::mutex> wakeUpLock(wakeUpMutex);
        // 避免重复唤醒
        auto currentTask = runningTask;
        if (isRunningTask(currentTask)) {
            wakeUp.store(true);
            currentTask->WakeUp();
        } else if (!taskOnly) {
            taskQueue.offer(WAKEUP_TASK);
        }
    }

    // 判断是否为正在运行的任务
    bool isRunningTask(const std::shared_ptr<SplitFetcherTask>& task) const
    {
        return task && task != WAKEUP_TASK;
    }

    // 判断是否应该空闲
    bool shouldIdle() const
    {
        return assignedSplitsMap.empty() && taskQueue.isEmpty();
    }

private:
    inline static std::shared_ptr<SplitFetcherTask> WAKEUP_TASK =
        std::make_shared<DummySplitFetcherTask>("WAKEUP_TASK");
    int id;
    BlockingDeque taskQueue;
    std::unordered_map<std::string, SplitT*> assignedSplitsMap;
    FutureCompletingBlockingQueue<E>* elementsQueue;
    SplitReader<E, SplitT>* splitReader;
    std::function<void(const std::exception_ptr&)> errorHandler;
    std::function<void()> shutdownHook;
    std::atomic<bool> wakeUp{false};
    std::atomic<bool> closed{false};
    std::shared_ptr<FetchTask<E, SplitT>> fetchTask;
    std::shared_ptr<SplitFetcherTask> runningTask;
    std::mutex lock;
    bool idle{false};
    std::mutex wakeUpMutex;

    void checkAndSetIdle()
    {
        if (shouldIdle()) {
            std::lock_guard<std::mutex> lockGuard(lock);
            if (shouldIdle()) {
                idle = true;
            }
        }
        elementsQueue->notifyAvailable();
    }

    bool shouldIdle()
    {
        return assignedSplitsMap.empty() && taskQueue.isEmpty();
    }

    void maybeEnqueueTask(const std::shared_ptr<SplitFetcherTask>& task)
    {
        if (!closed.load() && task && task != WAKEUP_TASK && task != fetchTask) {
            taskQueue.offerFirst(task);
            LOG("Enqueued task [ " + task->ToString() + " ]");
        }
    }

    bool isRunningTask(const std::shared_ptr<SplitFetcherTask>& task)
    {
        return task && task != WAKEUP_TASK;
    }
};

#endif // FLINK_TNEL_SPLITFETCHER_H
