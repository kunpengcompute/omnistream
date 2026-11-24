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

#include "DelayedWorkQueue.h"
#include "common.h"

void DelayedWorkQueue::Offer(std::shared_ptr<ScheduledFutureTask>& task)
{
    if (task == nullptr) {
        THROW_RUNTIME_ERROR("DelayedWorkQueue::Offer, task is nullptr");
    }
    std::lock_guard<std::mutex> lock(queueMutex);
    queue.push(task);
    condition.notify_one();
}

std::shared_ptr<ScheduledFutureTask> DelayedWorkQueue::Take()
{
    std::unique_lock<std::mutex> lock(queueMutex);

    while (!stop) {
        condition.wait(lock, [this] { return stop || !queue.empty(); });

        if (stop) {
            break;
        }

        std::shared_ptr<ScheduledFutureTask> task = queue.top();
        long delay = task->GetDelay();
        if (delay <= 0) {
            queue.pop();
            return task;
        }

        condition.wait_for(lock, std::chrono::milliseconds(delay), [this, task] {
            return stop || queue.empty() || queue.top() != task || task->GetDelay() <= 0;
        });
    }

    return nullptr;
}

void DelayedWorkQueue::Shutdown()
{
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        stop = true;
    }
    condition.notify_all();
}

void DelayedWorkQueue::NotifyAll()
{
    std::lock_guard<std::mutex> lock(queueMutex);
    condition.notify_all();
}
