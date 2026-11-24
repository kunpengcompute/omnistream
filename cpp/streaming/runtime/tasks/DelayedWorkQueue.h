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

#ifndef OMNISTREAM_DELAYEDWORKQUEUE_H
#define OMNISTREAM_DELAYEDWORKQUEUE_H

#include <queue>
#include "ScheduledFutureTask.h"

class DelayedWorkQueue {
public:
    void Offer(std::shared_ptr<ScheduledFutureTask>& task);

    std::shared_ptr<ScheduledFutureTask> Take();

    void Shutdown();

    void NotifyAll();

private:
    struct Comparator {
        bool operator()(
            const std::shared_ptr<ScheduledFutureTask>& left, const std::shared_ptr<ScheduledFutureTask>& right) const
        {
            // *left < *right means the right ScheduledFutureTask has higher priority (earlier execution time)
            return *left < *right;
        }
    };

    std::mutex queueMutex;
    std::condition_variable condition;
    std::priority_queue<
        std::shared_ptr<ScheduledFutureTask>,
        std::vector<std::shared_ptr<ScheduledFutureTask>>,
        Comparator>
        queue;
    bool stop = false;
};

#endif // OMNISTREAM_DELAYEDWORKQUEUE_H
