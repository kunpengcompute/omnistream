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

#ifndef OMNISTREAM_SCHEDULEDFUTURETASK_H
#define OMNISTREAM_SCHEDULEDFUTURETASK_H

#include "core/utils/threads/CompletableFuture.h"
#include <atomic>

class ScheduledFutureTask {
public:
    ScheduledFutureTask(long time, const long period, std::unique_ptr<omnistream::Runnable>&& outerTask);

    ScheduledFutureTask(long time, std::unique_ptr<omnistream::Runnable>&& outerTask);

    long GetDelay() const;

    void SetNextRuntime();

    void Run();

    // support interrupt executing task
    void Cancel();

    bool IsPeriodic() const;

    bool IsCancelled() const;

    bool operator<(const ScheduledFutureTask& other) const
    {
        // smaller time has higher priority
        return time > other.time;
    }

private:
    long time;
    const long period;
    std::unique_ptr<omnistream::Runnable> outerTask;
    std::atomic<bool> stop = false;
};

#endif // OMNISTREAM_SCHEDULEDFUTURETASK_H
