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

#include "ScheduledFutureTask.h"

ScheduledFutureTask::ScheduledFutureTask(
    long initialDelay, const long period, std::unique_ptr<omnistream::Runnable>&& outerTask)
    : period(period),
      outerTask(std::move(outerTask))
{
    this->time = initialDelay + static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         std::chrono::system_clock::now().time_since_epoch())
                                                         .count());
}

ScheduledFutureTask::ScheduledFutureTask(long initialDelay, std::unique_ptr<omnistream::Runnable>&& outerTask)
    : ScheduledFutureTask(initialDelay, 0, std::move(outerTask))
{
}

[[nodiscard]] long ScheduledFutureTask::GetDelay() const
{
    return time - static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count());
}

void ScheduledFutureTask::SetNextRuntime()
{
    time += period;
}

void ScheduledFutureTask::Cancel()
{
    stop.store(true);
}

void ScheduledFutureTask::Run()
{
    if (stop.load()) {
        return;
    }
    outerTask->run();
}

bool ScheduledFutureTask::IsPeriodic() const
{
    return period != 0;
}

bool ScheduledFutureTask::IsCancelled() const
{
    return stop.load();
}
