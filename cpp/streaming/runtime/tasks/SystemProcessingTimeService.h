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

#pragma once

#include <chrono>
#include <atomic>
#include <exception>
#include <memory>
#include "TimerService.h"
#include "core/utils/threads/CompletableFuture.h"
#include "ScheduledTaskExecutor.h"
#include "ProcessingTimeServiceUtil.h"
#include "common.h"
#include "runtime/tasks/ProcessingTimeCallback.h"
#include "runtime/tasks/ScheduledFutureTask.h"

class SystemProcessingTimeService : public omnistream::runtime::TimerService {
public:
    SystemProcessingTimeService() : status(0), timeService(1)
    {
        quiesceCompletedFuture = std::make_shared<omnistream::CompletableFuture>();
    }

    ~SystemProcessingTimeService() override
    {
        SystemProcessingTimeService::shutdownService();
    }

    int64_t getCurrentProcessingTime() override
    {
        return static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
    }

    bool isTerminated() override
    {
        return status.load() == STATUS_SHUTDOWN;
    }

    void shutdownService() override
    {
        if (status.exchange(STATUS_SHUTDOWN) != STATUS_SHUTDOWN) {
            timeService.Shutdown();
        }
    }

    bool shutdownServiceUninterruptible(long timeoutMs) override
    {
        // todo: need dev
        return false;
    }

    std::shared_ptr<ScheduledFutureTask> registerTimer(int64_t timestamp, ProcessingTimeCallback* target) override
    {
        std::shared_ptr<ProcessingTimeCallback> ptr(target, [](ProcessingTimeCallback*) {});
        return registerTimer(timestamp, ptr);
    }

    std::shared_ptr<ScheduledFutureTask> registerTimer(
        int64_t timestamp, std::shared_ptr<ProcessingTimeCallback> target) override
    {
        if (status.load() != STATUS_ALIVE) {
            return std::shared_ptr<ScheduledFutureTask>(nullptr);
        }

        int64_t delay = ProcessingTimeServiceUtil::getProcessingTimeDelay(timestamp, getCurrentProcessingTime());
        auto task = wrapOnTimerCallback(target, timestamp, 0);
        return timeService.Schedule(std::move(task), delay);
    }

    std::shared_ptr<ScheduledFutureTask> scheduleWithFixedDelay(
        std::shared_ptr<ProcessingTimeCallback> callback, long initialDelay, long period) override
    {
        return scheduleRepeatedly(callback, initialDelay, period, true);
    }

    std::unique_ptr<omnistream::Runnable> wrapOnTimerCallback(
        std::shared_ptr<ProcessingTimeCallback>& callback, long nextTimestamp, long period)
    {
        return std::make_unique<ScheduledTask>(status, callback, nextTimestamp, period);
    }

private:
    static const int STATUS_ALIVE = 0;
    static const int STATUS_QUIESCED = 1;
    static const int STATUS_SHUTDOWN = 2;
    std::atomic<int> status;

    ScheduledTaskExecutor timeService;
    std::shared_ptr<omnistream::CompletableFuture> quiesceCompletedFuture;

    class ScheduledTask : public omnistream::Runnable {
    public:
        ScheduledTask(
            std::atomic<int>& serviceStatus,
            std::shared_ptr<ProcessingTimeCallback>& callback,
            long timestamp,
            long period)
            : serviceStatus(serviceStatus),
              callback(callback),
              nextTimestamp(timestamp),
              period(period)
        {
        }

        void run()
        {
            if (serviceStatus.load() != SystemProcessingTimeService::STATUS_ALIVE) {
                return;
            }

            try {
                callback->OnProcessingTime(nextTimestamp);
            } catch (const std::exception& e) {
                LOG("Error: processing timer callback failed, timestamp=" << nextTimestamp << ", error=" << e.what());
            } catch (...) {
                LOG("Error: processing timer callback failed, timestamp=" << nextTimestamp << ", error=unknown");
            }

            nextTimestamp += period;
        }

    private:
        std::atomic<int>& serviceStatus;
        std::shared_ptr<ProcessingTimeCallback> callback;
        long nextTimestamp;
        const long period;
    };

    std::shared_ptr<ScheduledFutureTask> scheduleRepeatedly(
        std::shared_ptr<ProcessingTimeCallback>& callback, long initialDelay, long period, bool fixedDelay)
    {
        long nextTimestamp = getCurrentProcessingTime() + initialDelay;
        auto task = wrapOnTimerCallback(callback, nextTimestamp, period);

        try {
            if (fixedDelay) {
                return timeService.ScheduleWithFixedDelay(std::move(task), initialDelay, period);
            } else {
                return timeService.ScheduleAtFixedRate(std::move(task), initialDelay, period);
            }
        } catch (const std::runtime_error& e) {
            THROW_RUNTIME_ERROR(
                "failed to schedule given task, current stats of ProcessingTimeService is " +
                std::to_string(status.load()));
        }
    }
};
