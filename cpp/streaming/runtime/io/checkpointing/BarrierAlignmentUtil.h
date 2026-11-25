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

#ifndef FLINK_TNEL_BARRIERALIGNMENTUTIL
#define FLINK_TNEL_BARRIERALIGNMENTUTIL

#include <chrono>
#include <functional>
#include <future>
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "streaming/runtime/tasks/mailbox/MailboxExecutor.h"
#include "streaming/runtime/tasks/TimerService.h"
#include "core/utils/function/ThrowingRunnable.h"
namespace omnistream::runtime {
class BarrierAlignmentUtil {
public:
    static long getTimerDelay(long clockMillis, const CheckpointBarrier& announcedBarrier);

    class Cancellable {
    public:
        explicit Cancellable(std::shared_ptr<ScheduledFutureTask> scheduledFuture)
            : scheduledFuture_(scheduledFuture) {};

        ~Cancellable() = default;

        void Cancel()
        {
            if (auto sp = scheduledFuture_.lock()) {
                sp->Cancel();
            }
        }

    private:
        // ScheduledFutureTask will managed by std::shared_ptr and need release memeory after task finished
        // use the std::weak_ptr to prevent ScheduledFutureTask memory release postponed by the Cancellable callback
        std::weak_ptr<ScheduledFutureTask> scheduledFuture_;
    };

    template <typename Func>
    class ThrowingRunnableImpl : public omnistream::ThrowingRunnable {
    public:
        ThrowingRunnableImpl(Func func, const std::string& description)
            : func_(std::move(func)),
              description_(description) {};

        void Run() override
        {
            if (func_) {
                func_();
            }
        }

        void TryCancel() override
        {
            // No-op for this implementation
        }

        std::string ToString() const override
        {
            return "ThrowingRunnableImpl: " + description_;
        }

    private:
        Func func_;
        std::string description_;
    };

    template <typename Func>
    class DelayableTimer {
    public:
        DelayableTimer() = default;
        virtual ~DelayableTimer() = default;
        DelayableTimer(omnistream::MailboxExecutor* executor, TimerService* timerService)
            : executor_(executor),
              timerService_(timerService) {};

        class ProcessingTimeCallbackImpl : public ProcessingTimeCallback {
        public:
            ProcessingTimeCallbackImpl(Func func, omnistream::MailboxExecutor* executor)
                : func_(func),
                  executor_(executor) {};

            void OnProcessingTime(int64_t timestamp) override
            {
                executor_->execute(
                    std::make_shared<ThrowingRunnableImpl<Func>>(
                        func_, "BarrierAlignmentUtil::DelayableTimer::registerTask"),
                    "BarrierAlignmentUtil::DelayableTimer::registerTask");
            }

        private:
            Func func_;
            omnistream::MailboxExecutor* executor_;
        };

        virtual typename BarrierAlignmentUtil::Cancellable* RegisterTask(Func callable, std::chrono::milliseconds delay)
        {
            auto future = timerService_->registerTimer(
                timerService_->getCurrentProcessingTime() + delay.count(),
                std::make_shared<ProcessingTimeCallbackImpl>(callable, executor_));
            return new Cancellable(future);
        }

    private:
        omnistream::MailboxExecutor* executor_;
        TimerService* timerService_;
    };

    template <typename Func>
    class ThrowingDelayableTimer : public DelayableTimer<Func> {
    public:
        ThrowingDelayableTimer() : BarrierAlignmentUtil::DelayableTimer<Func>(nullptr, nullptr)
        {
        }

        typename BarrierAlignmentUtil::Cancellable* RegisterTask(Func, std::chrono::milliseconds) override
        {
            throw std::runtime_error("Strictly unaligned checkpoints should never register any callbacks");
        }
    };

    template <typename Func>
    static DelayableTimer<Func>* createRegisterTimerCallback(
        omnistream::MailboxExecutor* executor, TimerService* timerService);
};

template <typename Func>
inline typename BarrierAlignmentUtil::DelayableTimer<Func>* BarrierAlignmentUtil::createRegisterTimerCallback(
    omnistream::MailboxExecutor* executor, TimerService* timerService)
{
    return new DelayableTimer<Func>(executor, timerService);
}
} // namespace omnistream::runtime
#endif // FLINK_TNEL_BARRIERALIGNMENTUTIL
