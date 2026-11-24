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

#ifndef OMNISTREAM_TIMERTHREADPOOL_H
#define OMNISTREAM_TIMERTHREADPOOL_H

#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <atomic>
#include <unordered_set>
namespace omnistream {
class TimerThreadPool {
public:
    using TaskId = uint64_t;

    static TimerThreadPool* GetTimerThreadPoolInstance()
    {
        static TimerThreadPool instance;
        return &instance;
    }

    TimerThreadPool(size_t threads = 1);

    ~TimerThreadPool();

    // =============================================
    // API 1: 添加单次延迟任务 (和之前一样)
    // =============================================
    template <class F, class... Args>
    TaskId addDelayedTask(unsigned int delay_ms, F&& f, Args&&... args)
    {
        return submitTask(delay_ms, 0, std::forward<F>(f), std::forward<Args>(args)...);
    }

    // =============================================
    // API 2: 添加循环定时任务 【新功能】
    // interval_ms: 循环间隔
    // =============================================
    template <class F, class... Args>
    TaskId addPeriodicTask(unsigned int interval_ms, F&& f, Args&&... args)
    {
        // 对于循环任务，第一次执行通常也是在 interval_ms 之后
        // 如果希望立即执行一次，可以把第一个参数改为 0 (但逻辑上通常是延迟一个周期)
        return submitTask(interval_ms, interval_ms, std::forward<F>(f), std::forward<Args>(args)...);
    }

    void cancel(TaskId id);

private:
    struct Token {
        std::atomic<bool> is_valid{true}; // 原子布尔值，表示任务是否有效
    };
    struct TimerTask {
        std::chrono::steady_clock::time_point execute_time;
        std::function<void()> func;
        TaskId id;
        unsigned int period_ms; // 0 表示单次，>0 表示循环周期
        std::shared_ptr<Token> token;

        bool operator<(const TimerTask& other) const
        {
            return execute_time > other.execute_time; // 最小堆
        }
    };

    // 通用的任务提交内部函数
    template <class F, class... Args>
    TaskId submitTask(unsigned int delay_ms, unsigned int period_ms, F&& f, Args&&... args)
    {
        TaskId id = next_task_id_++;

        // 1. 创建一个生存令牌 (包含原子布尔)
        auto token = std::make_shared<Token>();

        // 2. 将令牌与 TaskId 关联起来
        {
            std::lock_guard<std::mutex> lock(tokens_mutex_);
            task_tokens_[id] = token;
        }

        // 3. 创建一个包装后的任务函数，它在执行原函数前先检查令牌
        auto task_func = [token, f = std::bind(std::forward<F>(f), std::forward<Args>(args)...)]() {
            // 原子读取，无需锁，安全
            if (token->is_valid.load(std::memory_order_acquire)) {
                f(); // 令牌有效，安全执行原函数
            }
        };

        auto execute_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms);

        {
            std::lock_guard<std::mutex> lock(timer_mutex_);
            timers_.push({execute_time, task_func, id, period_ms, token});
        }
        timer_cv_.notify_one();
        return id;
    }

    void cleanCancelledTasks();

    bool isCancelled(TaskId id);

    // Worker 相关
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> worker_tasks_;
    std::mutex queue_mutex_;
    std::condition_variable worker_cv_;

    // Scheduler 相关
    std::thread scheduler_;
    std::priority_queue<TimerTask> timers_;
    std::mutex timer_mutex_;
    std::condition_variable timer_cv_;

    std::atomic<bool> stop_;
    std::atomic<TaskId> next_task_id_;
    std::unordered_set<TaskId> cancelled_ids_;

    std::unordered_map<TaskId, std::shared_ptr<Token>> task_tokens_;
    mutable std::mutex tokens_mutex_;
};
} // namespace omnistream
#endif // OMNISTREAM_TIMERTHREADPOOL_H
