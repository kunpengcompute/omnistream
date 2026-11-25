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

#include "TimerThreadPool.h"

namespace omnistream {
TimerThreadPool::TimerThreadPool(size_t threads)
{
    // 1. 启动工作线程 (只负责执行，不负责计时)
    for (size_t i = 0; i < threads; ++i) {
        workers_.emplace_back([this] {
            while (!(stop_ && worker_tasks_.empty())) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex_);
                    worker_cv_.wait(lock, [this] { return stop_ || !worker_tasks_.empty(); });

                    if (stop_ && worker_tasks_.empty()) return;

                    task = std::move(worker_tasks_.front());
                    worker_tasks_.pop();
                }
                task();
            }
        });
    }

    // 2. 启动调度线程 (负责倒计时和循环重排)
    scheduler_ = std::thread([this]() {
        while (!stop_) {
            std::unique_lock<std::mutex> lock(timer_mutex_);

            if (timers_.empty()) {
                timer_cv_.wait(lock, [this] { return stop_ || !timers_.empty(); });
            }

            if (stop_) break;

            // --- 步骤A: 清理已取消的任务 ---
            // 必须在处理堆顶前清理，否则可能把已取消的任务重入队
            cleanCancelledTasks();

            if (timers_.empty()) continue;

            auto now = std::chrono::steady_clock::now();
            auto task_wrapper = timers_.top();

            if (task_wrapper.execute_time > now) {
                // 时间未到，继续睡
                timer_cv_.wait_until(lock, task_wrapper.execute_time);
            } else {
                // --- 步骤B: 时间到了，处理任务 ---
                timers_.pop();

                // 【核心修改】：如果是循环任务，计算下次时间并重新入队
                if (task_wrapper.period_ms > 0) {
                    if (!cancelled_ids_.count(task_wrapper.id)) {
                        TimerTask next_task = task_wrapper;
                        // 计算下一次执行时间（基于理论时间，防止误差累积）
                        next_task.execute_time += std::chrono::milliseconds(task_wrapper.period_ms);
                        timers_.push(next_task);
                    } else {
                        cancelled_ids_.erase(task_wrapper.id);
                    }
                }

                // 解锁定时器锁，因为后面要把任务发给工作线程
                lock.unlock();

                // --- 步骤C: 发送到工作队列执行 ---
                // 注意：这里要再次检查取消状态，防止在等待期间被取消（可选，视一致性要求而定）
                if (!isCancelled(task_wrapper.id)) {
                    {
                        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
                        worker_tasks_.push(task_wrapper.func);
                    }
                    worker_cv_.notify_one();
                }
            }
        }
    });
}

TimerThreadPool::~TimerThreadPool()
{
    {
        std::lock_guard<std::mutex> lock1(queue_mutex_);
        std::lock_guard<std::mutex> lock2(timer_mutex_);
        stop_ = true;
    }
    timer_cv_.notify_all();
    worker_cv_.notify_all();

    if (scheduler_.joinable()) scheduler_.join();
    for (std::thread& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
}

void TimerThreadPool::cancel(TaskId id)
{
    // 1. 从令牌 map 中移除并获取令牌的 shared_ptr
    std::shared_ptr<Token> token_to_invalidate;
    {
        std::lock_guard<std::mutex> lock(tokens_mutex_);
        auto it = task_tokens_.find(id);
        if (it != task_tokens_.end()) {
            token_to_invalidate = it->second;
            task_tokens_.erase(it);
        }
    }

    // 2. 如果找到了令牌，就将其有效性设为 false
    if (token_to_invalidate) {
        token_to_invalidate->is_valid.store(false, std::memory_order_release);
    }

    // 3. 同时，也要将 ID 加入旧的取消集合，以便 scheduler_ 线程清理队列
    {
        std::lock_guard<std::mutex> lock(timer_mutex_);
        cancelled_ids_.insert(id);
    }
}

void TimerThreadPool::cleanCancelledTasks()
{
    while (!timers_.empty()) {
        auto& top = timers_.top();
        if (cancelled_ids_.count(top.id)) {
            auto id = top.id;
            timers_.pop();
            // 注意：对于循环任务，如果这里移除了，就不会再重入队了，等于彻底取消
            cancelled_ids_.erase(id);
        } else {
            break;
        }
    }
}

bool TimerThreadPool::isCancelled(TimerThreadPool::TaskId id)
{
    std::lock_guard<std::mutex> lock(timer_mutex_);
    auto it = cancelled_ids_.find(id);
    if (it != cancelled_ids_.end()) {
        cancelled_ids_.erase(it);
        return true;
    }
    return false;
}
} // namespace omnistream
