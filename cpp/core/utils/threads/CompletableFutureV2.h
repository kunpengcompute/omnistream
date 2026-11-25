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

#ifndef COMPLETABLE_FUTURE_V2_H
#define COMPLETABLE_FUTURE_V2_H

#include <iostream>
#include <future>
#include <thread>
#include <mutex>
#include <memory>
#include <functional>
#include <exception>
#include <optional>
// 1. Forward declare primary
template <typename T>
class CompletableFutureV2;

// 2. Forward declare specialization
template <>
class CompletableFutureV2<void>;

template <typename T>
class CompletableFutureV2 {
public:
    // Constructor
    CompletableFutureV2() : future_(promise_.get_future().share())
    {
    }

    // Movable
    CompletableFutureV2(CompletableFutureV2&& other) noexcept
        : promise_(std::move(other.promise_)),
          future_(std::move(other.future_)),
          completed_(other.completed_.load()),
          completedExceptionally_(other.completedExceptionally_.load()),
          result_(other.result_),
          callbacks_(std::move(other.callbacks_))
    {
    }

    CompletableFutureV2& operator=(CompletableFutureV2&& other) noexcept
    {
        if (this != &other) {
            promise_ = std::move(other.promise_);
            future_ = std::move(other.future_);
            completed_.store(other.completed_.load());
            completedExceptionally_.store(other.completedExceptionally_.load());
            result_ = std::move(other.result_);
            callbacks_ = std::move(other.callbacks_);
        }
        return *this;
    }

    // Not copyable
    CompletableFutureV2(const CompletableFutureV2&) = delete;
    CompletableFutureV2& operator=(const CompletableFutureV2&) = delete;

    // Complete with value
    void Complete(const T& value)
    {
        std::call_once(flag_, [this, &value]() {
            promise_.set_value(value);
            completed_.store(true);
            result_ = value;
            NotifyCallbacks();
        });
    }
    // Complete with exception
    void CompleteExceptionally(std::exception_ptr ex)
    {
        std::call_once(flag_, [this, ex]() {
            promise_.set_exception(ex);
            completedExceptionally_.store(true);
        });
    }
    bool IsCancelled()
    {
        return completedExceptionally_;
    }
    // Get value
    T Get()
    {
        return future_.get();
    }
    // Non-blocking result access
    T GetNow(const T& valueIfAbsent)
    {
        if (IsDone()) {
            try {
                return future_.get();
            } catch (...) {
                return valueIfAbsent;
            }
        }
        return valueIfAbsent;
    }

    // SupplyAsync
    template <typename F>
    static std::shared_ptr<CompletableFutureV2<std::invoke_result_t<F>>> SupplyAsync(F&& func)
    {
        using R = std::invoke_result_t<F>;
        auto cf_ptr = std::make_shared<CompletableFutureV2<R>>();

        // Move func into the lambda to avoid unnecessary copies
        std::thread([cf_ptr, func = std::move(func)]() mutable {
            try {
                cf_ptr->Complete(func());
            } catch (...) {
                cf_ptr->CompleteExceptionally(std::current_exception());
            }
        }).detach(); // still detached, but safe because cf_ptr is captured by value

        return cf_ptr;
    }

    // Cancel the future (can't stop threads, but marks it logically cancelled)
    void Cancel()
    {
        Cancel(std::make_exception_ptr(std::runtime_error("Cancelled")));
    }

    void Cancel(std::exception_ptr cause)
    {
        std::call_once(flag_, [this, cause]() {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                completedExceptionally_ = true;
                // ThenApply callbacks expect a value. Supply a default value on
                // cancellation so they can release their completion resources
                // without dereferencing an empty optional.
                result_ = T{};
            }
            promise_.set_exception(cause);
            NotifyCallbacks();
        });
    }
    bool IsDone() const
    {
        return completed_.load() || completedExceptionally_.load();
    }

    // Callback after completion
    // This function is very likely to be problematic. Try to avoid it.
    void ThenApply(std::function<void(const T&)> callback)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (result_) {
            callback(*result_);
        } else {
            callbacks_.push_back(callback);
        }
    }

    // ThenRun: run a no-arg callback after future completes
    // This function is very likely to be problematic. Try to avoid it.
    void ThenRun(std::function<void()> callback)
    {
        ThenApply([callback](const T&) { callback(); });
    }
    // Wait for all futures to complete
    static std::shared_ptr<CompletableFutureV2<void>> AllOf(
        const std::vector<std::shared_ptr<CompletableFutureV2<T>>>& futures);

private:
    std::promise<T> promise_;
    std::shared_future<T> future_;
    // This flag_ is for completion
    std::once_flag flag_;
    std::atomic<bool> completed_{false};
    std::atomic<bool> completedExceptionally_{false}; // NEW

    // for callbacks;
    std::optional<T> result_;
    std::vector<std::function<void(const T&)>> callbacks_;
    std::mutex mutex_;
    void NotifyCallbacks()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& cb : callbacks_) {
            cb(*result_);
        }
        callbacks_.clear();
    }
};

template <>
class CompletableFutureV2<void> {
public:
    // Constructor
    CompletableFutureV2() : future_(promise_.get_future().share())
    {
    }

    // Movable
    CompletableFutureV2(CompletableFutureV2&& other) noexcept
        : promise_(std::move(other.promise_)),
          future_(std::move(other.future_)),
          completed_(other.completed_.load()),
          completedExceptionally_(other.completedExceptionally_.load()),
          callbacks_(std::move(other.callbacks_))
    {
    }

    CompletableFutureV2& operator=(CompletableFutureV2&& other) noexcept
    {
        if (this != &other) {
            promise_ = std::move(other.promise_);
            future_ = std::move(other.future_);
            completed_.store(other.completed_.load());
            completedExceptionally_.store(other.completedExceptionally_.load());
            callbacks_ = std::move(other.callbacks_);
        }
        return *this;
    }

    // Not copyable
    CompletableFutureV2(const CompletableFutureV2&) = delete;
    CompletableFutureV2& operator=(const CompletableFutureV2&) = delete;

    // Complete without value
    void Complete()
    {
        std::call_once(flag_, [this]() {
            promise_.set_value();
            completed_ = true;
            NotifyCallbacks();
        });
    }

    // Get (blocks until completion)
    void Get()
    {
        future_.get();
    }
    // Complete with exception
    void CompleteExceptionally(std::exception_ptr ex)
    {
        std::call_once(flag_, [this, ex]() {
            promise_.set_exception(ex);
            completedExceptionally_.store(true);
        });
    }

    // SupplyAsync
    template <typename F>
    static std::shared_ptr<CompletableFutureV2<std::invoke_result_t<F>>> SupplyAsync(F&& func)
    {
        using R = std::invoke_result_t<F>;
        auto cf_ptr = std::make_shared<CompletableFutureV2<R>>();

        // Move func into the lambda to avoid unnecessary copies
        std::thread([cf_ptr, func = std::move(func)]() mutable {
            try {
                func();
                cf_ptr->Complete();
            } catch (...) {
                cf_ptr->CompleteWithException(std::current_exception());
            }
        }).detach(); // still detached, but safe because cf_ptr is captured by value

        return cf_ptr;
    }

    // Cancel the future (can't stop threads, but marks it logically cancelled)
    void Cancel()
    {
        Cancel(std::make_exception_ptr(std::runtime_error("Cancelled")));
    }

    void Cancel(std::exception_ptr cause)
    {
        std::call_once(flag_, [this, cause]() {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                completedExceptionally_ = true;
            }
            promise_.set_exception(cause);
            NotifyCallbacks();
        });
    }
    bool IsDone() const
    {
        return completed_.load();
    }

    // ThenRun: run a no-arg callback after future completes
    // This function is very likely to be problematic. Try to avoid it.
    void ThenRun(std::function<void()> callback)
    {
        std::cout << "execute then run method!" << std::endl;
        auto wrapper = [this, callback]() {
            if (completed_) {
                callback();
            }
        };

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (completed_) {
                std::thread(wrapper).detach();
            } else {
                callbacks_.push_back(std::move(wrapper));
            }
        }
    }
    // Wait for all futures to complete
    static std::shared_ptr<CompletableFutureV2<void>> AllOf(
        const std::vector<std::shared_ptr<CompletableFutureV2<void>>>& futures)
    {
        auto cf_ptr = std::make_shared<CompletableFutureV2<void>>();
        std::thread([cf_ptr, futures]() mutable {
            try {
                for (auto& f : futures) {
                    f->Get(); // wait for each to complete
                }
                cf_ptr->Complete();
                std::cout << "all of future complete!" << std::endl;
            } catch (...) {
                cf_ptr->CompleteExceptionally(std::current_exception());
            }
        }).detach();
        return cf_ptr;
    }

    // 2. 新增链式 ThenRun：返回一个新的 future，其完成标志着 callback 执行完毕
    std::shared_ptr<CompletableFutureV2<void>> ThenRunWithFuture(std::function<void()> callback)
    {
        auto result = std::make_shared<CompletableFutureV2<void>>();
        auto wrapper = [this, callback, result]() {
            if (completed_) {
                callback();         // 执行用户回调
                result->Complete(); // 标记 result future 完成
            }
        };
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (completed_) {
                // 如果已经完成，立即执行 callback 并完成 result
                std::thread(std::move(wrapper)).detach(); // 异步执行，避免阻塞调用线程
            } else {
                // 否则将 wrapper 存入回调列表，待 future 完成时执行
                callbacks_.push_back(std::move(wrapper));
            }
        }
        return result;
    }

private:
    std::promise<void> promise_;
    std::shared_future<void> future_;
    // This flag_ is for completion
    std::once_flag flag_;
    std::atomic<bool> completed_{false};
    std::atomic<bool> completedExceptionally_{false}; // NEW

    // for callbacks;
    std::vector<std::function<void()>> callbacks_;
    std::mutex mutex_;
    void NotifyCallbacks()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& cb : callbacks_) {
            cb();
        }
        callbacks_.clear();
    }
};

// Wait for all futures to complete
template <typename T>
std::shared_ptr<CompletableFutureV2<void>> CompletableFutureV2<T>::AllOf(
    const std::vector<std::shared_ptr<CompletableFutureV2<T>>>& futures)
{
    auto cf_ptr = std::make_shared<CompletableFutureV2<void>>();
    std::thread([cf_ptr, futures]() mutable {
        try {
            for (auto& f : futures) {
                f->Get(); // wait for each to complete
            }
            cf_ptr->Complete();
        } catch (...) {
            cf_ptr->CompleteExceptionally(std::current_exception());
        }
    }).detach();
    return cf_ptr;
}

#endif
