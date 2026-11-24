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

#include <atomic>
#include <condition_variable>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace omnistream {

// Forward declaration
class CompletableFuture;
using CF = CompletableFuture;

// Runnable interface — kept for backward compatibility.
// Existing implementations: Task, MemberFunctionRunnable, ResumeWrapper, InnerRunnable.
class Runnable {
public:
    virtual ~Runnable() = default;
    virtual void run() = 0;
};

// Wrapper to compose a Runnable.
class Task : public Runnable {
public:
    explicit Task(std::shared_ptr<Runnable> runnable) : target(std::move(runnable))
    {
    }

    void run() override
    {
        if (target) {
            target->run();
        }
    }

private:
    std::shared_ptr<Runnable> target;
};

enum class FutureState {
    NOT_STARTED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
};

// CancellationException — thrown when a future is cancelled and get() is called.
class CancellationException : public std::exception {
public:
    explicit CancellationException(const std::string& msg = "Future cancelled") : msg_(msg)
    {
    }
    const char* what() const noexcept override
    {
        return msg_.c_str();
    }

private:
    std::string msg_;
};

// =============================================================================
// CompletableFuture (V1 rewrite, Java-semantic aligned)
//
// Changes from previous V1:
//   * thenRun: registered as callback, fires synchronously on completing thread
//     (no new thread spawned), returns NEW future (not parent).
//   * allOf / anyOf: pure callback chain, no joiner thread / no polling.
//   * complete() / cancel() / completeExceptionally(): fire callbacks.
//   * Added whenComplete() and cancel(bool mayInterruptIfRunning) semantics.
//   * runAsync / runAs: unchanged — still spawn std::thread (matches Java's
//     runAsync which uses ForkJoinPool; pool abstraction is a later PR).
// =============================================================================
class CompletableFuture : public std::enable_shared_from_this<CompletableFuture> {
public:
    CompletableFuture();
    explicit CompletableFuture(bool done);
    // Backward-compatible constructor (was used by pre-compiled static libs).
    // Default state is COMPLETED; callers should prefer complete() in new code.
    CompletableFuture(bool done, FutureState futureState);
    ~CompletableFuture();

    // Disable copying
    CompletableFuture(const CompletableFuture&) = delete;
    CompletableFuture& operator=(const CompletableFuture&) = delete;

    // Submit a task to run synchronously (on a new thread, via runAsync).
    void runAs(std::shared_ptr<Runnable> task);

    // Run a task asynchronously and return a new CompletableFuture.
    // Spawns std::thread (matches Java's runAsync which uses ForkJoinPool).
    static std::shared_ptr<CompletableFuture> runAsync(std::shared_ptr<Runnable> task);

    // Chain a Runnable to run after this one completes.
    // - If parent is not yet complete: register callback; callback fires on completing thread.
    // - If parent is already complete: callback fires synchronously on caller thread.
    // Returns a NEW CompletableFuture<Void>; never returns the parent.
    // If parent completes exceptionally, the new future also completes exceptionally
    // with the parent's exception (action is NOT invoked in that case).
    // If action throws, the new future completes exceptionally with that exception.
    std::shared_ptr<CompletableFuture> thenRun(std::shared_ptr<Runnable> action);

    // Combine futures (callback chain, no extra threads).
    // allOf: returns CF that completes when ALL input futures complete.
    //        If any input fails, result fails with the first exception.
    // anyOf: returns CF that completes when ANY input future completes.
    static std::shared_ptr<CompletableFuture> allOf(const std::vector<std::shared_ptr<CompletableFuture>>& futures);
    static std::shared_ptr<CompletableFuture> anyOf(const std::vector<std::shared_ptr<CompletableFuture>>& futures);

    // State setters — all fire registered callbacks.
    // complete(): mark as COMPLETED, fire callbacks.
    // completeExceptionally(ex): mark as FAILED, store ex, fire callbacks.
    // cancel(): mark as CANCELLED, fire callbacks with CancellationException.
    void complete();
    bool completeExceptionally(std::exception_ptr ex);
    bool cancel();

    // whenComplete: register a callback that is ALWAYS invoked (success or failure).
    // The returned future inherits the parent's outcome (callback cannot change it).
    std::shared_ptr<CompletableFuture> whenComplete(std::function<void(std::exception_ptr)> action);

    // Check current state
    FutureState getState();

    // Wait for completion and rethrow exception if failed.
    void get();
    bool get(long timeoutMillis);

    // Static factory: a future that is already completed.
    static std::shared_ptr<CompletableFuture> completedFuture();

    // Cancellation
    bool cancel(bool mayInterruptIfRunning); // alias for cancel() — for API parity
    bool isCancelled();

    // State queries
    bool isDone() const;
    bool isCompletedExceptionally() const;

    // Internal helpers
    void setCompleted();

    // Number of pending callbacks (0 if completed)
    int numberOfDependents();

    std::string toString() const;

private:
    // Internal: unified completion path. Returns true if state transitioned.
    bool completeImpl(FutureState targetState, std::exception_ptr ex);

    // Drain and invoke callbacks. Safe to call multiple times (idempotent).
    // Call with mtx_ NOT held.
    void drainCallbacks();

    // Add a propagation callback (thenRun semantics):
    //   parent success -> invoke action, child completes
    //   parent failure -> action NOT invoked, child completes exceptionally
    void addPropagationCallback(std::shared_ptr<CompletableFuture> child, std::function<void()> action);

    // Add an observation callback (whenComplete semantics):
    //   always invoke action; child inherits parent's outcome
    void addObservationCallback(
        std::shared_ptr<CompletableFuture> child, std::function<void(std::exception_ptr)> action);

    FutureState state_ = FutureState::NOT_STARTED;
    std::atomic<bool> done_{false};
    std::exception_ptr exception_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::thread worker_;                      // only used by runAs/runAsync
    std::shared_ptr<CompletableFuture> self_; // self-reference for async task lifecycle

    // Callback entry: pairs a child future with the action to invoke on completion.
    // For propagation callbacks (thenRun): action takes no args, child may be completed
    //   exceptionally without invoking action.
    // For observation callbacks (whenComplete): action receives exception_ptr,
    //   child inherits parent's outcome.
    enum class CallbackKind {
        PROPAGATION, // thenRun: skip action if parent failed
        OBSERVATION  // whenComplete: always invoke, child mirrors parent
    };
    struct CallbackEntry {
        std::shared_ptr<CompletableFuture> child;
        CallbackKind kind;
        std::function<void()> noArgAction;                 // for PROPAGATION
        std::function<void(std::exception_ptr)> obsAction; // for OBSERVATION
    };
    std::vector<CallbackEntry> callbacks_;

    static void executeTask(std::shared_ptr<CompletableFuture> future, std::shared_ptr<Runnable> task);
};

} // namespace omnistream
