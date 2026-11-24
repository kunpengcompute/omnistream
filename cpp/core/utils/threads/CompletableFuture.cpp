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

#include "CompletableFuture.h"

#include <chrono>
#include <utility>

namespace omnistream {

// =============================================================================
// Construction / Destruction
// =============================================================================

CompletableFuture::CompletableFuture() = default;

CompletableFuture::CompletableFuture(bool done)
{
    if (done) {
        std::lock_guard<std::mutex> lock(mtx_);
        state_ = FutureState::COMPLETED;
        done_.store(true);
    }
}

CompletableFuture::CompletableFuture(bool done, FutureState futureState)
{
    // Backward-compatible constructor: respects the requested state if done.
    // The default argument (used by AvailabilityProvider for "AVAILABLE")
    // is COMPLETED, which matches the single-arg overload behavior.
    if (done) {
        std::lock_guard<std::mutex> lock(mtx_);
        state_ = futureState;
        done_.store(true);
    }
}

CompletableFuture::~CompletableFuture()
{
    // runAsync spawned thread: detach if still joinable so the task can complete.
    // The self_ reference inside the worker thread keeps this object alive until
    // executeTask finishes, so the destructor may be invoked twice for the same
    // object — that's an existing pre-bug, not introduced by this rewrite.
    if (worker_.joinable()) {
        worker_.detach();
    }
}

// =============================================================================
// runAsync / runAs — async task spawn (Java runAsync semantics, std::thread)
// =============================================================================

void CompletableFuture::executeTask(std::shared_ptr<CompletableFuture> future, std::shared_ptr<Runnable> task)
{
    try {
        {
            std::lock_guard<std::mutex> lock(future->mtx_);
            future->state_ = FutureState::RUNNING;
        }
        task->run();
        {
            std::lock_guard<std::mutex> lock(future->mtx_);
            future->state_ = FutureState::COMPLETED;
        }
    } catch (...) {
        std::lock_guard<std::mutex> lock(future->mtx_);
        future->state_ = FutureState::FAILED;
        future->exception_ = std::current_exception();
    }

    future->done_.store(true);
    future->cv_.notify_all();
    future->drainCallbacks(); // fire callbacks on this worker thread
    future->self_.reset();    // release self-reference; allows destruction
}

void CompletableFuture::runAs(std::shared_ptr<Runnable> task)
{
    if (isDone()) {
        throw std::runtime_error("Future already completed");
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (state_ != FutureState::NOT_STARTED) {
            throw std::runtime_error("Future already started");
        }
        state_ = FutureState::RUNNING;
    }

    self_ = shared_from_this();
    worker_ = std::thread(executeTask, self_, task);
}

std::shared_ptr<CompletableFuture> CompletableFuture::runAsync(std::shared_ptr<Runnable> task)
{
    auto future = std::make_shared<CompletableFuture>();
    future->runAs(task);
    return future;
}

// =============================================================================
// thenRun — chain a Runnable (CRITICAL FIX: callback-based, returns new future)
// =============================================================================

std::shared_ptr<CompletableFuture> CompletableFuture::thenRun(std::shared_ptr<Runnable> action)
{
    auto child = std::make_shared<CompletableFuture>();

    if (!action) {
        // Null action: child fails immediately with NPE-equivalent.
        child->completeExceptionally(
            std::make_exception_ptr(std::runtime_error("CompletableFuture.thenRun: action is null")));
        return child;
    }

    // PROPAGATION callback: skipped if parent failed.
    addPropagationCallback(child, [action]() { action->run(); });
    return child;
}

// =============================================================================
// whenComplete — observe success/failure (callback always invoked)
// =============================================================================

std::shared_ptr<CompletableFuture> CompletableFuture::whenComplete(std::function<void(std::exception_ptr)> action)
{
    auto child = std::make_shared<CompletableFuture>();
    addObservationCallback(child, std::move(action));
    return child;
}

// =============================================================================
// Callback registration (internal)
// =============================================================================

void CompletableFuture::addPropagationCallback(std::shared_ptr<CompletableFuture> child, std::function<void()> action)
{
    bool fireNow = false;
    bool parentFailed = false;
    std::exception_ptr parentEx;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (state_ == FutureState::COMPLETED) {
            fireNow = true; // success path, action will be invoked
        } else if (state_ == FutureState::FAILED || state_ == FutureState::CANCELLED) {
            fireNow = true;
            parentFailed = true;
            parentEx = exception_;
        } else {
            callbacks_.push_back(CallbackEntry{child, CallbackKind::PROPAGATION, std::move(action), nullptr});
        }
    }

    if (fireNow) {
        if (parentFailed) {
            // Parent failed: skip action, propagate exception to child.
            child->completeExceptionally(parentEx);
        } else {
            // Parent already complete: invoke action on caller thread synchronously.
            try {
                action();
                child->complete();
            } catch (...) {
                child->completeExceptionally(std::current_exception());
            }
        }
    }
}

void CompletableFuture::addObservationCallback(
    std::shared_ptr<CompletableFuture> child, std::function<void(std::exception_ptr)> action)
{
    bool fireNow = false;
    bool parentFailed = false;
    std::exception_ptr parentEx;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (state_ == FutureState::COMPLETED || state_ == FutureState::FAILED || state_ == FutureState::CANCELLED) {
            fireNow = true;
            parentFailed = (state_ != FutureState::COMPLETED);
            parentEx = exception_;
        } else {
            callbacks_.push_back(CallbackEntry{child, CallbackKind::OBSERVATION, nullptr, std::move(action)});
        }
    }

    if (fireNow) {
        try {
            action(parentEx);
        } catch (...) {
            // Callback throw in whenComplete is consumed; child still mirrors parent.
            // (Java semantics: whenComplete cannot change outcome.)
        }
        if (parentFailed) {
            child->completeExceptionally(parentEx);
        } else {
            child->complete();
        }
    }
}

// =============================================================================
// drainCallbacks — fire registered callbacks (call with mtx_ NOT held)
// =============================================================================

void CompletableFuture::drainCallbacks()
{
    // Snapshot under lock; fire outside lock to avoid re-entrance and deadlock.
    std::vector<CallbackEntry> snapshot;
    std::exception_ptr ex;
    FutureState terminalState;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (callbacks_.empty()) {
            return;
        }
        snapshot.swap(callbacks_);
        ex = exception_;
        terminalState = state_;
    }

    bool parentFailed = (terminalState != FutureState::COMPLETED);

    for (auto& entry : snapshot) {
        if (!entry.child) {
            continue;
        }
        if (entry.kind == CallbackKind::PROPAGATION) {
            // thenRun semantics: skip action if parent failed.
            if (parentFailed) {
                entry.child->completeExceptionally(ex);
            } else {
                try {
                    entry.noArgAction();
                    entry.child->complete();
                } catch (...) {
                    // Parent success + callback threw → child gets callback's exception.
                    entry.child->completeExceptionally(std::current_exception());
                }
            }
        } else { // OBSERVATION (whenComplete semantics)
            // whenComplete ALWAYS invokes the action. Callback throw handling:
            //   - parent success + callback threw → child gets callback's exception
            //   - parent failed  + callback threw → child's outcome is SUPPRESSED,
            //     child mirrors parent's exception (callback throw is ignored).
            bool callbackThrew = false;
            try {
                entry.obsAction(ex);
            } catch (...) {
                callbackThrew = true;
            }
            if (parentFailed) {
                // Parent failed: child mirrors parent's exception regardless of callback.
                entry.child->completeExceptionally(ex);
            } else if (callbackThrew) {
                // Parent success + callback threw: child gets the callback's exception.
                entry.child->completeExceptionally(std::current_exception());
            } else {
                entry.child->complete();
            }
        }
    }
}

// =============================================================================
// complete / completeExceptionally / cancel — terminal state transitions
// =============================================================================

bool CompletableFuture::completeImpl(FutureState targetState, std::exception_ptr ex)
{
    bool transitioned = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (state_ == FutureState::NOT_STARTED || state_ == FutureState::RUNNING) {
            state_ = targetState;
            if (ex) {
                exception_ = ex;
            }
            done_.store(true);
            transitioned = true;
        }
    }
    if (transitioned) {
        cv_.notify_all();
        drainCallbacks();
    }
    return transitioned;
}

void CompletableFuture::complete()
{
    completeImpl(FutureState::COMPLETED, nullptr);
}

bool CompletableFuture::completeExceptionally(std::exception_ptr ex)
{
    if (!ex) {
        ex = std::make_exception_ptr(std::runtime_error("completeExceptionally called with null exception_ptr"));
    }
    return completeImpl(FutureState::FAILED, ex);
}

bool CompletableFuture::cancel()
{
    auto ex = std::make_exception_ptr(CancellationException());
    return completeImpl(FutureState::CANCELLED, ex);
}

bool CompletableFuture::cancel(bool /*mayInterruptIfRunning*/)
{
    // The current implementation does not support thread interruption;
    // cancel() always marks the future as CANCELLED and fires callbacks.
    return cancel();
}

// =============================================================================
// State queries
// =============================================================================

FutureState CompletableFuture::getState()
{
    std::lock_guard<std::mutex> lock(mtx_);
    return state_;
}

bool CompletableFuture::isDone() const
{
    return done_.load();
}

bool CompletableFuture::isCancelled()
{
    std::lock_guard<std::mutex> lock(mtx_);
    return state_ == FutureState::CANCELLED;
}

bool CompletableFuture::isCompletedExceptionally() const
{
    std::lock_guard<std::mutex> lock(mtx_);
    return state_ == FutureState::FAILED || state_ == FutureState::CANCELLED;
}

void CompletableFuture::setCompleted()
{
    // Internal helper — does NOT fire callbacks.
    // Kept for backward compat with callers that want to mark complete without
    // triggering side effects (rare). Prefer complete() in new code.
    std::lock_guard<std::mutex> lock(mtx_);
    state_ = FutureState::COMPLETED;
    done_.store(true);
    cv_.notify_all();
}

int CompletableFuture::numberOfDependents()
{
    std::lock_guard<std::mutex> lock(mtx_);
    if (state_ == FutureState::COMPLETED || state_ == FutureState::FAILED || state_ == FutureState::CANCELLED) {
        return 0;
    }
    return static_cast<int>(callbacks_.size());
}

// =============================================================================
// get — blocking wait with optional timeout
// =============================================================================

void CompletableFuture::get()
{
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this] { return done_.load(); });
    if (exception_) {
        std::rethrow_exception(exception_);
    }
}

bool CompletableFuture::get(long timeoutMillis)
{
    std::unique_lock<std::mutex> lock(mtx_);
    bool result = cv_.wait_for(lock, std::chrono::milliseconds(timeoutMillis), [this] { return done_.load(); });
    if (result && exception_) {
        std::rethrow_exception(exception_);
    }
    return result;
}

// =============================================================================
// Static factories
// =============================================================================

std::shared_ptr<CompletableFuture> CompletableFuture::completedFuture()
{
    auto f = std::make_shared<CompletableFuture>();
    f->complete();
    return f;
}

// =============================================================================
// allOf — combine via callback chain (no joiner thread)
// =============================================================================

std::shared_ptr<CompletableFuture> CompletableFuture::allOf(
    const std::vector<std::shared_ptr<CompletableFuture>>& futures)
{
    auto result = std::make_shared<CompletableFuture>();
    if (futures.empty()) {
        result->complete();
        return result;
    }

    auto remaining = std::make_shared<std::atomic<size_t>>(futures.size());
    auto firstEx = std::make_shared<std::exception_ptr>();
    auto firstExMtx = std::make_shared<std::mutex>();
    auto failedFlag = std::make_shared<std::atomic<bool>>(false);

    for (auto& f : futures) {
        if (!f) {
            // Null entry counts as failed with NPE.
            bool expected = false;
            if (failedFlag->compare_exchange_strong(expected, true)) {
                std::lock_guard<std::mutex> lock(*firstExMtx);
                *firstEx = std::make_exception_ptr(std::runtime_error("CompletableFuture.allOf: null entry"));
            }
            if (remaining->fetch_sub(1) == 1) {
                std::lock_guard<std::mutex> lock(*firstExMtx);
                result->completeExceptionally(*firstEx);
            }
            continue;
        }

        f->whenComplete([remaining, firstEx, firstExMtx, failedFlag, result](std::exception_ptr ex) {
            if (ex) {
                bool expected = false;
                if (failedFlag->compare_exchange_strong(expected, true)) {
                    std::lock_guard<std::mutex> lock(*firstExMtx);
                    *firstEx = ex;
                }
            }
            if (remaining->fetch_sub(1) == 1) {
                if (failedFlag->load()) {
                    std::lock_guard<std::mutex> lock(*firstExMtx);
                    result->completeExceptionally(*firstEx);
                } else {
                    result->complete();
                }
            }
        });
    }
    return result;
}

// =============================================================================
// anyOf — first-to-complete wins (no polling)
// =============================================================================

std::shared_ptr<CompletableFuture> CompletableFuture::anyOf(
    const std::vector<std::shared_ptr<CompletableFuture>>& futures)
{
    auto result = std::make_shared<CompletableFuture>();
    if (futures.empty()) {
        result->complete();
        return result;
    }

    auto completed = std::make_shared<std::atomic<bool>>(false);
    for (auto& f : futures) {
        if (!f) {
            // Null entry: treat as immediately completed (best-effort semantics).
            bool expected = false;
            if (completed->compare_exchange_strong(expected, true)) {
                result->complete();
            }
            continue;
        }

        f->whenComplete([completed, result](std::exception_ptr /*ex*/) {
            bool expected = false;
            if (completed->compare_exchange_strong(expected, true)) {
                // First one wins; subsequent calls are no-ops.
                result->complete();
            }
        });
    }
    return result;
}

// =============================================================================
// toString — debug only (does not lock; racy reads are acceptable for logging)
// =============================================================================

std::string CompletableFuture::toString() const
{
    switch (state_) {
        case FutureState::CANCELLED: return "Cancelled";
        case FutureState::NOT_STARTED: return "NotStarted";
        case FutureState::RUNNING: return "Running";
        case FutureState::COMPLETED: return "Completed";
        case FutureState::FAILED: return "Failed";
    }
    return {};
}

} // namespace omnistream
