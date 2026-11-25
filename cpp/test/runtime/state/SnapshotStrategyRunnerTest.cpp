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

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "runtime/state/SnapshotResources.h"
#include "runtime/state/SnapshotStrategyRunner.h"

namespace {

class EmptySupplier : public SnapshotResultSupplier<KeyedStateHandle> {
public:
    std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge>) override
    {
        return SnapshotResult<KeyedStateHandle>::Empty();
    }
};

class ThrowingSupplier : public SnapshotResultSupplier<KeyedStateHandle> {
public:
    std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge>) override
    {
        throw std::runtime_error("supplier failed");
    }
};

class NonStandardThrowingSupplier : public SnapshotResultSupplier<KeyedStateHandle> {
public:
    std::shared_ptr<SnapshotResult<KeyedStateHandle>> get(std::shared_ptr<omnistream::OmniTaskBridge>) override
    {
        throw 42;
    }
};

class CleanupTrackingResources : public SnapshotResources {
public:
    void cleanup() override
    {
        ++cleanupCount_;
    }

    int cleanupCount_ = 0;
};

class DestructionTrackingStrategy : public SnapshotStrategy<KeyedStateHandle, SnapshotResources> {
public:
    explicit DestructionTrackingStrategy(bool* destroyed, bool throwOnGet = false)
        : destroyed_(destroyed),
          throwOnGet_(throwOnGet),
          resources_(std::make_shared<SnapshotResources>())
    {
    }

    ~DestructionTrackingStrategy() override
    {
        *destroyed_ = true;
    }

    std::shared_ptr<SnapshotResources> syncPrepareResources(long) override
    {
        return resources_;
    }

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>&,
        long,
        long,
        std::shared_ptr<CheckpointStreamFactory>,
        CheckpointOptions*,
        std::string) override
    {
        if (throwOnGet_) {
            return std::make_shared<ThrowingSupplier>();
        }
        return std::make_shared<EmptySupplier>();
    }

private:
    bool* destroyed_;
    bool throwOnGet_;
    std::shared_ptr<SnapshotResources> resources_;
};

class NonStandardThrowingStrategy : public SnapshotStrategy<KeyedStateHandle, SnapshotResources> {
public:
    explicit NonStandardThrowingStrategy(std::shared_ptr<CleanupTrackingResources> resources)
        : resources_(std::move(resources))
    {
    }

    std::shared_ptr<SnapshotResources> syncPrepareResources(long) override
    {
        return resources_;
    }

    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> asyncSnapshot(
        const std::shared_ptr<SnapshotResources>&,
        long,
        long,
        std::shared_ptr<CheckpointStreamFactory>,
        CheckpointOptions*,
        std::string) override
    {
        return std::make_shared<NonStandardThrowingSupplier>();
    }

private:
    std::shared_ptr<CleanupTrackingResources> resources_;
};

} // namespace

// runner 销毁后不应额外持有 strategy；已创建异步 task 仍可独立返回 empty result。
TEST(SnapshotStrategyRunnerTest, OwnedStrategyIsDestroyedAfterPreparation)
{
    bool destroyed = false;
    std::weak_ptr<DestructionTrackingStrategy> weakStrategy;
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> task;
    {
        auto strategy = std::make_shared<DestructionTrackingStrategy>(&destroyed);
        weakStrategy = strategy;
        SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources> runner(
            "shared test", strategy, SnapshotExecutionType::ASYNCHRONOUS);
        strategy.reset();
        EXPECT_FALSE(weakStrategy.expired());
        task = runner.snapshot(1L, 1L, nullptr, nullptr, nullptr, "");
    }
    EXPECT_TRUE(destroyed);
    EXPECT_TRUE(weakStrategy.expired());
    auto future = task->get_future();
    (*task)();
    EXPECT_NE(future.get(), nullptr);
}

// 空 strategy 必须在构造阶段带有明确错误被拒绝，避免后续 snapshot 解引用空指针。
TEST(SnapshotStrategyRunnerTest, RejectsNullStrategy)
{
    auto constructRunner = []() {
        return SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources>(
            "null strategy", nullptr, SnapshotExecutionType::ASYNCHRONOUS);
    };

    try {
        constructRunner();
        FAIL() << "expected null strategy rejection";
    } catch (const std::invalid_argument& error) {
        EXPECT_STREQ(error.what(), "Snapshot strategy must not be null");
    }
}

// 异步 supplier 的原始异常必须由 packaged task 原样交给调用方。
TEST(SnapshotStrategyRunnerTest, AsyncTaskPreservesSupplierError)
{
    bool destroyed = false;
    auto strategy = std::make_shared<DestructionTrackingStrategy>(&destroyed, true);
    SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources> runner(
        "throwing strategy", strategy, SnapshotExecutionType::ASYNCHRONOUS);
    auto task = runner.snapshot(1L, 1L, nullptr, nullptr, nullptr, "");
    auto future = task->get_future();

    (*task)();

    try {
        future.get();
        FAIL() << "expected supplier error";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "supplier failed");
    }
}

// 同步 supplier 抛出非标准异常时，runner 仍必须清理资源并原样重抛。
TEST(SnapshotStrategyRunnerTest, SynchronousTaskCleansUpForNonStandardException)
{
    auto resources = std::make_shared<CleanupTrackingResources>();
    auto strategy = std::make_shared<NonStandardThrowingStrategy>(resources);
    SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources> runner(
        "non-standard exception", strategy, SnapshotExecutionType::SYNCHRONOUS);

    try {
        runner.snapshot(1L, 1L, nullptr, nullptr, nullptr, "");
        FAIL() << "expected non-standard supplier exception";
    } catch (int value) {
        EXPECT_EQ(value, 42);
    }
    EXPECT_EQ(resources->cleanupCount_, 1);
}

// packaged task 分支同样必须清理资源，并保留非标准异常的原始值。
TEST(SnapshotStrategyRunnerTest, AsyncTaskCleansUpForNonStandardException)
{
    auto resources = std::make_shared<CleanupTrackingResources>();
    auto strategy = std::make_shared<NonStandardThrowingStrategy>(resources);
    SnapshotStrategyRunner<KeyedStateHandle, SnapshotResources> runner(
        "non-standard exception", strategy, SnapshotExecutionType::ASYNCHRONOUS);
    auto task = runner.snapshot(1L, 1L, nullptr, nullptr, nullptr, "");
    auto future = task->get_future();

    (*task)();

    try {
        future.get();
        FAIL() << "expected non-standard supplier exception";
    } catch (int value) {
        EXPECT_EQ(value, 42);
    }
    EXPECT_EQ(resources->cleanupCount_, 1);
}
