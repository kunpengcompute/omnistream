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
#include <thread>

#include "core/utils/threads/CompletableFutureV2.h"
TEST(CompletableFutureTest, CompleteAndGet)
{
    CompletableFutureV2<int> fut;
    fut.Complete(42);
    EXPECT_EQ(fut.Get(), 42);
    EXPECT_TRUE(fut.IsDone());
}

// A non-inline supplier function with a parameter
int ComputeAnswer()
{
    return 42;
}

TEST(CompletableFutureV2Test, SupplyAsyncReturnsValue)
{
    auto cf = CompletableFutureV2<int>::SupplyAsync([]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return 42;
    });

    // Wait for completion
    auto result = cf->Get(); // blocks until completed

    EXPECT_EQ(result, 42);
    EXPECT_TRUE(cf->IsDone());
}

TEST(CompletableFutureTest, ThenApplyCallback)
{
    /*
    CompletableFutureV2<int> future;

    auto chained = future
        .ThenApply([](int x) { return x + 1; });

    future.Complete(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(chained.Get(), 11);
     */
}

TEST(CompletableFutureTest, AllOfCompletes)
{
    auto f1 = std::make_shared<CompletableFutureV2<int>>();
    auto f2 = std::make_shared<CompletableFutureV2<int>>();
    auto f3 = std::make_shared<CompletableFutureV2<int>>();

    f1->Complete(1);
    f2->Complete(2);
    f3->Complete(3);
    // After this all future should have run get()
    auto all = CompletableFutureV2<int>::AllOf({f1, f2, f3});

    // Allow callback to run
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(all->IsDone());
}

TEST(CompletableFutureTest, TwoFuturesCombineResults)
{
    auto f1 = std::make_shared<CompletableFutureV2<int>>();
    auto f2 = std::make_shared<CompletableFutureV2<int>>();

    std::thread t1([f1]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        f1->Complete(40);
    });

    std::thread t2([f2]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        f2->Complete(2);
    });

    // Block until both are ready
    int r1 = f1->Get();
    int r2 = f2->Get();

    int combined = r1 + r2;
    EXPECT_EQ(combined, 42);

    t1.join();
    t2.join();
}
