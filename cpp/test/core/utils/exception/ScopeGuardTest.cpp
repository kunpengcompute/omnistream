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

#include <functional>
#include <stdexcept>
#include <vector>

#include "core/utils/exception/ScopeGuard.h"

namespace omnistream {
TEST(ScopeGuardTest, RunsCleanupOnceInReverseOrderOnScopeExit)
{
    std::vector<int> calls;
    {
        ScopeGuard stop([&]() { calls.push_back(3); });
        ScopeGuard release([&]() { calls.push_back(2); });
        ScopeGuard cleanup([&]() { calls.push_back(1); });
        EXPECT_TRUE(calls.empty());
    }
    EXPECT_EQ(calls, (std::vector<int>{1, 2, 3}));
}

TEST(ScopeGuardTest, AcceptsEmptyCleanup)
{
    EXPECT_NO_THROW({ ScopeGuard guard(std::function<void()>{}); });
}

TEST(ScopeGuardTest, ContinuesCleanupAfterStandardAndUnknownExceptions)
{
    std::vector<int> calls;
    EXPECT_NO_THROW({
        ScopeGuard stop([&]() { calls.push_back(3); });
        ScopeGuard release([&]() {
            calls.push_back(2);
            throw 42;
        });
        ScopeGuard cleanup([&]() {
            calls.push_back(1);
            throw std::runtime_error("cleanup failed");
        });
    });
    EXPECT_EQ(calls, (std::vector<int>{1, 2, 3}));
}

TEST(ScopeGuardTest, CleansUpDuringUnwindingAndPreservesOriginalException)
{
    std::vector<int> calls;
    bool caughtOriginalException = false;
    try {
        ScopeGuard stop([&]() { calls.push_back(3); });
        ScopeGuard release([&]() {
            calls.push_back(2);
            throw 42;
        });
        ScopeGuard cleanup([&]() {
            calls.push_back(1);
            throw std::runtime_error("cleanup failed");
        });
        throw std::logic_error("invoke failed");
    } catch (const std::logic_error& e) {
        caughtOriginalException = true;
        EXPECT_STREQ(e.what(), "invoke failed");
    }
    EXPECT_TRUE(caughtOriginalException);
    EXPECT_EQ(calls, (std::vector<int>{1, 2, 3}));
}
} // namespace omnistream
