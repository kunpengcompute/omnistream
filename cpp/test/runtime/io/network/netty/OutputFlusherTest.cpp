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
#include <thread>
#include <vector>
#include "runtime/io/network/api/writer/V2/OutputFlusher.h"

TEST(OutputFlusherTest, ConcurrentTerminateAndRepeatedClose)
{
    for (int iteration = 0; iteration < 32; ++iteration) {
        omnistream::OutputFlusher flusher("concurrent-close-test", 10, nullptr);
        flusher.start();

        std::promise<void> release;
        auto ready = release.get_future().share();
        std::vector<std::thread> callers;
        for (int caller = 0; caller < 8; ++caller) {
            callers.emplace_back([&flusher, ready]() {
                ready.wait();
                flusher.terminate();
            });
        }
        release.set_value();
        for (auto& caller : callers) {
            caller.join();
        }

        // Explicit close and destruction must remain safe after concurrent callers finish.
        flusher.terminate();
    }
    SUCCEED();
}
