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

#include "streaming/runtime/tasks/AsyncCheckpointRunnable.h"
#include <gtest/gtest.h>

TEST(AsyncCheckpointRunnableTest, InitTest)
{
    auto asyncCheckpointRunnable = new AsyncCheckpointRunnable(
        new std::unordered_map<OperatorID, OperatorSnapshotFutures*>(),
        CheckpointMetaData(1, 1000),
        CheckpointMetricsBuilder(),
        0,
        "TestTask",
        new std::function<void(AsyncCheckpointRunnable*)>([](AsyncCheckpointRunnable*) {}),
        nullptr,
        new std::function<void(std::string, std::exception)>([](std::string, std::exception) {}),
        false,
        false,
        std::make_shared<omnistream::LambdaSupplier<bool>>(
            std::function<std::shared_ptr<bool>()>([]() { return std::make_shared<bool>(true); })));
    EXPECT_TRUE(asyncCheckpointRunnable->IsRunning());
    delete asyncCheckpointRunnable;
}
