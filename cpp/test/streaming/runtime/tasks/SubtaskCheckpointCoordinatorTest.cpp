/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this
 * software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:

 * *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES
 * OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT
 * FOR A PARTICULAR PURPOSE.
 */

#include <gtest/gtest.h>
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.h"
using namespace omnistream::runtime;
TEST(SubtaskCheckpointCoordinatorTest, InitTest)
{
    auto coordinator = std::make_shared<SubtaskCheckpointCoordinatorImpl>(
        nullptr,
        nullptr,
        "tempName",
        std::make_shared<omnistream::ImmediateStreamTaskActionExecutor>(),
        std::make_shared<omnistream::RuntimeEnvironmentV2>(),
        false,
        false,
        new std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>(),
        new BarrierAlignmentUtil::DelayableTimer<std::function<void()>>());
}
