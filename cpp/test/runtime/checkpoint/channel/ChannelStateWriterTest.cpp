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
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "core/utils/threads/CompletableFutureV2.h"

using namespace omnistream;

TEST(ChannelStateWriteResultTest, CreateEmpty)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    EXPECT_FALSE(result->IsDone());

    result->Fail(std::make_exception_ptr(std::runtime_error("Test failure")));
    EXPECT_TRUE(result->GetInputChannelStateHandles()->IsCancelled());
    EXPECT_TRUE(result->GetResultSubpartitionStateHandles()->IsCancelled());
}

TEST(ChannelStateWriteResultTest, IsDone)
{
    auto inputFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::InputChannelStateHandleVecPtr>>();
    auto resultFuture = std::make_shared<
        CompletableFutureV2<ChannelStateWriter::ChannelStateWriteResult::ResultSubpartitionStateVecPtr>>();

    ChannelStateWriter::ChannelStateWriteResult result(inputFuture, resultFuture);
    EXPECT_FALSE(result.IsDone());

    inputFuture->Complete(std::make_shared<std::vector<std::shared_ptr<InputChannelStateHandle>>>());
    EXPECT_FALSE(result.IsDone());

    resultFuture->Complete(std::make_shared<std::vector<std::shared_ptr<ResultSubpartitionStateHandle>>>());
    EXPECT_TRUE(result.IsDone());
}
