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
#include "streaming/runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "runtime/checkpoint/CheckpointType.cpp"
#include "runtime/tasks/mailbox/MailboxExecutor.cpp"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
using namespace omnistream::runtime;
class MailboxExecutorTest : public omnistream::MailboxExecutor {
public:
    MailboxExecutorTest() = default;
    void execute(std::shared_ptr<omnistream::ThrowingRunnable> command, const std::string& description) override
    {
        // Mock implementation for testing
    }

    void execute(
        std::shared_ptr<omnistream::ThrowingRunnable> command,
        const std::string& descriptionFormat,
        const std::vector<std::any>& descriptionArgs) override
    {
        // Mock implementation for testing
    }

    void yield() override
    {
    }
    bool tryYield() override
    {
        return true;
    }
    std::string toString() const override
    {
        return "MockMailboxExecutor";
    }
};

TEST(BarrierAlignmentUtilTest, GetTimerDelay)
{
    std::vector<uint8_t> bytes = {0x01, 0x02, 0x03};
    auto options = new CheckpointOptions(
        CheckpointType::CHECKPOINT,
        std::make_shared<CheckpointStorageLocationReference>(std::make_shared<std::vector<uint8_t>>(bytes)),
        CheckpointOptions::AlignmentType::ALIGNED,
        1000); // 1 second timeout
    auto barrier = new CheckpointBarrier(0, 1234567890, options);

    long clockMillis = 1234567890 + 500;
    long delay = BarrierAlignmentUtil::getTimerDelay(clockMillis, *barrier);
    EXPECT_EQ(delay, 500);

    clockMillis = 1234567890 + 1500;
    delay = BarrierAlignmentUtil::getTimerDelay(clockMillis, *barrier);
    EXPECT_EQ(delay, 0);
    delete barrier;
}

TEST(BarrierAlignmentUtilTest, DelayableTimerRegisterTask)
{
    auto executor = new MailboxExecutorTest();
    auto timerService = new SystemProcessingTimeService();

    auto delayableTimer =
        BarrierAlignmentUtil::createRegisterTimerCallback<std::function<void()>>(executor, timerService);
    auto cancellable = delayableTimer->RegisterTask([]() {}, std::chrono::milliseconds(100));

    EXPECT_NE(cancellable, nullptr);
    EXPECT_NO_THROW(cancellable->Cancel());

    delete cancellable;
    delete executor;
    delete timerService;
}
