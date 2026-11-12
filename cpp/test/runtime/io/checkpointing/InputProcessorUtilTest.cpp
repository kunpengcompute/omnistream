/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "runtime/io/checkpointing/InputProcessorUtil.h"
#include "MockInputGate.h"
#include "MockClasses.h"

using namespace omnistream;

namespace {
class MockCheckpointableTask : public CheckpointableTask {
public:
    MOCK_METHOD(
        void,
        TriggerCheckpointOnBarrier,
        (std::shared_ptr<CheckpointMetaData> checkpointMetaData,
         std::shared_ptr<CheckpointOptions> checkpointOptions,
         std::shared_ptr<CheckpointMetricsBuilder> checkpointMetrics),
        (override));

    MOCK_METHOD(void, abortCheckpointOnBarrier, (long checkpointId, CheckpointException cause), (override));
};

class MockSubtaskCheckpointCoordinator : public SubtaskCheckpointCoordinator {
public:
    MOCK_METHOD(void, InitInputsCheckpoint, (long id, CheckpointOptions* checkpointOptions), (override));
    MOCK_METHOD(std::shared_ptr<ChannelStateWriter>, getChannelStateWriter, (), (override));
};
} // namespace

TEST(InputProcessorUtilTest, CreatesCheckpointedMultipleInputGates)
{
    auto mailboxExecutor = std::make_shared<MailboxExecutorTest>();
    auto timerService = std::make_shared<SystemProcessingTimeService>();
    auto coordinator = std::make_shared<MockSubtaskCheckpointCoordinator>();
    auto task = std::make_unique<MockCheckpointableTask>();
    // Create two input gate groups with one MockInputGate each
    auto gate1 = std::make_shared<MockInputGate>(0);
    auto gate2 = std::make_shared<MockInputGate>(1);
    gate1->initializeInputFormat(InputFormat::OMNI_BATCH);
    gate2->initializeInputFormat(InputFormat::FLINK_ROW);
    std::vector<std::vector<std::shared_ptr<IndexedInputGate>>> inputGateGroups = {{gate1}, {gate2}};

    std::vector<std::shared_ptr<OmniStreamTaskSourceInput>> sourceInputs; // No source inputs

    auto handler = InputProcessorUtil::CreateCheckpointBarrierHandler(
        task.get(),
        "TestTask",
        coordinator,
        mailboxExecutor,
        timerService,
        inputGateGroups,
        sourceInputs,
        false, // enableUnaligned
        0,     // alignedCheckpointTimeoutMillis
        true   // enableCheckpointAfterTasksFinish
    );
    ASSERT_NE(handler, nullptr);
    ASSERT_EQ(handler->GetLatestCheckpointId(), -1);

    auto result = InputProcessorUtil::CreateCheckpointedMultipleInputGate(mailboxExecutor, inputGateGroups, handler);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0]->getInputFormat(), InputFormat::OMNI_BATCH);
    EXPECT_EQ(result[1]->getInputFormat(), InputFormat::FLINK_ROW);
    for (const auto& gate : result) {
        ASSERT_NE(gate, nullptr);
    }
}
