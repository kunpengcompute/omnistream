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
#include <memory>
#include "streaming/runtime/tasks/omni/OmniOneInputStreamTask.h"
#include "streaming/runtime/tasks/omni/OmniTwoInputStreamTask.h"
#include "streaming/runtime/tasks/mailbox/MailboxExecutorImpl.h"
#include "test/functionaltest/e2e/FrameworkConfig.h"
#include "runtime/partition/consumer/SingleInputGate.h"
#include "runtime/buffer/ObjectSegmentProvider.h"
#include "test/runtime/io/checkpointing/MockInputGate.h"
using namespace omnistream;

class OmniTwoInputStreamTaskTest : public ::testing::Test {
protected:
    std::shared_ptr<OmniTwoInputStreamTask> task;
    std::shared_ptr<RuntimeEnvironmentV2> env;
    std::shared_ptr<MailboxExecutor> mockMailbox;
    std::shared_ptr<StreamTaskActionExecutor> executor;

    void SetUp() override
    {
        env = std::make_shared<RuntimeEnvironmentV2>();
        task = std::make_shared<OmniTwoInputStreamTask>(env);
    }
};

TEST_F(OmniTwoInputStreamTaskTest, DISABLED_InitDistributesGatesAndInitializesProcessor)
{
    auto gate1 = std::make_shared<MockInputGate>(0);
    auto gate2 = std::make_shared<MockInputGate>(1);

    std::vector<std::shared_ptr<IndexedInputGate>> gates = {gate1, gate2};

    // Create StreamEdgePODs with distinct type numbers
    StreamEdgePOD edge1, edge2;
    edge1.setTypeNumber(1); // Routed to inputList1
    edge2.setTypeNumber(2); // Routed to inputList2

    // Set config with these 2 input edges
    auto configPOD = make_shared<StreamConfigPOD>();
    configPOD->setNumberOfNetworkInputs(2);
    configPOD->setInStreamEdges({edge1, edge2});
    auto taskConfig = make_shared<TaskInformationPOD>();
    taskConfig->setStreamConfigPOD(*configPOD);
    env->setTaskConfiguration(*(taskConfig));
    //    env->taskConfiguration().setStreamConfigPOD(*(configPOD.get()));
    env->SetInputGates(gates);
    task->restore();
    // Initialize the task
    //    task->init();

    // Assertions to ensure proper distribution and processor creation
    ASSERT_NE(task->GetCheckpointBarrierHandler(), nullptr);
    //    ASSERT_NE(task->GetInputProcessor(), nullptr);
}

TEST(OmniOneInputStreamTaskUnionAllTest, UsesUnionInputGateOnlyForSqlMultipleInputs)
{
    EXPECT_FALSE(OmniOneInputStreamTask::ShouldUseUnionInputGate(1, 0, true));
    EXPECT_FALSE(OmniOneInputStreamTask::ShouldUseUnionInputGate(1, 1, true));
    EXPECT_FALSE(OmniOneInputStreamTask::ShouldUseUnionInputGate(1, 2, false));
    EXPECT_TRUE(OmniOneInputStreamTask::ShouldUseUnionInputGate(1, 2, true));
    EXPECT_TRUE(OmniOneInputStreamTask::ShouldUseUnionInputGate(1, 3, true));

    EXPECT_FALSE(OmniOneInputStreamTask::ShouldUseUnionInputGate(2, 2, true));
}

TEST(OmniOneInputStreamTaskUnionAllTest, KeepsLegacyChannelIdsWhenUnionAllIsDisabled)
{
    InputChannelInfo shiftedGateChannel(5, 7);

    EXPECT_EQ(OmniOneInputStreamTask::GetChannelDeserializerId(shiftedGateChannel, false), 7);
    EXPECT_EQ(
        OmniOneInputStreamTask::GetChannelDeserializerId(shiftedGateChannel, true), shiftedGateChannel.getComplexId());
}
