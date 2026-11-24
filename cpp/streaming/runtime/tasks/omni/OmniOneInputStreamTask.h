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

#ifndef OMNIONEINPUTSTREAMTASK_H
#define OMNIONEINPUTSTREAMTASK_H

#include "OmniStreamTask.h"
#include "runtime/execution/OmniEnvironment.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "streaming/runtime/io/OmniStreamTaskInput.h"
#include "runtime/metrics/SimpleCounter.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/InputProcessorUtil.h"

namespace omnistream {
class OmniOneInputStreamTask : public OmniStreamTask {
public:
    explicit OmniOneInputStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env, int taskType)
        : OmniStreamTask(env, taskType)
    {
    }

    void init() override;

    void processInput(MailboxDefaultAction::Controller* controller) override;

    ~OmniOneInputStreamTask() override = default;
    const std::string getName() const override;
    OmniPushingAsyncDataInput::OmniDataOutput* createDataOutput(
        std::shared_ptr<omnistream::SimpleCounter>& numRecordsIn,
        std::shared_ptr<omnistream::SimpleCounter>& numBytesIn);
    OmniStreamTaskInput* CreateTaskInput(std::shared_ptr<CheckpointedInputGate> inputGate);
    std::shared_ptr<CheckpointedInputGate> CreateCheckpointedInputGate();
    static bool ShouldUseUnionInputGate(int taskType, size_t inputGateCount, bool hasUnionAllInput);
    static long GetChannelDeserializerId(const InputChannelInfo& channelInfo, bool useUnionAllChannelId);
    bool HasUnionAllInput() const;
};
} // namespace omnistream

#endif
