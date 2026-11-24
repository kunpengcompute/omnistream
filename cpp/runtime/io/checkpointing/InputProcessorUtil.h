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

#ifndef OMNISTREAM_INPUTPROCESSORUTIL_H
#define OMNISTREAM_INPUTPROCESSORUTIL_H
#pragma once

#include <vector>
#include <memory>
#include <stdexcept>
#include <string>
#include <numeric>

#include "runtime/io/checkpointing/CheckpointedInputGate.h"
#include "runtime/io/checkpointing/SingleCheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "streaming/runtime/tasks/TimerService.h"
#include "streaming/runtime/io/OmniStreamTaskSourceInput.h"
#include "partition/consumer/RemoteInputChannel.h"
#include "metrics/SystemClock.h"

namespace omnistream {

class InputProcessorUtil {
public:
    static std::vector<std::shared_ptr<CheckpointedInputGate>> CreateCheckpointedMultipleInputGate(
        const std::shared_ptr<MailboxExecutor>& mailboxExecutor,
        const std::vector<std::vector<std::shared_ptr<IndexedInputGate>>>& inputGateGroups,
        const std::shared_ptr<CheckpointBarrierHandler>& barrierHandler,
        bool graphContainsLoops = false)
    {
        std::vector<std::shared_ptr<CheckpointedInputGate>> checkpointedInputGates;

        // 1. Flatten groups into a single vector
        std::vector<std::shared_ptr<IndexedInputGate>> unionedInputGates;
        for (const auto& group : inputGateGroups) {
            unionedInputGates.insert(unionedInputGates.end(), group.begin(), group.end());
        }

        // 2. Wrap each InputGate with CheckpointedInputGate
        for (const auto& inputGate : unionedInputGates) {
            auto checkpointedGate = std::make_shared<CheckpointedInputGate>(inputGate, barrierHandler, mailboxExecutor);

            checkpointedInputGates.push_back(checkpointedGate);
        }

        return checkpointedInputGates;
    }

    static std::shared_ptr<CheckpointBarrierHandler> CreateCheckpointBarrierHandler(
        CheckpointableTask* toNotifyOnCheckpoint,
        const std::string& taskName,
        const std::shared_ptr<SubtaskCheckpointCoordinator>& coordinator,
        const std::shared_ptr<MailboxExecutor>& mailboxExecutor,
        const std::shared_ptr<omnistream::runtime::TimerService>& timerService,
        const std::vector<std::vector<std::shared_ptr<IndexedInputGate>>>& inputGateGroups,
        const std::vector<std::shared_ptr<OmniStreamTaskSourceInput>>& sourceInputs,
        bool enableUnaligned,
        std::int64_t alignedCheckpointTimeoutMillis,
        bool enableCheckpointAfterTasksFinish)
    {
        std::vector<CheckpointableInput*> allInputs;

        for (const auto& group : inputGateGroups) {
            for (const auto& input : group) {
                allInputs.push_back(input.get());
            }
        }

        for (const auto& source : sourceInputs) {
            allInputs.push_back(source.get());
        }

        std::sort(allInputs.begin(), allInputs.end(), [](CheckpointableInput* a, CheckpointableInput* b) {
            return a->GetInputGateIndex() < b->GetInputGateIndex();
        });

        int totalChannels = 0;
        for (auto* input : allInputs) {
            totalChannels += static_cast<int>(input->GetChannelInfos().size());
        }

        // timer callback
        auto timerCallback = runtime::BarrierAlignmentUtil::createRegisterTimerCallback<std::function<void()>>(
            mailboxExecutor.get(), timerService.get());

        // Force aligned
        if (!enableUnaligned) {
            LOG("creates a aligned barrier handler");
            return runtime::SingleCheckpointBarrierHandler::aligned(
                taskName,
                toNotifyOnCheckpoint,
                SystemClock::GetInstance(),
                totalChannels,
                timerCallback,
                enableCheckpointAfterTasksFinish,
                allInputs);
        }

        // Flink 1.16.3 behavior:
        //  - aligned-checkpoint-timeout == 0  => Always Unaligned (no alignment attempt)
        //  - aligned-checkpoint-timeout > 0   => Aligned attempt + timeout => Unaligned
        if (alignedCheckpointTimeoutMillis == 0) {
            LOG("creates a unaligned barrier handler");
            return runtime::SingleCheckpointBarrierHandler::unaligned(
                taskName,
                toNotifyOnCheckpoint,
                coordinator.get(),
                SystemClock::GetInstance(),
                totalChannels,
                timerCallback,
                enableCheckpointAfterTasksFinish,
                allInputs);
        }

        LOG("creates a alternating barrier handler");
        return runtime::SingleCheckpointBarrierHandler::alternating(
            taskName,
            toNotifyOnCheckpoint,
            coordinator.get(),
            SystemClock::GetInstance(),
            totalChannels,
            timerCallback,
            enableCheckpointAfterTasksFinish,
            allInputs);
    }
};
} // namespace omnistream

#endif // OMNISTREAM_INPUTPROCESSORUTIL_H
