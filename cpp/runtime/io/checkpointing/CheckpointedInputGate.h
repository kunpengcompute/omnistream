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
#ifndef OMNISTREAM_CHECKPOINTEDINPUTGATE_H
#define OMNISTREAM_CHECKPOINTEDINPUTGATE_H

#include <memory>
#include <optional>
#include <future>
#include <vector>
#include <string>
#include <iostream>
#include <stdexcept>
#include "runtime/partition/consumer/InputGate.h"
#include "runtime/partition/consumer/BufferOrEvent.h"
#include "streaming/runtime/tasks/mailbox/MailboxExecutor.h"
#include "runtime/io/checkpointing/CheckpointBarrierHandler.h"
#include "io/network/api/serialization/EventSerializer.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/io/checkpointing/UpstreamRecoveryTracker.h"

namespace omnistream {

class CheckpointedInputGate : public PullingAsyncDataInput<BufferOrEvent> {
public:
    CheckpointedInputGate(
        std::shared_ptr<InputGate> inputGate,
        std::shared_ptr<CheckpointBarrierHandler> barrierHandler,
        std::shared_ptr<MailboxExecutor> mailboxExecutor);

    CheckpointedInputGate(
        std::shared_ptr<InputGate> inputGate,
        std::shared_ptr<CheckpointBarrierHandler> barrierHandler,
        std::shared_ptr<MailboxExecutor> mailboxExecutor,
        std::shared_ptr<UpstreamRecoveryTracker> upstreamRecoveryTracker);

    ~CheckpointedInputGate() override;

    // This uses old CompletableFuture because the InputGate uses it. It might be problematic!
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;

    BufferOrEvent* PollNext() override;
    bool IsFinished() override;
    bool HasReceivedEndOfData() override;
    InputFormat getInputFormat() const;

    // Checkpoint-specific APIs
    std::shared_ptr<CompletableFutureV2<void>> GetAllBarriersReceivedFuture(long checkpointId);
    int GetNumberOfInputChannels() const;
    std::shared_ptr<InputChannel> GetChannel(int channelIndex);
    std::vector<InputChannelInfo> GetChannelInfos() const;
    bool AllChannelsRecovered() const;
    bool AllInnerChannelsRecovered() const;
    long GetLatestCheckpointId() const;
    long GetAlignmentDurationNanos() const;
    long GetCheckpointStartDelayNanos() const;
    void Close();
    BufferOrEvent* HandleEvent(BufferOrEvent* bufferOrEvent);
    BufferOrEvent* HandleEmptyBuffer();

private:
    std::shared_ptr<InputGate> inputGate_;
    std::shared_ptr<CheckpointBarrierHandler> barrierHandler_;
    std::shared_ptr<MailboxExecutor> mailboxExecutor_;
    std::shared_ptr<UpstreamRecoveryTracker> upstreamRecoveryTracker_;
    bool isFinished_;
};
} // namespace omnistream

#endif // OMNISTREAM_CHECKPOINTEDINPUTGATE_H
