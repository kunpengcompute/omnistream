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

#ifndef OMNISTREAM_INDEXEDINPUTGATE_H
#define OMNISTREAM_INDEXEDINPUTGATE_H

#include <memory>
#include <vector>
#include <string>
#include <iostream>

#include "InputChannelInfo.h"
#include "runtime/io/network/partition/consumer/CheckpointableInput.h"
#include "InputChannel.h"
#include "InputGate.h"
#include "checkpoint/channel/ChannelStateWriter.h"
// check
namespace omnistream {
class QueueInputGateRunnable;

class IndexedInputGate : public InputGate, public CheckpointableInput {
public:
    IndexedInputGate() = default;
    ~IndexedInputGate() override = default;

    virtual int GetGateIndex() = 0;
    virtual std::vector<InputChannelInfo> getUnfinishedChannels() = 0;

    int GetInputGateIndex() override;
    void CheckpointStarted(const CheckpointBarrier& barrier) override;
    void CheckpointStopped(long checkpointId) override;
    void notifyDataAvailable() override;
    std::vector<InputChannelInfo> GetChannelInfos() override;
    void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter);
    void BlockConsumption(const InputChannelInfo& channelInfo) override
    { // checkinto related , do it ilater
    }
    void TimeOutResumeConsumption(const InputChannelInfo& channelInfo) override
    { // checkinto related , do it ilater
    }
    void ConvertToPriorityEvent(int channelIndex, int sequenceNumber)
    { // checkinto related , do it ilater
        getChannel(channelIndex)->ConvertToPriorityEvent(sequenceNumber);
    }
    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info);
    virtual int getBuffersInUseCount() = 0;
    virtual void announceBufferSize(int bufferSize) = 0;
    virtual void triggerDebloating()
    {
    }

    std::string toString() override
    {
        return "IndexedInputGate [gateIndex=" + std::to_string(GetGateIndex()) + "]";
    }
    std::shared_ptr<ChannelStateWriter> channelStateWriter_ = nullptr;
    friend class QueueInputGateRunnable;
};

} // namespace omnistream

#endif // OMNISTREAM_INDEXEDINPUTGATE_H
