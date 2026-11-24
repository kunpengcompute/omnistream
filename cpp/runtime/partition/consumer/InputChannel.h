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

// InputChannel.h
#ifndef INPUT_CHANNEL_H
#define INPUT_CHANNEL_H

#include <memory>
#include <atomic>
#include <optional>
#include <stdexcept>
#include <event/TaskEvent.h>
#include <metrics/Counter.h>

#include "BufferAndAvailability.h"
#include "executiongraph/descriptor/ResultPartitionIDPOD.h"
#include "InputChannelInfo.h"
#include "partition/virtual_enable_shared_from_this_base.h"
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "event/TaskEvent.h"
#include "checkpoint/channel/ChannelStateWriter.h"

namespace omnistream {
class SingleInputGate;

class InputChannel : public virtual_enable_shared_from_this<InputChannel> {
public:
    InputChannel(
        std::shared_ptr<SingleInputGate> inputGate,
        int channelIndex,
        ResultPartitionIDPOD partitionId,
        int initialBackoff,
        int maxBackoff,
        std::shared_ptr<Counter> numBytesIn,
        std::shared_ptr<Counter> numBuffersIn);

    int getChannelIndex() const;
    int getGateIdx() const;
    InputChannelInfo getChannelInfo() const;
    InputChannelInfo& getChannelInfo();
    ResultPartitionIDPOD getPartitionId() const;

    virtual void resumeConsumption() = 0;
    virtual void TimeOutResumeConsumption() = 0;
    virtual void acknowledgeAllRecordsProcessed() = 0;

public:
    virtual void requestSubpartition(int subpartitionIndex) = 0;
    virtual std::optional<BufferAndAvailability> getNextBuffer() = 0;

    virtual void CheckpointStarted(
        const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter)
    {
    }

    virtual void CheckpointStopped(long checkpointId)
    {
    }

    virtual void notifyDataAvailable()
    {
    }

    virtual void ConvertToPriorityEvent(int sequenceNumber)
    {
    }

    virtual void sendTaskEvent(std::shared_ptr<TaskEvent> event) = 0;

    virtual bool isReleased() = 0;
    virtual void releaseAllResources() = 0;
    virtual void announceBufferSize(int newBufferSize) = 0;
    virtual int getBuffersInUseCount() = 0;
    virtual void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) = 0;
    virtual void SetPersistenceFlag(bool flag) = 0;
    virtual bool IsNeedPersistence() = 0;
    virtual void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info) = 0;

public:
    void checkError();
    void setError(std::exception_ptr cause);

    int getCurrentBackoff() const;
    bool increaseBackoff();

    void setConsumedSubpartitionIndex(int index)
    {
        consumedSubpartitionIndex = index;
    }

    std::shared_ptr<SingleInputGate> getInputGate()
    {
        return inputGate;
    }

public:
    virtual int unsynchronizedGetNumberOfQueuedBuffers()
    {
        return 0;
    }
    virtual int unsynchronizedGetSizeOfQueuedBuffers()
    {
        return 0;
    }
    virtual size_t getConsumedBufferSize() const
    {
        return 0;
    }
    virtual void setup() {};
    virtual std::string toString() {};
    static const int initBackoffConstant = 100;
    static const int maxBackoffConstant = 1000;

protected:
    InputChannelInfo channelInfo;
    ResultPartitionIDPOD partitionId;
    std::shared_ptr<SingleInputGate> inputGate;
    // std::atomic<std::shared_ptr<std::exception>> cause;
    std::atomic<bool> exception_occurred{false};
    std::exception_ptr cause;
    std::mutex exception_mutex;

    int initialBackoff;
    int maxBackoff;
    std::shared_ptr<Counter> numBytesIn;
    std::shared_ptr<Counter> numBuffersIn;
    int currentBackoff;
    int consumedSubpartitionIndex;

protected:
    void notifyChannelNonEmpty();
    void NotifyPriorityEvent(int priorityBufferNumber);
    virtual void notifyBufferAvailable(int numAvailableBuffers)
    {
    }
};

} // namespace omnistream

#endif // INPUT_CHANNEL_H
