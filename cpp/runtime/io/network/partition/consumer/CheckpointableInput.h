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

#ifndef OMNISTREAM_CHECKPOINTABLEINPUT_H
#define OMNISTREAM_CHECKPOINTABLEINPUT_H

#include <vector>
#include <stdexcept>
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/io/network/api/CheckpointBarrier.h"

class CheckpointableInput {
public:
    virtual ~CheckpointableInput() = default;

    virtual void BlockConsumption(const omnistream::InputChannelInfo& channelInfo) = 0;

    virtual void ResumeConsumption(const omnistream::InputChannelInfo& channelInfo) = 0;

    virtual void TimeOutResumeConsumption(const omnistream::InputChannelInfo& channelInfo) = 0;

    virtual std::vector<omnistream::InputChannelInfo> GetChannelInfos() = 0;

    virtual void CheckpointStarted(const CheckpointBarrier& barrier) = 0;

    virtual void CheckpointStopped(long cancelledCheckpointId) = 0;

    virtual void notifyDataAvailable() = 0;

    virtual int GetInputGateIndex() = 0;

    virtual void ConvertToPriorityEvent(int channelIndex, int sequenceNumber) = 0;
};

#endif // OMNISTREAM_CHECKPOINTABLEINPUT_H
