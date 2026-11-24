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

#ifndef OMNISTREAM_BARRIERHANDLERSTATE_H
#define OMNISTREAM_BARRIERHANDLERSTATE_H

#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/partition/consumer/InputChannelInfo.h"

using omnistream::InputChannelInfo;

class Controller {
public:
    virtual ~Controller() = default;
    virtual bool AllBarriersReceived() const = 0;
    virtual const CheckpointBarrier* GetPendingCheckpointBarrier() const = 0;
    virtual void TriggerGlobalCheckpoint(const CheckpointBarrier& checkpointBarrier) = 0;
    virtual void InitInputsCheckpoint(const CheckpointBarrier& checkpointBarrier) = 0;
    virtual bool IsTimedOut(const CheckpointBarrier& barrier) = 0;
};

class BarrierHandlerState {
public:
    virtual ~BarrierHandlerState() = default;

    virtual BarrierHandlerState* BarrierReceived(
        Controller* controller, InputChannelInfo channelInfo, CheckpointBarrier* barrier, bool markChannelBlocked) = 0;

    virtual BarrierHandlerState* AlignedCheckpointTimeout(Controller* controller, CheckpointBarrier* barrier) = 0;

    virtual BarrierHandlerState* AnnouncementReceived(
        Controller* /*controller*/, InputChannelInfo /*channelInfo*/, int /*sequenceNumber*/)
    {
        return this;
    }

    virtual BarrierHandlerState* FinishCheckpoint() = 0;

    // Abort the pending checkpoint and resume only channels tracked as blocked by this state.
    virtual void AbortCheckpoint() = 0;
};

#endif // OMNISTREAM_BARRIERHANDLERSTATE_H
