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

#ifndef OMNISTREAM_WAITINGFORFIRSTBARRIER_H
#define OMNISTREAM_WAITINGFORFIRSTBARRIER_H

#include "runtime/io/checkpointing/BarrierHandlerState.h"
#include "runtime/io/checkpointing/ChannelState.h"

/**
 * Aligned-only checkpoint barrier handler state.
 *
 * In aligned mode, once a barrier is received on an input channel we (optionally)
 * block that channel until barriers from all input channels arrive. only after
 * all barriers are received we trigger the checkpoint, then unblock all channels.
 */

class WaitingForFirstBarrier : public BarrierHandlerState {
public:
    explicit WaitingForFirstBarrier(ChannelState state);

    BarrierHandlerState* BarrierReceived(
        Controller* controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier* barrier,
        bool markChannelBlocked) override;

    BarrierHandlerState* AlignedCheckpointTimeout(Controller* controller, CheckpointBarrier* barrier) override;

    BarrierHandlerState* FinishCheckpoint() override;
    void AbortCheckpoint() override;
    BarrierHandlerState* FinishSavepoint(long id);

private:
    ChannelState state_;
};

#endif // OMNISTREAM_WAITINGFORFIRSTBARRIER_H
