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

#include "AlternatingWaitingForFirstBarrier.h"
#include "AlternatingWaitingForFirstBarrierUnaligned.h"
#include "AlternatingCollectingBarriers.h"

AlternatingWaitingForFirstBarrier::AlternatingWaitingForFirstBarrier(ChannelState state)
    : AbstractAlternatingAlignedBarrierHandlerState(std::move(state))
{
}

BarrierHandlerState* AlternatingWaitingForFirstBarrier::AlignedCheckpointTimeout(
    Controller* controller, CheckpointBarrier* barrier)
{
    state.PrioritizeAllAnnouncements();
    return new AlternatingWaitingForFirstBarrierUnaligned(true, state);
}

BarrierHandlerState* AlternatingWaitingForFirstBarrier::TransitionAfterBarrierReceived(ChannelState state)
{
    return new AlternatingCollectingBarriers(std::move(state));
}
