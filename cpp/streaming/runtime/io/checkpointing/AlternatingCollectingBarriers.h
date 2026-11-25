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

#ifndef OMNISTREAM_ALTERNATINGCOLLECTINGBARRIERS_H
#define OMNISTREAM_ALTERNATINGCOLLECTINGBARRIERS_H

#include "AbstractAlternatingAlignedBarrierHandlerState.h"
#include "runtime/io/network/partition/consumer/CheckpointableInput.h"

class AlternatingCollectingBarriers : public AbstractAlternatingAlignedBarrierHandlerState {
public:
    explicit AlternatingCollectingBarriers(ChannelState state);
    ~AlternatingCollectingBarriers() override = default;

    BarrierHandlerState* AlignedCheckpointTimeout(Controller* controller, CheckpointBarrier* barrier) override;

    BarrierHandlerState* endOfPartitionReceived(Controller* controller, const InputChannelInfo& channelInfo);

protected:
    BarrierHandlerState* TransitionAfterBarrierReceived(ChannelState state) override;
};

#endif // OMNISTREAM_ALTERNATINGCOLLECTINGBARRIERS_H
