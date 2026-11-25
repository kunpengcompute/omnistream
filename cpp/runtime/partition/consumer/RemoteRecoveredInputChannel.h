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

#ifndef OMNISTREAM_REMOTERECOVEREDINPUTCHANNEL_H
#define OMNISTREAM_REMOTERECOVEREDINPUTCHANNEL_H

#include "RecoveredInputChannel.h"
#include "runtime/partition/ResultPartitionManager.h"
#include "RemoteInputChannel.h"
#include "iostream"
#include "checkpoint/channel/ChannelStateWriterImpl.h"

class RemoteRecoveredInputChannel : public RecoveredInputChannel {
public:
    RemoteRecoveredInputChannel(
        std::shared_ptr<SingleInputGate> inputGate,
        int channelIndex,
        ResultPartitionIDPOD partitionId,
        std::shared_ptr<omnistream::ResultPartitionManager> manager,
        int initialBackoff,
        int maxBackoff,
        int networkBuffersPerChannel,
        std::shared_ptr<Counter> numBytesIn,
        std::shared_ptr<Counter> numBuffersIn)
        : RecoveredInputChannel(
              inputGate,
              channelIndex,
              partitionId,
              consumedSubpartitionIndex,
              initialBackoff,
              maxBackoff,
              numBytesIn,
              numBuffersIn,
              networkBuffersPerChannel),
          partitionManager(manager)
    {
    }

    std::shared_ptr<omnistream::InputChannel> toInputChannelInternal()
    {
        auto stateWriter = GetChannelStateWriter();
        if (stateWriter == nullptr) {
            LOG("ERROR: RemoteRecoveredInputChannel.h stateWriter is null.");
            throw std::runtime_error("ERROR: RemoteRecoveredInputChannel.h stateWriter is null.");
        }

        return std::make_shared<omnistream::RemoteInputChannel>(
            inputGate,
            getChannelIndex(),
            partitionId,
            partitionManager,
            initialBackoff,
            maxBackoff,
            getNetworkBuffersPerChannel(),
            numBytesIn,
            numBuffersIn,
            stateWriter);
    }

private:
    std::shared_ptr<omnistream::ResultPartitionManager> partitionManager;
};

#endif // OMNISTREAM_REMOTERECOVEREDINPUTCHANNEL_H
