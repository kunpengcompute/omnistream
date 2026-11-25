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

#include "IndexedInputGate.h"

namespace omnistream {

void IndexedInputGate::CheckpointStarted(const CheckpointBarrier& barrier)
{
    int num = GetNumberOfInputChannels();
    for (int i = 0; i < num; ++i) {
        if (getChannel(i)->IsNeedPersistence()) {
            getChannel(i)->SetPersistenceFlag(true);
        } else {
            getChannel(i)->SetPersistenceFlag(false);
        }
        getChannel(i)->CheckpointStarted(barrier, channelStateWriter_);
    }
}

void IndexedInputGate::AddInputData(long checkpointId, const omnistream::InputChannelInfo& info)
{
    int num = GetNumberOfInputChannels();
    for (int i = 0; i < num; ++i) {
        if (getChannel(i)->getChannelInfo() == info) {
            getChannel(i)->AddInputData(checkpointId, info);
            break;
        }
    }
}

void IndexedInputGate::CheckpointStopped(long checkpointId)
{
    int num = GetNumberOfInputChannels();
    for (int i = 0; i < num; ++i) {
        getChannel(i)->CheckpointStopped(checkpointId);
    }
}

void IndexedInputGate::notifyDataAvailable()
{
    int num = GetNumberOfInputChannels();
    for (int i = 0; i < num; ++i) {
        getChannel(i)->notifyDataAvailable();
    }
}

int IndexedInputGate::GetInputGateIndex()
{
    return GetGateIndex();
}

std::vector<InputChannelInfo> IndexedInputGate::GetChannelInfos()
{
    std::vector<InputChannelInfo> infos;
    for (int i = 0; i < GetNumberOfInputChannels(); ++i) {
        auto channel = getChannel(i);
        if (channel) {
            infos.emplace_back(channel->getChannelInfo());
        }
    }
    return infos;
}

void IndexedInputGate::SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter)
{
    int num = GetNumberOfInputChannels();
    for (int i = 0; i < num; ++i) {
        getChannel(i)->SetChannelStateWriter(channelStateWriter);
    }
    if (channelStateWriter_ == nullptr) {
        channelStateWriter_ = channelStateWriter;
    }
}
} // namespace omnistream
