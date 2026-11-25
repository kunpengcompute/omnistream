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

#ifndef OMNISTREAM_CHANNELSTATE_H
#define OMNISTREAM_CHANNELSTATE_H

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "runtime/io/network/partition/consumer/CheckpointableInput.h"
#include "runtime/partition/consumer/InputChannelInfo.h"

using omnistream::InputChannelInfo;

class ChannelState {
public:
    explicit ChannelState(std::vector<CheckpointableInput*> inputs) : inputs(std::move(inputs))
    {
    }

    void BlockChannel(InputChannelInfo channelInfo)
    {
        if (blockedChannels.count(channelInfo) == 0) {
            inputs[channelInfo.getGateIdx()]->BlockConsumption(channelInfo);
            blockedChannels.insert(channelInfo);
        }
    }

    void ChannelFinished(InputChannelInfo channelInfo)
    {
        blockedChannels.erase(channelInfo);
        sequenceNumberInAnnouncedChannels.erase(channelInfo);
    }

    void PrioritizeAllAnnouncements()
    {
        for (auto& entry : sequenceNumberInAnnouncedChannels) {
            InputChannelInfo channelInfo = entry.first;
            inputs[channelInfo.getGateIdx()]->ConvertToPriorityEvent(channelInfo.getInputChannelIdx(), entry.second);
        }
        sequenceNumberInAnnouncedChannels.clear();
    }

    void UnblockAllChannels()
    {
        for (auto& channelInfo : blockedChannels) {
            inputs[channelInfo.getGateIdx()]->ResumeConsumption(channelInfo);
        }
        blockedChannels.clear();
    }

    void TimeOutUnblockAllChannels()
    {
        for (auto& channelInfo : blockedChannels) {
            inputs[channelInfo.getGateIdx()]->TimeOutResumeConsumption(channelInfo);
        }
        blockedChannels.clear();
    }

    ChannelState& EmptyState()
    {
        if (!blockedChannels.empty()) {
            throw std::runtime_error("Blocked channels exist during reset");
        }
        sequenceNumberInAnnouncedChannels.clear();
        return *this;
    }

    std::vector<CheckpointableInput*> getInputs() const
    {
        return inputs;
    }

    void removeSeenAnnouncement(InputChannelInfo channelInfo)
    {
        sequenceNumberInAnnouncedChannels.erase(channelInfo);
    }

    void addSeenAnnouncement(InputChannelInfo channelInfo, int sequenceNumber)
    {
        sequenceNumberInAnnouncedChannels[channelInfo] = sequenceNumber;
    }

private:
    std::vector<CheckpointableInput*> inputs;
    std::unordered_map<InputChannelInfo, int> sequenceNumberInAnnouncedChannels;
    std::unordered_set<InputChannelInfo> blockedChannels;
};

#endif // OMNISTREAM_CHANNELSTATE_H
