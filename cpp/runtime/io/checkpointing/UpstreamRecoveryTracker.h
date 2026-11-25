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

#ifndef OMNISTREAM_UPSTREAMRECOVERYTRACKER_H
#define OMNISTREAM_UPSTREAMRECOVERYTRACKER_H

#include "runtime/partition/consumer/InputGate.h"
#include <unordered_set>

namespace omnistream {

class UpstreamRecoveryTracker {
public:
    virtual void handleEndOfRecovery(const InputChannelInfo& channelInfo) = 0;
    virtual bool allChannelsRecovered() const = 0;
    virtual void handleInnerRecover(const InputChannelInfo& channelInfo) {};
    virtual bool allInnerRecovered() const
    {
        return false;
    }
    static std::shared_ptr<UpstreamRecoveryTracker> forInputGate(std::shared_ptr<InputGate> inputGate);
    static std::shared_ptr<UpstreamRecoveryTracker> NO_OP();
};

class NoOpRecoveryTracker : public UpstreamRecoveryTracker {
public:
    void handleEndOfRecovery(const InputChannelInfo&) override
    {
    }
    bool allChannelsRecovered() const override
    {
        return true;
    }
};

class UpstreamRecoveryTrackerImpl : public UpstreamRecoveryTracker {
public:
    UpstreamRecoveryTrackerImpl(std::shared_ptr<InputGate> inputGate) : inputGate_(std::move(inputGate))
    {
        numUnrestoredChannels_ = inputGate_->GetNumberOfInputChannels();
        numUnrestoredInnerChannels_ = inputGate_->GetNumberOfInputChannels();
    }

    void handleEndOfRecovery(const InputChannelInfo& channelInfo) override
    {
        if (numUnrestoredChannels_ > 0) {
            auto result = restoredChannels_.insert(channelInfo);
            if (!result.second) {
                INFO_RELEASE("Channel already restored:" << channelInfo.toString());
                throw std::runtime_error("Channel already restored: " + channelInfo.toString());
            }
            --numUnrestoredChannels_;
            if (numUnrestoredChannels_ == 0) {
                for (const auto& info : inputGate_->getChannelInfos()) {
                    inputGate_->ResumeConsumption(info);
                }
                restoredChannels_.clear();
            }
        }
    }

    void handleInnerRecover(const InputChannelInfo& channelInfo) override
    {
        --numUnrestoredInnerChannels_;
    }

    bool allInnerRecovered() const override
    {
        INFO_RELEASE(
            "numUnrestoredInnerChannels_:" << numUnrestoredInnerChannels_ << "--"
                                           << inputGate_->GetNumberOfInputChannels());
        return numUnrestoredInnerChannels_ == 0;
    }

    bool allChannelsRecovered() const override
    {
        INFO_RELEASE(
            "numUnrestoredChannels_:" << numUnrestoredChannels_ << "--" << inputGate_->GetNumberOfInputChannels());
        return numUnrestoredChannels_ == 0;
    }

private:
    std::unordered_set<InputChannelInfo> restoredChannels_;
    int numUnrestoredChannels_;
    int numUnrestoredInnerChannels_;
    std::shared_ptr<InputGate> inputGate_;
};

inline std::shared_ptr<UpstreamRecoveryTracker> UpstreamRecoveryTracker::forInputGate(
    std::shared_ptr<InputGate> inputGate)
{
    return std::make_shared<UpstreamRecoveryTrackerImpl>(inputGate);
}

inline std::shared_ptr<UpstreamRecoveryTracker> UpstreamRecoveryTracker::NO_OP()
{
    static auto instance = std::make_shared<NoOpRecoveryTracker>();
    return instance;
}

} // namespace omnistream

#endif // OMNISTREAM_CHECKPOINTEDINPUTGATE_H
