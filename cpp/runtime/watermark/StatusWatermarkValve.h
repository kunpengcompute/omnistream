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

#ifndef OMNISTREAM_STATUSWATERMARKVALVE_H
#define OMNISTREAM_STATUSWATERMARKVALVE_H

#include <cstdint>
#include "WatermarkStatus.h"
#include "common.h"
#include "api/watermark/Watermark.h"

template <typename T>
class StatusWatermarkValve {
public:
    explicit StatusWatermarkValve(int32_t numInputChannels)
    {
        if (numInputChannels <= 0) {
            THROW_LOGIC_EXCEPTION("Number of input channels must be greater than 0");
        }

        for (auto i = 0; i < numInputChannels; i++) {
            channelStatuses_.emplace_back(InputChannelStatus(INT64_MIN, WatermarkStatus::active(), true));
        }

        lastOutputWatermark_ = INT64_MIN;
        lastOutputWatermarkStatus_ = WatermarkStatus::active();
    }

    void inputWatermark(Watermark* watermark, const int32_t channelIndex, T* output)
    {
        if (channelIndex < 0 || channelIndex >= channelStatuses_.size()) {
            THROW_LOGIC_EXCEPTION("Channel index out of range.");
        }

        // ignore the input watermark if its input channel, or all input channels are idle (i.e.
        // overall the valve is idle).
        if (!lastOutputWatermarkStatus_->IsActive() || !channelStatuses_[channelIndex].watermarkStatus->IsActive()) {
            return;
        }
        int64_t newWatermark = watermark->getTimestamp();

        if (newWatermark > channelStatuses_[channelIndex].watermark) {
            channelStatuses_[channelIndex].watermark = newWatermark;

            if (!channelStatuses_[channelIndex].isWatermarkAligned && newWatermark >= lastOutputWatermark_) {
                channelStatuses_[channelIndex].isWatermarkAligned = true;
            }

            findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
        }
    }

    void inputWatermarkStatus(WatermarkStatus* watermarkStatus, const int32_t channelIndex, T* output)
    {
        if (channelIndex < 0 || channelIndex >= channelStatuses_.size()) {
            THROW_LOGIC_EXCEPTION("Channel index out of range.");
        }

        bool isWatermarkStatusActive = watermarkStatus->IsActive();

        if (!isWatermarkStatusActive && channelStatuses_[channelIndex].watermarkStatus->IsActive()) {
            channelStatuses_[channelIndex].watermarkStatus = WatermarkStatus::idle();
            channelStatuses_[channelIndex].isWatermarkAligned = false;
            if (!InputChannelStatus::hasActiveChannels(channelStatuses_)) {
                if (channelStatuses_[channelIndex].watermark == lastOutputWatermark_) {
                    findAndOutputMaxWatermarkAcrossAllChannels(output);
                }
                lastOutputWatermarkStatus_ = WatermarkStatus::idle();
                output->emitWatermarkStatus(lastOutputWatermarkStatus_);
            } else if (channelStatuses_[channelIndex].watermark == lastOutputWatermark_) {
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        } else if (isWatermarkStatusActive && channelStatuses_[channelIndex].watermarkStatus->IsIdle()) {
            channelStatuses_[channelIndex].watermarkStatus = WatermarkStatus::active();

            if (channelStatuses_[channelIndex].watermark >= lastOutputWatermark_) {
                channelStatuses_[channelIndex].isWatermarkAligned = true;
            }

            if (lastOutputWatermarkStatus_->IsIdle()) {
                lastOutputWatermarkStatus_ = WatermarkStatus::active();
                output->emitWatermarkStatus(lastOutputWatermarkStatus_);
            }
        }
    }

private:
    struct InputChannelStatus {
        int64_t watermark = INT64_MIN;
        WatermarkStatus* watermarkStatus = WatermarkStatus::active();
        bool isWatermarkAligned = true;

        InputChannelStatus() = default;

        InputChannelStatus(int64_t watermark, WatermarkStatus* watermarkStatus, bool isWatermarkAligned)
            : watermark(watermark),
              watermarkStatus(watermarkStatus),
              isWatermarkAligned(isWatermarkAligned)
        {
        }

        static bool hasActiveChannels(const std::vector<InputChannelStatus>& channelStatuses)
        {
            for (auto& channelStatus : channelStatuses) {
                if (channelStatus.watermarkStatus->IsActive()) {
                    return true;
                }
            }
            return false;
        }
    };

    void findAndOutputNewMinWatermarkAcrossAlignedChannels(T* output)
    {
        int64_t newMinWatermark = INT64_MAX;
        bool hasAlignedChannels = false;
        for (auto& channelStatus : channelStatuses_) {
            if (channelStatus.isWatermarkAligned) {
                hasAlignedChannels = true;
                newMinWatermark = std::min(channelStatus.watermark, newMinWatermark);
            }
        }

        if (hasAlignedChannels && newMinWatermark > lastOutputWatermark_) {
            lastOutputWatermark_ = newMinWatermark;
            auto newWatermark = Watermark(lastOutputWatermark_);
            output->emitWatermark(&newWatermark);
        }
    }

    void findAndOutputMaxWatermarkAcrossAllChannels(T* output)
    {
        int64_t maxWatermark = INT64_MIN;

        for (auto& channelStatus : channelStatuses_) {
            maxWatermark = std::max(channelStatus.watermark, maxWatermark);
        }

        if (maxWatermark > lastOutputWatermark_) {
            lastOutputWatermark_ = maxWatermark;
            auto newWatermark = Watermark(lastOutputWatermark_);
            output->emitWatermark(&newWatermark);
        }
    }

    std::vector<InputChannelStatus> channelStatuses_;
    int64_t lastOutputWatermark_ = INT64_MIN;
    WatermarkStatus* lastOutputWatermarkStatus_ = nullptr;
};

#endif // OMNISTREAM_STATUSWATERMARKVALVE_H
