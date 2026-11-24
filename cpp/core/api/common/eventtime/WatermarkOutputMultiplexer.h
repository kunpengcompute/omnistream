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

#ifndef OMNISTREAM_WATERMARKOUTPUTMULTIPLEXER_H
#define OMNISTREAM_WATERMARKOUTPUTMULTIPLEXER_H

#include "CombinedWatermarkStatus.h"
#include "WatermarkOutput.h"

#include <memory>
#include <unordered_map>
#include <string>
#include <mutex>
#include <stdexcept>

class WatermarkOutputMultiplexer {
public:
    explicit WatermarkOutputMultiplexer(WatermarkOutput* underlyingOutput) : underlyingOutput(underlyingOutput)
    {
    }

    ~WatermarkOutputMultiplexer() = default;

    void RegisterNewOutput(const std::string& id)
    {
        if (watermarkPerOutputId.find(id) != watermarkPerOutputId.end()) {
            throw std::runtime_error("Already contains an output for ID " + id);
        }
        auto outputState = new omnistream::PartialWatermark();
        watermarkPerOutputId.emplace(id, outputState);
        combinedWatermarkStatus.Add(outputState);
    }

    bool UnregisterOutput(const std::string& id)
    {
        auto it = watermarkPerOutputId.find(id);
        if (it != watermarkPerOutputId.end()) {
            omnistream::PartialWatermark* watermark = it->second;
            combinedWatermarkStatus.Remove(watermark);
            delete watermark;
            watermarkPerOutputId.erase(it);
            if (watermarkPerOutputId.empty()) {
                underlyingOutput->MarkIdle();
            } else {
                UpdateCombinedWatermark();
            }
            return true;
        }
        return false;
    }

    WatermarkOutput* GetImmediateOutput(const std::string& outputId)
    {
        auto it = watermarkPerOutputId.find(outputId);
        if (it == watermarkPerOutputId.end()) {
            THROW_RUNTIME_ERROR("no output registered under id " + outputId);
        }
        return new ImmediateOutput(it->second, this);
    }

    WatermarkOutput* GetDeferredOutput(const std::string& outputId)
    {
        auto it = watermarkPerOutputId.find(outputId);
        if (it == watermarkPerOutputId.end()) {
            THROW_RUNTIME_ERROR("no output registered under id " + outputId);
        }
        return new DeferredOutput(it->second);
    }

    void OnPeriodicEmit()
    {
        UpdateCombinedWatermark();
    }

private:
    WatermarkOutput* underlyingOutput;
    // Non-owning index; combinedWatermarkStatus deletes the registered states.
    std::unordered_map<std::string, omnistream::PartialWatermark*> watermarkPerOutputId;
    omnistream::CombinedWatermarkStatus combinedWatermarkStatus;

    void UpdateCombinedWatermark()
    {
        if (watermarkPerOutputId.empty()) {
            return;
        }
        if (combinedWatermarkStatus.UpdateCombinedWatermark()) {
            auto watermark = Watermark(combinedWatermarkStatus.GetCombinedWatermark());
            underlyingOutput->emitWatermark(&watermark);
        } else if (combinedWatermarkStatus.IsIdle()) {
            underlyingOutput->MarkIdle();
        } else {
            underlyingOutput->MarkActive();
        }
    }

    class ImmediateOutput : public WatermarkOutput {
    public:
        ImmediateOutput(omnistream::PartialWatermark* state, WatermarkOutputMultiplexer* parent)
            : state_(state),
              parent_(parent)
        {
        }

        void emitWatermark(Watermark* watermark) override
        {
            long timestamp = watermark->getTimestamp();
            bool wasUpdated = state_->SetWatermark(timestamp);
            if (wasUpdated && timestamp > parent_->combinedWatermarkStatus.GetCombinedWatermark()) {
                parent_->UpdateCombinedWatermark();
            }
        }

        void MarkIdle() override
        {
            state_->SetIdle(true);
            parent_->UpdateCombinedWatermark();
        }

        void MarkActive() override
        {
            state_->SetIdle(false);
            parent_->UpdateCombinedWatermark();
        }

    private:
        omnistream::PartialWatermark* state_;
        WatermarkOutputMultiplexer* parent_;
    };

    class DeferredOutput : public WatermarkOutput {
    public:
        explicit DeferredOutput(omnistream::PartialWatermark* state) : state_(state)
        {
        }

        void emitWatermark(Watermark* watermark) override
        {
            state_->SetWatermark(watermark->getTimestamp());
        }

        void MarkIdle() override
        {
            state_->SetIdle(true);
        }

        void MarkActive() override
        {
            state_->SetIdle(false);
        }

    private:
        omnistream::PartialWatermark* state_;
    };
};

#endif // OMNISTREAM_WATERMARKOUTPUTMULTIPLEXER_H
