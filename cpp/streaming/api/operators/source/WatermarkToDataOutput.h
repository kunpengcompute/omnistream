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

#ifndef OMNISTREAM_WATERMARKTODATAOUTPUT_H
#define OMNISTREAM_WATERMARKTODATAOUTPUT_H

#include "core/api/common/eventtime/WatermarkOutput.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "TimestampsAndWatermarks.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

class WatermarkToDataOutput : public WatermarkOutput {
public:
    WatermarkToDataOutput(OmniDataOutputPtr output, TimestampsAndWatermarks::WatermarkUpdateListener* watermarkEmitted)
        : output(output),
          watermarkEmitted(watermarkEmitted),
          maxWatermarkSoFar(INT64_MIN),
          isIdle(false)
    {
    }

    void emitWatermark(Watermark* watermark) override
    {
        const long newWatermark = watermark->getTimestamp();
        if (newWatermark <= maxWatermarkSoFar) {
            return;
        }

        maxWatermarkSoFar = newWatermark;
        watermarkEmitted->UpdateCurrentEffectiveWatermark(maxWatermarkSoFar);

        MarkActiveInternally();
        output->emitWatermark(watermark);
    }

    void MarkIdle() override
    {
        if (isIdle) {
            return;
        }

        output->emitWatermarkStatus(WatermarkStatus::idle());
        watermarkEmitted->UpdateIdle(true);
        isIdle = true;
    }

    void MarkActive() override
    {
        MarkActiveInternally();
    }

private:
    OmniDataOutputPtr output;
    TimestampsAndWatermarks::WatermarkUpdateListener* watermarkEmitted;
    long maxWatermarkSoFar;
    bool isIdle;

    bool MarkActiveInternally()
    {
        if (!isIdle) {
            return true;
        }

        output->emitWatermarkStatus(WatermarkStatus::active());
        watermarkEmitted->UpdateIdle(false);
        isIdle = false;
        return false;
    }
};

#endif // OMNISTREAM_WATERMARKTODATAOUTPUT_H
