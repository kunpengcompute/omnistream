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

#pragma once

#include "WatermarkGenerator.h"

class BoundedOutOfOrdernessWatermarks : public WatermarkGenerator {
public:
    explicit BoundedOutOfOrdernessWatermarks(long outOfOrdernessMillis)
        : maxTimestamp(INT64_MIN + outOfOrdernessMillis + 1),
          outOfOrdernessMillis(outOfOrdernessMillis)
    {
    }

    void OnEvent(void* event, long eventTimestamp, WatermarkOutput* output) override
    {
        maxTimestamp = std::max(maxTimestamp, eventTimestamp);
    }

    void OnPeriodicEmit(WatermarkOutput* output) override
    {
        lastEmittedMaxTimestamp_ = maxTimestamp;
        auto watermark = Watermark(maxTimestamp - outOfOrdernessMillis - 1);
        output->emitWatermark(&watermark);
    }

    int64_t getLastEmittedMaxTimestamp() override
    {
        return lastEmittedMaxTimestamp_;
    }

    ~BoundedOutOfOrdernessWatermarks() override = default;

private:
    long maxTimestamp;
    const long outOfOrdernessMillis;
    int64_t lastEmittedMaxTimestamp_ = INT64_MIN;
};
