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

#ifndef FLINK_TNEL_TIMESTAMPSANDWATERMARKS_H
#define FLINK_TNEL_TIMESTAMPSANDWATERMARKS_H

#include <memory>
#include <functional>
#include <chrono>
#include "core/api/connector/source/ReaderOutput.h"
#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "core/api/common/eventtime/TimestampAssigner.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "api/common/eventtime/WatermarkStrategy.h"

using OmniDataOutputPtr = omnistream::OmniPushingAsyncDataInput::OmniDataOutput*;

class TimestampsAndWatermarks {
public:
    class WatermarkUpdateListener {
    public:
        virtual void UpdateIdle(bool isIdle) = 0;
        virtual void UpdateCurrentEffectiveWatermark(long watermark) = 0;
        virtual ~WatermarkUpdateListener() = default;
    };

    // 纯虚函数声明
    virtual ReaderOutput* CreateMainOutput(OmniDataOutputPtr output, WatermarkUpdateListener* watermarkCallback) = 0;
    virtual void StartPeriodicWatermarkEmits()
    {
    }
    virtual void StopPeriodicWatermarkEmits()
    {
    }
    static std::shared_ptr<TimestampsAndWatermarks> CreateNoOpEventTimeLogic(
        const std::shared_ptr<WatermarkStrategy>& watermarkStrategy);
    static std::shared_ptr<TimestampsAndWatermarks> CreateProgressiveEventTimeLogic(
        const std::shared_ptr<WatermarkStrategy>& watermarkStrategy,
        ProcessingTimeService* timeService,
        long periodicWatermarkIntervalMillis);
    virtual ~TimestampsAndWatermarks() = default;
};

#endif // FLINK_TNEL_TIMESTAMPSANDWATERMARKS_H
