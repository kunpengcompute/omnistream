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

#ifndef FLINK_TNEL_MANUALWATERMARKCONTEXT_H
#define FLINK_TNEL_MANUALWATERMARKCONTEXT_H

#include "WatermarkContext.h"
#include "../Output.h"

class ManualWatermarkContext : public WatermarkContext {
public:
    ManualWatermarkContext(
        Output* output,
        ProcessingTimeService* timeService,
        Object* checkpointLock,
        int64_t idleTimeout,
        bool emitProgressiveWatermarks)
        : WatermarkContext(timeService, checkpointLock, idleTimeout),
          emitProgressiveWatermarks(emitProgressiveWatermarks),
          output(output)
    {
        reuse = new StreamRecord();
    }

    ~ManualWatermarkContext() override
    {
        delete reuse;
    }

protected:
    void processAndCollect(void* element) override
    {
        output->collect(reuse->replace(element));
    }

    void processAndCollectWithTimestamp(void* element, int64_t timestamp) override
    {
        output->collect(reuse->replace(element, timestamp));
    }

    void processAndEmitWatermark(Watermark* mark) override
    {
        output->emitWatermark(mark);
    }

    void processAndEmitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        if (idle != watermarkStatus->IsIdle()) {
            output->emitWatermarkStatus(watermarkStatus);
        }
        idle = watermarkStatus->IsIdle();
    }

    bool allowWatermark(Watermark* mark) override
    {
        return emitProgressiveWatermarks || mark->getTimestamp() == INT64_MAX;
    }

private:
    bool emitProgressiveWatermarks;
    Output* output;
    StreamRecord* reuse;
    bool idle = false;
};

#endif // FLINK_TNEL_MANUALWATERMARKCONTEXT_H
