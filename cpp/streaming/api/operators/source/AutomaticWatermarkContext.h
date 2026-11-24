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

#ifndef FLINK_TNEL_AUTOMATICWATERMARKCONTEXT_H
#define FLINK_TNEL_AUTOMATICWATERMARKCONTEXT_H

#include "WatermarkContext.h"
#include "../Output.h"

class AutomaticWatermarkContext : public WatermarkContext {
public:
    AutomaticWatermarkContext(
        Output* output,
        int64_t watermarkInterval,
        ProcessingTimeService* timeService,
        Object* checkpointLock,
        int64_t idleTimeout)
        : WatermarkContext(timeService, checkpointLock, idleTimeout),
          output(output),
          watermarkInterval(watermarkInterval)
    {
        reuse = new StreamRecord();
        lastRecordTime = INT64_MAX;
        int64_t now = timeService->getCurrentProcessingTime();
        timeService->registerTimer(
            now + watermarkInterval, std::make_shared<WatermarkEmittingTask>(timeService, checkpointLock, output));
    }

    ~AutomaticWatermarkContext() override
    {
        delete reuse;
    }

    void close() override
    {
        WatermarkContext::close();
        if (nextWatermarkTimer != nullptr) {
            nextWatermarkTimer->cancel(true);
        }
    }

protected:
    void processAndCollect(void* element) override
    {
        lastRecordTime = this->timeService->getCurrentProcessingTime();
        output->collect(reuse->replace(element, lastRecordTime));
        if (lastRecordTime > nextWatermarkTime) {
            int64_t watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
            nextWatermarkTime = watermarkTime + watermarkInterval;
            auto watermark = Watermark(watermarkTime);
            output->emitWatermark(&watermark);
        }
    }

    void processAndCollectWithTimestamp(void* element, int64_t timestamp) override
    {
        processAndCollect(element);
    }

    bool allowWatermark(Watermark* mark) override
    {
        // allow Long.MAX_VALUE since this is the special end-watermark that for example the
        // Kafka source emits
        return mark->getTimestamp() == INT64_MAX && nextWatermarkTime != INT64_MAX;
    }

    void processAndEmitWatermark(Watermark* mark) override
    {
        nextWatermarkTime = INT64_MAX;
        output->emitWatermark(mark);

        if (nextWatermarkTimer != nullptr) {
            nextWatermarkTimer->cancel(true);
        }
    }

    void processAndEmitWatermarkStatus(WatermarkStatus* watermarkStatus) override
    {
        if (idle != watermarkStatus->IsIdle()) {
            output->emitWatermarkStatus(watermarkStatus);
        }
        idle = watermarkStatus->IsIdle();
    }

private:
    class WatermarkEmittingTask : public ProcessingTimeCallback {
    public:
        WatermarkEmittingTask(ProcessingTimeService* timeService, Object* lock, Output* output)
            : timeService(timeService),
              lock(lock),
              output(output)
        {
        }

        void OnProcessingTime(int64_t timestamp) override
        {
            NOT_IMPL_EXCEPTION;
        }

        ~WatermarkEmittingTask() override = default;

    private:
        ProcessingTimeService* timeService;
        Object* lock;
        Output* output;
    };

    Output* output;
    StreamRecord* reuse;
    long watermarkInterval;
    volatile ScheduledFuture* nextWatermarkTimer;
    volatile int64_t nextWatermarkTime{};
    volatile int64_t lastRecordTime;

    bool idle = false;
};

#endif
