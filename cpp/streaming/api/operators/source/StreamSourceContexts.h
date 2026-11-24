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

#ifndef FLINK_TNEL_STREAMSOURCECONTEXTS_H
#define FLINK_TNEL_STREAMSOURCECONTEXTS_H

#include "streaming/api/TimeCharacteristic.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "SwitchingOnClose.h"
#include "ManualWatermarkContext.h"
#include "AutomaticWatermarkContext.h"
#include "NonTimestampContext.h"
#include "functions/SourceContext.h"

class StreamSourceContexts {
public:
    static SourceContext* getSourceContext(
        TimeCharacteristic timeCharacteristic,
        ProcessingTimeService* processingTimeService,
        Object* checkpointLock,
        Output* output,
        int64_t watermarkInterval,
        int64_t idleTimeout,
        bool emitProgressiveWatermarks,
        bool isStream = false)
    {
        SourceContext* ctx = nullptr;
        switch (timeCharacteristic) {
            case EventTime:
                ctx = new ManualWatermarkContext(
                    output, processingTimeService, checkpointLock, idleTimeout, emitProgressiveWatermarks);

                break;
            case IngestionTime:
                if (!emitProgressiveWatermarks) {
                    THROW_LOGIC_EXCEPTION(

                        "Ingestion time is not available when emitting progressive watermarks is disabled.");
                }
                ctx = new AutomaticWatermarkContext(
                    output, watermarkInterval, processingTimeService, checkpointLock, idleTimeout);
                break;
            case ProcessingTime: ctx = new NonTimestampContext(checkpointLock, output, isStream); break;
            default: THROW_LOGIC_EXCEPTION("timeCharacteristic is Illegal.");
        }
        return new SwitchingOnClose(ctx);
    }
};

#endif // FLINK_TNEL_STREAMSOURCECONTEXTS_H
