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

#ifndef FLINK_TNEL_ASYNCDATAOUTPUTTOOUTPUT_H
#define FLINK_TNEL_ASYNCDATAOUTPUTTOOUTPUT_H

#include "../io/DataOutput.h"
#include "WatermarkGaugeExposingOutput.h"
#include "streaming/runtime/metrics/WatermarkGauge.h"

class AsyncDataOutputToOutput : public DataOutput {
public:
    AsyncDataOutputToOutput(Output* output)
    {
        this->output = output;
    }

    void emitRecord(StreamRecord* streamRecord)
    {
        output->collect(streamRecord);
    }

    void emitWatermark(Watermark* watermark) override
    {
        output->emitWatermark(watermark);
    }

private:
    Output* output;
    WatermarkGauge* watermarkGauge;
};

#endif
