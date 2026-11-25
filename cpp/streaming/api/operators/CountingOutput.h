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

#ifndef FLINK_TNEL_COUNTINGOUTPUT_H
#define FLINK_TNEL_COUNTINGOUTPUT_H

#include <memory>
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/metrics/Counter.h"
#include "runtime/metrics/SimpleCounter.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/tasks/WatermarkGaugeExposingOutput.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"

class CountingOutput : public WatermarkGaugeExposingOutput {
public:
    CountingOutput(
        WatermarkGaugeExposingOutput* output,
        const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup,
        omnistream::OperatorPOD& opConfig);

    void emitWatermark(Watermark* mark) override;
    void collect(void* record) override;
    void close() override;
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;

private:
    WatermarkGaugeExposingOutput* output;
    std::shared_ptr<omnistream::SimpleCounter> numRecordsOut;
};

class BytesCountingOutput : public WatermarkGaugeExposingOutput {
public:
    BytesCountingOutput(
        WatermarkGaugeExposingOutput* output,
        const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup,
        omnistream::OperatorPOD& opConfig);

    void emitWatermark(Watermark* mark) override;
    void collect(void* record) override;
    void close() override;
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;

private:
    WatermarkGaugeExposingOutput* output;
    std::shared_ptr<omnistream::SimpleCounter> numBytesOut;
};

#endif // FLINK_TNEL_COUNTINGOUTPUT_H
