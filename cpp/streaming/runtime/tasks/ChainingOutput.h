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

#ifndef FLINK_TNEL_CHAININGOUTPUT_H
#define FLINK_TNEL_CHAININGOUTPUT_H

#include "WatermarkGaugeExposingOutput.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "streaming/api/operators/Input.h"
#include "streaming/runtime/metrics/WatermarkGauge.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "runtime/metrics/SimpleCounter.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "executiongraph/operatorchain/OperatorPOD.h"

class ChainingOutput : public WatermarkGaugeExposingOutput {
public:
    explicit ChainingOutput(Input*);
    explicit ChainingOutput(
        Input* op, const std::shared_ptr<omnistream::TaskMetricGroup>& metricGroup, omnistream::OperatorPOD& opConfig);
    void collect(void* record) override;
    void close() override;
    void emitWatermark(Watermark* watermark) override;
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override;

private:
    Input* operator_;
    WatermarkGauge watermarkGauge;
    // Only the previously announced status value is needed; the incoming WatermarkStatus is owned elsewhere.
    int announcedStatus;
    std::shared_ptr<omnistream::SimpleCounter> numRecordsIn;
    std::shared_ptr<omnistream::SimpleCounter> numBytesIn;

    // Counter *counter_;
};

#endif // FLINK_TNEL_CHAININGOUTPUT_H
