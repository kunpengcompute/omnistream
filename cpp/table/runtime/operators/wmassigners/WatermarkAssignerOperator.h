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

#include <chrono>
#include <regex>
#include "table/data/GenericRowData.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/Output.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/runtime/tasks/ProcessingTimeCallback.h"
#include "runtime/watermark/WatermarkStatus.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"

class WatermarkAssignerOperator : public AbstractStreamOperator<int>,
                                  public OneInputStreamOperator,
                                  public ProcessingTimeCallback {
public:
    explicit WatermarkAssignerOperator(
        Output*,
        int rowtimeIndex,
        int64_t outOfOrderT,
        int64_t idleTimeout,
        ProcessingTimeService* processingTimeService);
    ~WatermarkAssignerOperator() override = default;

    // Processing functions
    void processBatch(StreamRecord* input) override;
    void processElement(StreamRecord* element) override;
    void ProcessWatermark(Watermark* mark) override;
    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override
    {
        INFO_RELEASE("WatermarkAssignerOperator::initializeState not impl");
    }

    void notifyCheckpointComplete(long checkpointId) override
    {
        INFO_RELEASE("WatermarkAssignerOperator::notifyCheckpointComplete not impl checkpointId : " << checkpointId);
    }

    void notifyCheckpointAborted(long checkpointId) override
    {
        INFO_RELEASE("WatermarkAssignerOperator::notifyCheckpointAborted not impl checkpointId : " << checkpointId);
    }

    // Setup and closing operations
    void open() override;
    void finish() override;
    void close() override;

    const char* getName() override;

    std::string getTypeName() override;
    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override;
    void setSplitWaterMark(bool doSplitWaterMark);
    void processBatchSimple(StreamRecord* element);
    void processBatchWatermark(StreamRecord* element);

private:
    int rowtimeIndex_;
    bool splitWaterMark = false;
    int64_t outOfOrderTime_ = 4000;
    int64_t idleTimeout_;

    // Watermark Tracking
    int64_t lastWatermark_ = 0;
    int64_t currentWatermark_ = 0;
    int64_t lastRecordTime_ = 0;
    // This is an option retrieved by ExecutionConfig, not sure if it is relevant here.
    // For q0, this remains at 200
    int64_t emissionInterval_ = 200;

    WatermarkStatus* currentStatus;

    void OnProcessingTime(int64_t timestamp) override;
    int64_t currentWatermark(int64_t element_timestamp);
    void advanceWatermark();
    void emitWatermarkStatus(WatermarkStatus* watermarkStatus);
};
