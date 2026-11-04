/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_WINDOWTABLEFUNCTIONOPERATOR_H
#define FLINK_TNEL_WINDOWTABLEFUNCTIONOPERATOR_H

#include <memory>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "streaming/api/watermark/Watermark.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/runtime/operators/window/assigners/WindowAssigner.h"
#include "table/utils/TimeWindowUtil.h"

/**
 * Native WindowTableFunctionOperator, ports Flink 1.16.3
 * org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator.
 *
 * It acts as a table-valued function to assign windows (TUMBLE/HOP/CUMULATE) for input rows.
 * Output rows include the original columns plus 3 columns: window_start, window_end, window_time.
 * Stateless and emits result per record instead of at the end of window.
 */
class WindowTableFunctionOperator : public AbstractStreamOperator<long>, public OneInputStreamOperator {
public:
    explicit WindowTableFunctionOperator(const nlohmann::json& config, Output* output);

    ~WindowTableFunctionOperator() override;

    void open() override;

    const char* getName() override;

    std::string getTypeName() override;

    void close() override;

    void processBatch(StreamRecord* input) override;

    void processElement(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void ProcessWatermark(Watermark* mark) override;

    void processWatermarkStatus(WatermarkStatus* watermarkStatus) override;

    void initializeState(StreamTaskStateInitializerImpl* initializer, TypeSerializer* keySerializer) override;

    void notifyCheckpointComplete(long checkpointId) override;

    void notifyCheckpointAborted(long checkpointId) override;

private:
    void processBatchInternal(omnistream::VectorBatch* batch);

    omnistream::VectorBatch* buildOutputBatch(
        omnistream::VectorBatch* inputBatch, const std::vector<std::vector<TimeWindow>>& windowsByRow);

    nlohmann::json description_;
    std::vector<std::string> inputTypeStrs_;
    std::vector<std::string> outputTypeStrs_;
    std::vector<omniruntime::type::DataTypeId> outputTypeIds_;
    WindowAssigner<TimeWindow>* windowAssigner_ = nullptr;
    int64_t rowtimeIndex_ = -1;
    std::string shiftTimeZone_ = "UTC";
    bool isEventTime_ = true;
    int64_t numNullRowTimeRecordsDropped_ = 0;
    TimestampedCollector* collector_ = nullptr;
};

#endif // FLINK_TNEL_WINDOWTABLEFUNCTIONOPERATOR_H
