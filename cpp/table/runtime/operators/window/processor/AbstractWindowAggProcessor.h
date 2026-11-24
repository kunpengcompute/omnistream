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

#include "SlicingWindowProcessor.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/operators/aggregate/window/buffers/RecordsWindowBuffer.h"
#include "table/runtime/operators/window/state/WindowValueState.h"
#include "table/runtime/operators/window/LocalSlicingWindowAggOperator.h"
#include "table/runtime/keyselector/KeySelector.h"
#include <unordered_set>
#include <cstdint>
#include <memory>
#include "runtime/generated/function/WindowAggsHandleFunction.h"
#include "table/typeutils/InternalTypeInfo.h"

class AbstractWindowAggProcessor : public SlicingWindowProcessor<std::shared_ptr<RowData>, int64_t> {
public:
    using KeyType = std::shared_ptr<RowData>;

    AbstractWindowAggProcessor(nlohmann::json description, Output* output);
    ~AbstractWindowAggProcessor()
    {
        delete sliceAssigner;
    };
    void open(
        AbstractKeyedStateBackend<KeyType>* state,
        const nlohmann::json& config,
        StreamingRuntimeContext<KeyType>* runtimeCtx,
        InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService) override;
    void initializeWatermark(int64_t watermark) override;
    bool processBatch(omnistream::VectorBatch* key) override;
    void advanceProgress(long progress) override;
    void prepareCheckpoint() override;
    void fireWindow(int64_t window) override;
    void clearWindow(int64_t window) override;
    void close() override;
    TypeSerializer* createWindowSerializer() override;
    Output* getOutput() override;
    void SetOutputBatch(omnistream::VectorBatch* outputBatch, JoinedRowData* resultRow);
    void setClockService(ClockService* newClock);
    bool isWindowEmpty();
    RowData* GetHopResult(int64_t windowEnd, int64_t numSlices, int64_t interval);
    void NextWindowEndProcess(int64_t nextWindowEnd, SliceAssigner* assigner);
    RowData* GetNonHopResult(int64_t windowEnd);
    void ProcessHopResult(RowData* result);
    void ProcessNonHopResult(RowData* result);
    bool shouldDeleteWindowStateValue() const;

protected:
    std::unique_ptr<RecordsWindowBuffer> windowBuffer;
    int64_t currentProgress = INT64_MIN;
    int64_t nextTriggerProgress = INT64_MIN;
    int64_t windowInterval = 0;
    SliceAssigner* sliceAssigner = nullptr;
    int indexOfCountStar_ = -1;
    bool isEventTime;
    std::unique_ptr<NamespaceAggsHandleFunction<int64_t>> aggregator;
    std::unique_ptr<WindowValueState<KeyType, int64_t, RowData*>> windowState;
    Output* output;

    int accumulatorArity_ = 0;
    std::vector<std::string> accTypes_;
    AbstractKeyedStateBackend<KeyType>* stateBackend = nullptr;
    std::unique_ptr<JoinedRowData> resultRow = std::make_unique<JoinedRowData>();
    omnistream::VectorBatch* resultBatch = nullptr;
    std::unique_ptr<TimestampedCollector> collector;
    std::vector<std::string> outputTypes;
    std::vector<omniruntime::type::DataTypeId> outputTypeIds;
    std::unique_ptr<ClockService> clockService = std::make_unique<ClockService>();
    InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService = nullptr;
    std::vector<std::string> inputTypes;
    std::vector<int32_t> inputTypeIds_;
    std::vector<int32_t> keyedIndex;
    std::vector<int32_t> keyedTypes;
    std::unique_ptr<KeySelector<KeyType>> keySelector;

private:
    std::unordered_set<int64_t> uniqueData;
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;

    std::unique_ptr<NamespaceAggsHandleFunction<int64_t>> initNamespaceAggsHandleFunction(
        bool isWindowAgg, const nlohmann::json& aggInfoList);
};
