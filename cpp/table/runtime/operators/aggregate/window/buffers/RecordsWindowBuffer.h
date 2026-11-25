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

#include <unordered_map>
#include <vector>
#include <streaming/api/operators/TimestampedCollector.h>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/operators/window/WindowKey.h"
#include "table/runtime/operators/window/state/WindowValueState.h"
#include "test/core/operators/OutputTest.h"
#include <mutex>
#include <memory>
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "runtime/generated/function/WindowAggsHandleFunction.h"
#include "runtime/operators/InternalTimerServiceImpl.h"
#include "state/KeyedStateBackend.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/JoinedRowData.h"

using namespace omnistream;
class RecordsWindowBuffer {
public:
    using KeyType = std::shared_ptr<RowData>;

    RecordsWindowBuffer(
        const nlohmann::json& config,
        WindowValueState<KeyType, int64_t, RowData*>* state,
        Output* output,
        KeyedStateBackend<KeyType>* stateBackend_,
        SliceAssigner* sliceAssigner,
        InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService);
    RecordsWindowBuffer(const nlohmann::json& config, Output* output, SliceAssigner* sliceAssigner);
    void InitializeKeySelectorAndTypes(const nlohmann::json& config);
    void addVectorBatch(
        omnistream::VectorBatch* elementBatch, std::vector<int64_t>& sliceEndArr, std::vector<bool>& dropArr);
    void addVectorBatch(omnistream::VectorBatch* elementBatch, std::vector<int64_t>& sliceEndArr);
    void advanceProgress(long currentProgress);
    void flush();
    void close()
    {
        if (isWindowAgg) {
            delete collector;
            collector = nullptr;
        }
    };
    omnistream::VectorBatch* createOutputBatch(std::vector<std::unique_ptr<RowData>>& collectedRows);
    void collectOutputBatch(TimestampedCollector* out, omnistream::VectorBatch* outputBatch);
    std::vector<std::string> getKeyedTypes(std::vector<int32_t> keyedIndex, std::vector<std::string> inputTypes);
    Output* getOutput();
    void combineAccumulator(const WindowKey& windowKey, RowData* acc);
    void globalWinAggProcess(const WindowKey& currentWindowKey, std::vector<std::unique_ptr<RowData>>& sliceResultArr);
    void winAggProcess(const WindowKey& currentWindowKey, std::vector<int64_t>& sliceResultArr);
    bool shouldDeleteWindowStateValue() const;

private:
    static constexpr int AVG_ACCUMULATOR_SLOTS = 2; // AVG needs sum + count
    static constexpr int DEFAULT_ACCUMULATOR_SLOTS = 1;
    nlohmann::json description;
    std::unordered_map<WindowKey, std::vector<std::unique_ptr<RowData>>> globalRecordsBuffer;
    // optimization: localRecordsBuffer save the windowKey, and the vector of combinedId for the input row data.
    std::unordered_map<WindowKey, std::vector<int64_t>> localRecordsBuffer;
    int64_t recordsBufferSize_ = 0; // TODO: this is a temp fix, preventing the recordsBuffer be too large
    const int64_t recordsBufferSizeLimit_ =
        100000; // TODO: this is a temp fix, preventing the recordsBuffer be too large
    KeyedStateBackend<KeyType>* stateBackend_;
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;
    std::vector<std::string> inputTypes;
    std::vector<int32_t> inputTypeIds_;
    std::vector<std::string> outputTypes;
    std::vector<omniruntime::type::DataTypeId> outputTypeIds;
    std::vector<int32_t> keyedIndex;
    std::vector<int32_t> keyedTypes;
    std::unique_ptr<KeySelector<KeyType>> keySelector;
    int accumulatorArity = 0;
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> localFunctions;
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> globalFunctions;
    std::unique_ptr<WindowAggsHandleFunction> localAggregator;
    std::unique_ptr<WindowAggsHandleFunction> globalAggregator;
    int aggregateCallsCount = 0;
    omnistream::VectorBatch* resultBatch = nullptr;
    TimestampedCollector* collector;
    int rowTimeIndex;
    WindowValueState<KeyType, int64_t, RowData*>* accState;
    Output* output;
    std::unique_ptr<GenericRowData> windowRow;
    std::unique_ptr<JoinedRowData> accWindowRow;
    std::unique_ptr<JoinedRowData> resultRow;
    BinaryRowData* reUseAccumulator;
    InternalTimerServiceImpl<KeyType, int64_t>* internalTimerService;
    bool isWindowAgg;
    std::mutex bufferMutex;
    SliceAssigner* sliceAssigner;
    std::string shiftTimeZone;
    int64_t minSliceEnd = INT64_MAX;
    int currentBatchId = 0;
    int lastDeletedBatchIndex = 0;
    std::vector<omnistream::VectorBatch*> retainedBatches;

    void initNamespaceAggsHandleFunction(const nlohmann::json& aggInfoList);
    void SetLong(omniruntime::vec::VectorBatch* outputBatch, int rowIndex, int colIndex, RowData* collectedRow);
    void SetInt(omniruntime::vec::VectorBatch* outputBatch, int rowIndex, int colIndex, RowData* collectedRow);
    void SetStringVectorBatch(omnistream::VectorBatch* outputBatch, int rowIndex, int colIndex, RowData* collectedRow);
};
