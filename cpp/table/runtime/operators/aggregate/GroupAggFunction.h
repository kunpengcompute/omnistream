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

#ifndef FLINK_TNEL_GROUP_AGG_FUNCTION_H
#define FLINK_TNEL_GROUP_AGG_FUNCTION_H

#include <nlohmann/json.hpp>
#include <vector>
#include "window/RecordCounter.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "table/runtime/generated/GeneratedAggsHandleFunctionMinMax.h"
#include "table/runtime/generated/GeneratedAggsHandleFunctionAverage.h"
#include "table/runtime/generated/GeneratedAggsHandleFunctionCount.h"
#include "table/runtime/generated/RecordEqualiser.h"
#include "table/data/util/RowDataUtil.h"
#include "table/data/RowData.h"
#include "table/types/logical/LogicalType.h"
#include "table/data/JoinedRowData.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "functions/OpenContext.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "functions/RuntimeContext.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/runtime/keyselector/KeySelector.h"

using namespace omniruntime::type;
using json = nlohmann::json;
struct DistinctInfo {
    std::vector<int> filterArgs;
    std::vector<int> argIndexes;
    std::vector<int> aggIndexes;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DistinctInfo, filterArgs, argIndexes, aggIndexes)
};

class GroupAggFunction : public KeyedProcessFunction<RowData*, RowData*, RowData*> {
public:
    struct RowInfo {
        int rowIndex;
        RowKind rowKind;
    };

    using GroupedRowsByKey = std::unordered_map<RowData*, std::vector<RowInfo>>;

    GroupAggFunction(long stateRetentionTime, const nlohmann::json& config);
    ~GroupAggFunction();
    void open(const Configuration& parameters) override;
    JoinedRowData* getResultRow() override;
    void processElement(RowData* input, Context* ctx, TimestampedCollector* out);
    void processBatch(
        omnistream::VectorBatch* inputBatch,
        KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out);
    void finish(KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx, TimestampedCollector& out);
    void processBatchColumnar(omnistream::VectorBatch* input, const std::vector<RowInfo>& groupInfo, RowData* pData);
    void close();
    ValueState<RowData*>* getValueState() override;
    static std::vector<std::int32_t> getKeyedTypes(
        const std::vector<int32_t> keyedIndex, const std::vector<std::string> inputTypes);
    omnistream::VectorBatch* createOutputBatch(
        std::vector<RowData*> collectedKeys, std::vector<RowData*> collectedValues, std::vector<RowKind> rowKinds);
    void collectOutputBatch(TimestampedCollector out, omnistream::VectorBatch* outputBatch);

    void InitAggFunctions(int& accStartingIndex, int& aggValueIndex);

    bool FirstRowAccumulate(std::vector<RowInfo>& groupInfo, RowData*& accumulators);
    void ClearEnv(
        omnistream::VectorBatch* input,
        std::vector<RowData*> resultKeys,
        std::vector<RowData*> resultValues,
        std::vector<RowKind> resultRowKinds,
        TimestampedCollector& out,
        std::unordered_map<RowData*, std::vector<RowInfo>> keyToRowIndices);

    void AssembleResultForBatch(
        RowData* accumulators,
        bool isEqual,
        bool firstRow,
        RowData* currentKey,
        std::vector<RowData*>& resultKeys,
        std::vector<RowData*>& resultValues,
        std::vector<RowKind>& resultRowKinds);
    void AssembleResultForElement(
        RowData* accumulators, bool isEqual, bool firstRow, RowData* currentKey, TimestampedCollector& out);
    void FillRowIndices(
        omnistream::VectorBatch* input,
        std::unordered_map<RowData*, std::vector<RowInfo>>& keyToRowIndices,
        int rowCount);
    bool EndAssemble(bool isEqual);
    void UpdateAccumulatorsInRocksDB(std::unordered_map<RowData*, RowData*>& pendingUpdates);

private:
    std::vector<std::string> accTypes;
    std::vector<std::string> aggValueTypes;
    std::unique_ptr<RecordCounter> recordCounter;
    bool generateUpdateBefore;
    long stateRetentionTime;
    int accumulatorArity = 0;
    int aggregateCallsCount = 0;
    std::vector<int> accumulatorsIdx;
    std::vector<AggsHandleFunction*> functions;
    JoinedRowData* resultRow = nullptr;
    omnistream::VectorBatch* resultBatch = nullptr;
    ValueState<RowData*>* accState = nullptr;
    using equalizerFuncType = bool (*)(RowData*, RowData*, int);
    std::vector<equalizerFuncType> equalisers;
    nlohmann::json description;
    BinaryRowData* reUsePrevAggValue = nullptr;
    BinaryRowData* reUseNewAggValue = nullptr;
    BinaryRowData* sharedAccmulators = nullptr;
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    std::vector<int32_t> keyedTypes;
    std::vector<int32_t> keyedIndex;
    std::vector<DistinctInfo> distinctInfos;
    KeySelector<RowData*>* groupByKeySelector = nullptr;
    int indexOfCountStar = -1;

    inline void setInt(
        omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec);
    inline void setLong(
        omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec);
    inline void setString(
        omniruntime::vec::VectorBatch* outputBatch, int numRows, int colIndex, std::vector<RowData*> vec);
    std::vector<std::string> handleInputTypes();
    void deleteRowData(vector<RowData*>& rowVector);
    int backend = 0; // 0: memory, 1: bss, 2: rocksdb
    // rocksdb update container
    std::unordered_map<RowData*, RowData*> pendingUpdates;
};

#endif // FLINK_TNEL_GROUP_AGG_FUNCTION_H
