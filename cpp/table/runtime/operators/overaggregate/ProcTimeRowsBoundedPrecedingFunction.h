/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You can obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_PROCTIME_ROWS_BOUNDED_PRECEDING_FUNCTION_H
#define FLINK_TNEL_PROCTIME_ROWS_BOUNDED_PRECEDING_FUNCTION_H

#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "table/data/RowData.h"
#include "table/data/JoinedRowData.h"
#include "data/binary/BinaryRowData.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "table/types/logical/LogicalType.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "functions/OpenContext.h"
#include "functions/RuntimeContext.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"
#include "table/data/RowKind.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/typeutils/InternalTypeInfo.h"

using namespace omniruntime::type;

/**
 * C++ native counterpart of Flink's
 * org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction
 * (processing-time ROWS-bounded OVER window):
 *
 *   SELECT ..., SUM(price) OVER (PARTITION BY bidder ORDER BY proctime
 *          ROWS BETWEEN <n> PRECEDING AND CURRENT ROW) FROM bid
 *
 * The window holds at most precedingOffset rows per partition key. For each
 * input row, in the row's partition:
 *   1. if the window is full, retract the oldest buffered row from every
 *      aggregate function (the buffer is a time-bucketed map state, see
 *      inputState) and advance the oldest-timestamp watermark,
 *   2. otherwise grow the fill counter,
 *   3. buffer the input row in its ingestion-time bucket,
 *   4. accumulate the row into every aggregate function,
 *   5. write the accumulator back,
 *   6. emit (input row || current aggregate values) as an INSERT row.
 *
 * State layout mirrors the Java operator exactly (names included), so the
 * native state backend stays structurally equivalent to native Flink; no
 * batch/columnar data is ever written into the state backend:
 *   - ValueState<RowData> "accState"          accumulator row
 *   - MapState<Long, List<RowData>> "inputState"  window rows, bucketed by
 *     ingestion time; same-millisecond rows keep FIFO order inside a bucket
 *   - ValueState<Long> "processedCountState"  buffered-row counter (capped at
 *     precedingOffset)
 *   - ValueState<Long> "smallestTSState"      ingestion time of the oldest
 *     buffered row (Long.MAX_VALUE when all buckets drained, as in Java)
 *
 * Java's onTimer only performs TTL state cleanup (registerProcessingCleanupTimer
 * with stateCleaningEnabled == false by default); v1 does not model state TTL,
 * matching the ProcTimeUnboundedPrecedingFunction v1 boundary.
 */
class ProcTimeRowsBoundedPrecedingFunction : public KeyedProcessFunction<RowData*, RowData*, RowData*> {
public:
    ProcTimeRowsBoundedPrecedingFunction(long stateRetentionTime, const nlohmann::json& config);
    ~ProcTimeRowsBoundedPrecedingFunction() override;

    void open(const Configuration& parameters) override;

    /** Single-row entry point (also used by the unit tests). */
    void processElement(RowData* input, Context* ctx, TimestampedCollector* out) override;

    /** Vectorised entry point used by the KeyedProcessOperator. */
    void processBatch(
        omnistream::VectorBatch* inputBatch,
        KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override;

    JoinedRowData* getResultRow() override;

    ValueState<RowData*>* getValueState() override;

    void close() override;

private:
    /** Validates and extracts inputTypes / grouping / aggInfoList / precedingOffset. */
    void parseDescription(const nlohmann::json& config);

    /**
     * Same slot-assignment scheme as ProcTimeUnboundedPrecedingFunction:
     * builds the aggregate functions for aggInfoList.aggregateCalls
     * (SUM/SUM0/COUNT/MAX/MIN) and assigns accumulator / output-value slots.
     */
    void initAggFunctions(int& accStartingIndex, int& aggValueIndex);

    /** Steps 1-6 of the walk-through for one row of one key. */
    void processRowForKey(RowData* input, RowData* key, long currentTime);

    /** Copies the aggregated values into the reusable aggValue row. */
    void materialiseAggValue();

    /** Materialises row @p rowId of @p batch as an owned BinaryRowData. */
    RowData* materialiseRow(omnistream::VectorBatch* batch, int rowId);

    /** Turns the collected rows into a VectorBatch and emits it; frees the rows. */
    void flushCollected(TimestampedCollector& out);

    // ---- description (parsed once in the constructor) ----
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    std::vector<std::string> accTypes;
    std::vector<std::string> aggValueTypes;
    std::vector<int> groupingIndices;
    std::vector<omniruntime::type::DataTypeId> inputRowTypes;
    nlohmann::json aggInfoList;
    long precedingOffset = 0; // ROWS BETWEEN <n> PRECEDING AND CURRENT ROW, n > 0
    long stateRetentionTime;

    // ---- aggregate machinery ----
    std::vector<AggsHandleFunction*> functions;
    int accumulatorArity = 0;
    int aggValueArity = 0;
    KeySelector<RowData*>* groupByKeySelector = nullptr;

    // ---- runtime state ----
    int backend = 0; // 0: heap/memory, 1: bss, 2: rocksdb
    ValueState<RowData*>* accState = nullptr;
    MapState<int64_t, std::vector<RowData*>*>* inputState = nullptr;
    ValueState<int64_t>* counterState = nullptr;
    ValueState<int64_t>* smallestTsState = nullptr;
    BinaryRowData* reuseAggValue = nullptr;        // reusable "aggregate values" row
    JoinedRowData* resultRow = nullptr;            // (input || aggValue) output row

    // ---- output collection (batched, TopN-style) ----
    std::vector<RowData*> collectedRows;           // owned materialised input rows
    std::vector<RowData*> collectedAggValues;       // owned per-row aggValue snapshots
    std::vector<RowKind> collectedRowKinds;
    omnistream::VectorBatch* createOutputBatch();
    void collectOutputBatch(TimestampedCollector& out, omnistream::VectorBatch* outputBatch);
};

#endif // FLINK_TNEL_PROCTIME_ROWS_BOUNDED_PRECEDING_FUNCTION_H
