/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_PROCTIME_UNBOUNDED_PRECEDING_FUNCTION_H
#define FLINK_TNEL_PROCTIME_UNBOUNDED_PRECEDING_FUNCTION_H

#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "table/data/RowData.h"
#include "table/data/JoinedRowData.h"
#include "data/binary/BinaryRowData.h"
#include "table/runtime/generated/AggsHandleFunction.h"
#include "table/types/logical/LogicalType.h"
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
 * org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction
 * (processing-time unbounded OVER window, i.e. prefix aggregation):
 *
 *   SELECT ..., SUM(price) OVER (PARTITION BY bidder ORDER BY proctime
 *          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM bid
 *
 * Per input row, for the row's partition key:
 *   1. fetch the key's accumulator from keyed state (create on first row),
 *   2. accumulate the row into every aggregate function,
 *   3. write the accumulator back (incremental, never recomputed),
 *   4. emit (input row || current aggregate values) as an INSERT row.
 *
 * Unlike GroupAggFunction this is append-only: every input row produces
 * exactly one output row, so there is no changelog bookkeeping
 * (no recordCounter, no equalisers, no generateUpdateBefore).
 *
 * v1 whitelist (enforced upstream by the OmniAdaptor Validate strategy,
 * re-checked here as a last line of defence):
 *   - aggregate functions: SUM / COUNT / MAX / MIN, no DISTINCT,
 *     no WithRetract variants, single argument (COUNT(*) has none),
 *   - frame: UNBOUNDED PRECEDING only,
 *   - state TTL cleanup is not implemented (matches the GroupAggFunction
 *     production path, which is also created with retention 0).
 */
class ProcTimeUnboundedPrecedingFunction : public KeyedProcessFunction<RowData*, RowData*, RowData*> {
public:
    ProcTimeUnboundedPrecedingFunction(long stateRetentionTime, const nlohmann::json& config);
    ~ProcTimeUnboundedPrecedingFunction() override;

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
    /** Validates and extracts inputTypes / grouping / aggInfoList. */
    void parseDescription(const nlohmann::json& config);

    /**
     * Slimmed-down version of GroupAggFunction::InitAggFunctions: builds the
     * aggregate functions for aggInfoList.aggregateCalls (SUM/COUNT/MAX/MIN
     * only) and assigns accumulator slots / output-value slots.
     */
    void initAggFunctions(int& accStartingIndex, int& aggValueIndex);

    /** Steps 1-5 of the walk-through for one row of one key. */
    void processRowForKey(RowData* input, RowData* key);

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
    long stateRetentionTime;

    // ---- aggregate machinery ----
    std::vector<AggsHandleFunction*> functions;
    int accumulatorArity = 0;
    int aggValueArity = 0;
    KeySelector<RowData*>* groupByKeySelector = nullptr;

    // ---- runtime state ----
    int backend = 0; // 0: heap/memory, 1: bss, 2: rocksdb
    ValueState<RowData*>* accState = nullptr;
    BinaryRowData* reuseAggValue = nullptr;        // reusable "aggregate values" row
    JoinedRowData* resultRow = nullptr;            // (input || aggValue) output row

    // ---- output collection (batched, TopN-style) ----
    std::vector<RowData*> collectedRows;           // owned materialised input rows
    std::vector<RowData*> collectedAggValues;       // owned per-row aggValue snapshots
    std::vector<RowKind> collectedRowKinds;
    omnistream::VectorBatch* createOutputBatch();
    void collectOutputBatch(TimestampedCollector& out, omnistream::VectorBatch* outputBatch);
};

#endif // FLINK_TNEL_PROCTIME_UNBOUNDED_PRECEDING_FUNCTION_H
