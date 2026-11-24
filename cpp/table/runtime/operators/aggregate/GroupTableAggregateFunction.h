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

#ifndef FLINK_TNEL_GROUPTABLEAGGREGATEFUNCTION_H
#define FLINK_TNEL_GROUPTABLEAGGREGATEFUNCTION_H

#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "table/runtime/generated/TableAggsHandleFunction.h"
#include "table/runtime/generated/function/RowNumberFunction.h"
#include "table/data/RowData.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/types/logical/LogicalType.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "functions/OpenContext.h"
#include "functions/RuntimeContext.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/api/operators/TimestampedCollector.h"

/**
 * GroupBy (without window) TABLE aggregate operator, the native counterpart of
 * Flink's org.apache.flink.table.runtime.operators.aggregate.GroupTableAggFunction.
 *
 * Skeleton is copied from GroupAggFunction ("same input, same flow"), only the emission
 * section is replaced by the table-aggregate emitValue flow ("different ledger, different
 * board"):
 *
 *   processElement (mirrors Flink GroupTableAggFunction 1:1):
 *     1. accumulators = accState.value(); null => firstRow. Retraction input is
 *        dropped up front: the Top2 UDF declares no retract (plain Flink's codegen
 *        would fail on it), and the whitelist plan is append-only anyway.
 *     2. function.setAccumulators(acc)  -- the function loads its [first, second,
 *        ...] working copy from the accState row of the current group.
 *     3. if (!firstRow && generateUpdateBefore) emitValue(true)  -- retract the OLD board
 *        BEFORE accumulate, so the function still sees the old ranking.
 *     4. accumulate the input row (append-only: retract was dropped in step 1).
 *     5. function.getAccumulators(acc); accState.update(acc)  -- persist the new
 *        accumulator; then emitValue(false, firstRow) emits the NEW board (INSERT
 *        on first row, UPDATE_AFTER otherwise). The Top2 accumulator only grows,
 *        so the group never dies and accState is never cleared.
 *
 * Accumulator state (Scheme A, Flink state-shape parity): accState is a
 * ValueState<RowData> holding the accumulator row [first, second, ...] with
 * rankEnd BIGINT slots -- byte-identical to what plain Flink's codegen'd Top2
 * handler persists into its own accState, so checkpoints/savepoints are
 * engine-interoperable (a Java job and a native job can resume each other's
 * state). The handler (RowNumberFunction) keeps no private state of its own;
 * its working copy is loaded/stored through setAccumulators/getAccumulators.
 *
 * v1 scope: single ROW_NUMBER aggregate call. Multiple table aggregate calls extend the
 * creation section into an if-chain/factory, mirroring GroupAggFunction::InitAggFunctions.
 */
class GroupTableAggregateFunction : public KeyedProcessFunction<RowData*, RowData*, RowData*> {
public:
    GroupTableAggregateFunction(long stateRetentionTime, const nlohmann::json& config);

    ~GroupTableAggregateFunction() override;

    void open(const Configuration& parameters) override;

    void processElement(RowData* input, Context* ctx, TimestampedCollector* out) override;

    /**
     * Native-chain batch path (GroupAggFunction pattern). The chained downstream
     * (e.g. StreamCalcBatch) expects ONE VectorBatch per operator output, not
     * row-at-a-time RowData collects (the latter segfaults in
     * VectorBatch::RearrangeColumns). Every input row still runs the full
     * processElement flow (Top2 semantics: each row re-emits the whole board),
     * but the board rows are collected and assembled into a single output batch.
     */
    void processBatch(
        omnistream::VectorBatch* inputBatch,
        KeyedProcessFunction<RowData*, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override;

    void close() override;

    /** Table aggregate emits 0..N rows per input via emitValue; no reusable single row. */
    JoinedRowData* getResultRow() override
    {
        return nullptr;
    }

    ValueState<RowData*>* getValueState() override
    {
        return accState;
    }

private:
    /** Parse the plan JSON: rankRange / sort fields / unique key / grouping. */
    void parseDescription(const nlohmann::json& config);

    /**
     * Batch-path core: the same five steps as processElement, but the old/new boards
     * are collected into vectors (emitValueToRows) instead of being collected
     * row-at-a-time. rowKeys tracks one key RowData per input row for cleanup
     * (resultKeys may hold the same pointer twice: old board + new board).
     */
    void processElementToRows(
        RowData* input,
        Context* ctx,
        std::vector<RowData*>& rowKeys,
        std::vector<RowData*>& resultKeys,
        std::vector<RowData*>& resultAggRows,
        std::vector<RowKind>& resultRowKinds);

    /**
     * Assemble the collected rows into ONE output VectorBatch:
     * [grouping key columns | value, rank] (GroupAggFunction::createOutputBatch
     * pattern; all v1 columns are int64-backed). RowKinds ride on the batch rows.
     */
    omnistream::VectorBatch* createOutputBatch(
        const std::vector<RowData*>& resultKeys,
        const std::vector<RowData*>& resultAggRows,
        const std::vector<RowKind>& resultRowKinds);

    // ---- plan configuration ----
    long stateRetentionTime;
    nlohmann::json description;
    bool generateUpdateBefore = false;

    // ROW_NUMBER plan fields (naming aligned with AbstractTopNFunction where possible)
    int sortKeyIndex = -1;
    std::string sortKeyType;
    bool sortAscending = false;
    long rankStart = 1;
    long rankEnd = 1;
    int uniqueKeyIndex = -1;
    std::string uniqueKeyType;
    int filterIndex = -1;

    // grouping (batch-path key extraction)
    std::vector<int32_t> keyedIndex;
    std::vector<int32_t> keyedTypes;
    KeySelector<RowData*>* groupByKeySelector = nullptr;

    // output row type [grouping key | value, rank] (batch-path output assembly)
    std::vector<std::string> outputTypes;

    // ---- runtime state ----
    ValueState<RowData*>* accState = nullptr;
    // RocksDB + falcon cache DISABLED (state-cache-sizeLimit == 0, the default):
    // RocksdbValueState::value() hands back a fresh heap copy on every call and
    // update() only serializes it, so the CALLER owns the object and frees it
    // after the final update (GroupAggFunction backend==2 pendingUpdates-delete
    // contract) -- otherwise it leaks once per input row. Heap backend and
    // falcon-ENABLED RocksDB own the object themselves (backend store /
    // stateCache->put), so it must NOT be freed in those modes.
    bool stateValueCallerOwned_ = false;
    TableAggsHandleFunction* function = nullptr; // v1: single ROW_NUMBER call
    // Concrete-type view of `function` for the batch path: emitValueToRows is a
    // RowNumberFunction extension, not part of the TableAggsHandleFunction contract.
    RowNumberFunction* rowNumberFunction = nullptr;
    int accumulatorArity = 0; // arity of the accState row (= rankEnd BIGINT slots)
};

#endif // FLINK_TNEL_GROUPTABLEAGGREGATEFUNCTION_H
