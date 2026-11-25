/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_ROWNUMBERFUNCTION_H
#define FLINK_TNEL_ROWNUMBERFUNCTION_H

#include <string>
#include <vector>
#include "../TableAggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
#include "../runtime/state/VoidNamespace.h"
#include "table/types/logical/LogicalType.h"

/**
 * Native handler for the ROW_NUMBER table aggregate (Top-N of values).
 *
 * Accumulator model = Scheme A (BinaryRowData slots, Flink state-shape parity):
 *   - ALL persistent state lives in the operator's ValueState<RowData> accState as a
 *     [first, second, ...] row of rankEnd BIGINT slots. This is byte-identical to what
 *     plain Flink's codegen'd Top2 handler persists (Top2Accum {first, second} as an
 *     accumulator row), so checkpoints/savepoints are engine-interoperable: a Java job
 *     can resume from a native checkpoint and vice versa.
 *   - NO MapView / ledger / sequence-number bookkeeping: a keyed MapState would store a
 *     different shape than the native accState and break that interoperability.
 *   - Empty slot sentinel = LLONG_MIN, mirroring the Top2 UDF's Long.MIN_VALUE
 *     accumulator initialization (a numeric minimum, NOT a SQL NULL).
 *
 * Semantics (v1 scope, the Top2 point-name whitelist; other plans fall back to Java):
 *   - sort key: BIGINT only, DESC only (the Top2 UDF accumulate() uses strict '>'
 *     comparisons to keep the largest values). Ascending or non-BIGINT throws at
 *     construction and the plan falls back to Java.
 *   - keyless only (uniqueKeyIndex must be -1): the acceptance plan's input row is
 *     [grouping key, UDF argument] with no identity column. Keyed/Top-N-member
 *     semantics belong to the Rank operator, not to a table aggregate.
 *   - accumulate is the Top2 UDF logic generalized to rankEnd slots: a strictly
 *     greater value shifts the weaker slots down one place; an equal value settles
 *     one slot lower, so the SAME value arriving twice ranks twice (plain Flink
 *     TableAggregateFunction semantics, no idempotence). A value not greater than
 *     any slot leaves the state untouched.
 *   - retract: the Top2 UDF declares no retract; the operator drops retraction
 *     messages before they reach this handler (the retract() here is defensive only).
 *   - emitValue: emits the occupied slots as 0..N rows of [currentKey, value, rn]
 *     (conditional output: an empty slot is not emitted).
 *     RowKind policy (retract-stream semantics, vanilla 1.16.3 baseline captured
 *     empirically; differs from GroupAggFunction's -U/+U changelog on purpose):
 *       isRetract=true  -> always DELETE (-D)   [generateUpdateBefore gates
 *                          WHETHER the old board is emitted, not its kind]
 *       isRetract=false -> always INSERT (+I)   [not firstRow-dependent]
 */
class RowNumberFunction : public TableAggsHandleFunction {
public:
    RowNumberFunction(
        int sortKeyIndex,
        const std::string& sortKeyType,
        bool sortAscending,
        long rankEnd,
        int uniqueKeyIndex,
        const std::string& uniqueKeyType,
        int aggFuncIndex = -1,
        int filterIndex = -1,
        bool generateUpdateBefore = true);

    ~RowNumberFunction() override = default;

    /** Contract hook; Scheme A needs no data view, the store is kept for future handlers. */
    void open(StateDataViewStore* store) override;

    // ---- TableAggsHandleFunction contract ----

    void accumulate(RowData* input) override;
    void retract(RowData* input) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void setAccumulators(RowData* accumulators) override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void cleanup() override;
    void close() override;

    /**
     * Emit the occupied slots as 0..N rows of [currentKey, value, rn].
     * RowKind policy is documented in the class comment.
     */
    void emitValue(TimestampedCollector* out, RowData* currentKey, bool isRetract, bool firstRow) override;

    /**
     * Collecting variant of emitValue for the operator's batch path. Instead of
     * collecting row-at-a-time through a TimestampedCollector (the chained native
     * downstream, e.g. StreamCalcBatch, expects ONE VectorBatch and segfaults in
     * VectorBatch::RearrangeColumns on a RowData), appends the aggregate part
     * ([value, rn] GenericRowData) of every occupied slot to aggRows and its RowKind
     * to kinds. The caller pairs these with the current key and assembles a single
     * output VectorBatch (GroupAggFunction::createOutputBatch pattern).
     */
    void emitValueToRows(
        std::vector<RowData*>& aggRows,
        std::vector<RowKind>& kinds,
        bool isRetract,
        bool firstRow);

    /** State-shape parity with Flink: rankEnd BIGINT slots in the accState row. */
    int accumulatorSlots() const override
    {
        return static_cast<int>(rankEnd);
    }

    /**
     * Top2 never retracts, so the accumulator only grows: the group cannot die.
     * (Flink parity: GroupTableAggFunction clears the accState only through the
     * record-counter on retraction, which the Top2 plan cannot produce.)
     */
    bool isGroupEmpty() const override
    {
        return false;
    }

private:
    /** Strict ordering: does value `a` rank strictly before `b`? (v1: DESC, a > b.) */
    static bool betterThan(long long a, long long b, bool ascending)
    {
        return ascending ? a < b : a > b;
    }

    // ---- plan configuration ----
    int sortKeyIndex;
    bool sortAscending;
    long rankEnd; // Top-N capacity N (rankStart fixed at 1 in v1)
    int aggFuncIndex;
    int filterIndex;
    bool hasFilter;
    bool generateUpdateBefore;

    // ---- contract-provided store (unused in Scheme A) ----
    StateDataViewStore* store = nullptr;

    // ---- in-memory working copy of the accState row ----
    // rankEnd BIGINT slots in rank order (index 0 = rank 1); EMPTY_SLOT = free.
    std::vector<long long> slots;

    /** typeIds of the emitted aggregate row [value, rn] (both BIGINT in v1). */
    std::vector<int> aggValueTypeIds;
};

#endif // FLINK_TNEL_ROWNUMBERFUNCTION_H
