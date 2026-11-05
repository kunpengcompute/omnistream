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

#include "RowNumberFunction.h"

#include <climits>

#include "core/include/common.h"
#include "table/data/GenericRowData.h"
#include "table/data/JoinedRowData.h"

namespace {
// Mirrors the Top2 UDF's Long.MIN_VALUE sentinel: an empty rank slot is the numeric
// minimum (NOT a SQL NULL), exactly like the codegen'd Top2 accumulator row that
// plain Flink persists into accState.
constexpr long long EMPTY_SLOT = LLONG_MIN;
} // namespace

RowNumberFunction::RowNumberFunction(
    int sortKeyIndex,
    const std::string& sortKeyType,
    bool sortAscending,
    long rankEnd,
    int uniqueKeyIndex,
    const std::string& uniqueKeyType,
    int aggFuncIndex,
    int filterIndex,
    bool generateUpdateBefore)
    : sortKeyIndex(sortKeyIndex),
      sortAscending(sortAscending),
      rankEnd(rankEnd),
      aggFuncIndex(aggFuncIndex),
      filterIndex(filterIndex),
      hasFilter(filterIndex != -1),
      generateUpdateBefore(generateUpdateBefore)
{
    // v1 scope mirrors the Top2 UDF: BIGINT sort key, DESC (strict '>' keeps the
    // largest values). Anything else throws here and the plan falls back to Java.
    if (sortKeyType != "BIGINT") {
        throw std::runtime_error("RowNumberFunction v1 only supports BIGINT sort key, got: " + sortKeyType);
    }
    if (sortAscending) {
        throw std::runtime_error("RowNumberFunction v1 only supports DESC sort (Top2 keeps the largest values)");
    }
    // Keyed mode (a member identity column) is Rank/Top-N semantics, not a table
    // aggregate: the Top2 whitelist plan is always keyless. Keep the parameter for
    // interface stability, refuse the value.
    if (uniqueKeyIndex != -1) {
        throw std::runtime_error(
            "RowNumberFunction v1 is keyless only (Top2 table aggregate); keyed plans fall back to Java");
    }
    if (rankEnd < 1) {
        throw std::runtime_error("RowNumberFunction rankEnd must be >= 1, got " + std::to_string(rankEnd));
    }

    slots.assign(static_cast<size_t>(rankEnd), EMPTY_SLOT);

    // Emitted aggregate row is [value, rn], both BIGINT in v1.
    aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId("BIGINT"));
    aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId("BIGINT"));
}

void RowNumberFunction::open(StateDataViewStore* store)
{
    // Scheme A: all persistent state lives in the operator's accState row; no data
    // view is created. The store is kept for contract stability / future handlers.
    this->store = store;
}

void RowNumberFunction::accumulate(RowData* input)
{
    if (hasFilter) {
        const bool filterNull = input->isNullAt(filterIndex);
        if (filterNull || !*(input->getBool(filterIndex))) {
            return;
        }
    }
    if (input->isNullAt(sortKeyIndex)) {
        // NULL sort key never ranks (the Top2 UDF's v != null guard).
        return;
    }
    const long long value = static_cast<long long>(*(input->getLong(sortKeyIndex)));

    // Top2 UDF accumulate, generalized to rankEnd slots (strict '>'):
    //   rankEnd=2 reads exactly as  v > first ? (second = first, first = v)
    //                                 : v > second ? (second = v) : /* not in top */
    // An equal value settles one slot lower, so the same value arriving twice
    // ranks twice (plain Flink TableAggregateFunction semantics, no idempotence).
    for (size_t i = 0; i < slots.size(); ++i) {
        if (betterThan(value, slots[i], sortAscending)) {
            for (size_t j = slots.size() - 1; j > i; --j) {
                slots[j] = slots[j - 1];
            }
            slots[i] = value;
            return;
        }
    }
    // Not greater than any occupied/free slot floor: state unchanged.
}

void RowNumberFunction::retract(RowData* /*input*/)
{
    // The Top2 UDF declares no retract; the operator drops retraction messages
    // before they reach the handler. This is defensive only (plain Flink's codegen
    // would fail outright on a retract call to a UDF without retract()).
    LOG("RowNumberFunction: retract is not supported by the Top2 UDF and is dropped by the operator");
}

void RowNumberFunction::createAccumulators(BinaryRowData* accumulators)
{
    // Fresh group: every slot is the numeric-minimum sentinel (not SQL NULL), the
    // same bytes the codegen'd Top2 handler writes for Top2Accum{MIN_VALUE, MIN_VALUE}.
    slots.assign(static_cast<size_t>(rankEnd), EMPTY_SLOT);
    for (int i = 0; i < static_cast<int>(rankEnd); ++i) {
        accumulators->setLong(i, static_cast<long>(EMPTY_SLOT));
    }
}

void RowNumberFunction::setAccumulators(RowData* accumulators)
{
    // Per-key switch (also the checkpoint-recovery path): load this group's
    // [first, second, ...] from the accState row into the working copy.
    for (int i = 0; i < static_cast<int>(rankEnd); ++i) {
        slots[static_cast<size_t>(i)] =
            accumulators->isNullAt(i) ? EMPTY_SLOT : static_cast<long long>(*(accumulators->getLong(i)));
    }
}

void RowNumberFunction::getAccumulators(BinaryRowData* accumulators)
{
    // Persist the working copy back into the accState row (the operator updates
    // the ValueState right after this call, mirroring Flink's getAccumulators flow).
    for (int i = 0; i < static_cast<int>(rankEnd); ++i) {
        accumulators->setLong(i, static_cast<long>(slots[static_cast<size_t>(i)]));
    }
}

void RowNumberFunction::cleanup()
{
    // Scheme A: all state lives in the operator-managed accState; nothing to clean.
}

void RowNumberFunction::close() {}

void RowNumberFunction::emitValue(
    TimestampedCollector* out,
    RowData* currentKey,
    bool isRetract,
    bool firstRow)
{
    RowKind kind;
    if (isRetract) {
        kind = generateUpdateBefore ? RowKind::UPDATE_BEFORE : RowKind::DELETE;
    } else {
        kind = firstRow ? RowKind::INSERT : RowKind::UPDATE_AFTER;
    }

    // Conditional output (Top2 UDF emitValue): only occupied slots are emitted.
    // Slots are ordered, so the first EMPTY_SLOT ends the board.
    long rn = 1;
    for (const long long value : slots) {
        if (value == EMPTY_SLOT) {
            break;
        }
        auto* aggRow = new GenericRowData(aggValueTypeIds);
        aggRow->setField(0, static_cast<long>(value));
        aggRow->setField(1, rn);
        // TODO(perf): reuse a member JoinedRowData/aggRow pair and only refresh fields.
        auto* resultRow = new JoinedRowData(kind, currentKey, aggRow);
        out->collect(resultRow);
        ++rn;
    }
}
