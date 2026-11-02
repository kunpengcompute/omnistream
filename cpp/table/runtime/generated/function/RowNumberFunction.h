/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Description: Native handler for the ROW_NUMBER table aggregate (Top-N leaderboard).
 *
 * Accumulator model = Scheme B (StateDataView / MapView), aligned with JsonArrayAggFunction:
 *   - keyed MapView<VoidNamespace, long, long> : uniqueId -> sortKey.
 *     The type pair is dictated by the state-backend dispatch tables
 *     (HeapKeyedStateBackend / RocksdbKeyedStateBackend createOrUpdateInternalState):
 *     a MapState's <UK, UV> is chosen from a whitelist of (BackendDataType keyId, valueId)
 *     combinations, and (BIGINT_BK, BIGINT_BK) -> <int64_t, int64_t> is the combination that
 *     is registered on BOTH backends and whose serializer (LongSerializer) round-trips
 *     symmetrically. String-valued MapViews ((BIGINT, VARCHAR) -> <int64_t, std::string>,
 *     the JSON_ARRAYAGG combination) only work on the heap backend: RocksDB really calls
 *     StringSerializer, whose native payload type is std::u32string, so a std::string value
 *     is type-punned to garbage there.
 *   - NO BinaryRowData accumulator slot is used (accumulatorSlots()==0), so the operator's
 *     accStartingIndex bookkeeping stays aligned with the Flink RAW-accType plan.
 *
 * In-memory working copy: sortedMembers holds the current group's leaderboard in rank order
 * (index 0 = rank 1). It is rebuilt from the MapView in setAccumulators (per-key switch, also
 * the checkpoint-recovery path) and kept in sync by accumulate/retract.
 *
 * Semantics (v1 scope, documented deviations fall back to Java):
 *   - sort key: BIGINT only (POC auction bids); other types throw at construction.
 *   - unique key: BIGINT / INT column (numeric), required for retract matching and
 *     idempotence; STRING and other types throw at construction and the plan falls back to
 *     Java (a string unique key cannot be stored in the (BIGINT, BIGINT) MapView without a
 *     lossy hash, which we refuse: idempotence must be exact).
 *   - Top-N bounds: rankStart fixed at 1; rankEnd = N. Members beyond rankEnd are evicted from
 *     both the in-memory board and the MapView (bounded state, Flink Top-N parity).
 *   - rank order is the lexicographic (sortKey, uniqueId) order: equal sort keys are broken
 *     deterministically by ascending uniqueId, identical on both backends and across restarts
 *     (MapView iteration order itself is unspecified, so the board must never depend on it).
 *   - accumulate: filtered, idempotent by uniqueId; inserts in rank order, evicts the worst
 *     member when the board overflows.
 *   - retract: removes the member with the same uniqueId; retracting an already-evicted
 *     member is a no-op (Flink Top-N retract parity).
 *   - emitValue: emits the whole board as 0..N rows of [currentKey, sortKey, rn].
 *     RowKind policy (mirrors Flink GroupTableAggFunction):
 *       isRetract=true  -> generateUpdateBefore ? UPDATE_BEFORE : DELETE
 *       isRetract=false -> firstRow ? INSERT : UPDATE_AFTER
 */

#ifndef FLINK_TNEL_ROWNUMBERFUNCTION_H
#define FLINK_TNEL_ROWNUMBERFUNCTION_H

#include <string>
#include <vector>
#include "../TableAggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
#include "../runtime/state/VoidNamespace.h"
#include "table/types/logical/LogicalType.h"

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

    /** Obtain the keyed MapView from the operator-provided store (JsonArrayAggFunction pattern). */
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
     * Emit the whole leaderboard as 0..N rows of [currentKey, sortKey, rn].
     * RowKind policy is documented in the class comment.
     */
    void emitValue(TimestampedCollector* out, RowData* currentKey, bool isRetract, bool firstRow) override;

    // State lives entirely in the keyed MapView; no BinaryRowData accumulator slot is used.
    int accumulatorSlots() const override
    {
        return 0;
    }

    /** 分组存活判断：内存榜为空 = 分组消亡（Top-N 语义：榜上无人 = 组死）。 */
    bool isGroupEmpty() const override
    {
        return sortedMembers.empty();
    }

private:
    /** One leaderboard member, identified by its unique key (numeric in v1). */
    struct LeaderboardMember {
        long long uniqueId = 0;
        long long sortKey = 0; // BIGINT sort key (v1 scope)
    };

    /** Strict ordering: does candidate `a` rank strictly before `b`? */
    static bool betterThan(long long a, long long b, bool ascending)
    {
        return ascending ? a < b : a > b;
    }

    /**
     * Full strict rank order: (sortKey, uniqueId) lexicographic. Equal sort keys are broken
     * by ascending uniqueId so the board never depends on MapView iteration order.
     */
    static bool ranksBefore(long long sortA, long long idA, long long sortB, long long idB, bool ascending)
    {
        if (sortA != sortB) {
            return betterThan(sortA, sortB, ascending);
        }
        return idA < idB;
    }

    /** Extract the unique-key column value as a number (BIGINT / INT in v1). */
    long long extractUniqueKey(RowData* input) const;

    /** Rebuild sortedMembers from the MapView of the current key. */
    void rebuildFromView();

    // ---- plan configuration ----
    int sortKeyIndex;
    int sortKeyId = 0; // omni DataTypeId of the sort column
    bool sortAscending;
    long rankEnd; // Top-N capacity N (rankStart fixed at 1 in v1)
    int uniqueKeyIndex;
    std::string uniqueKeyTypeName;
    int aggFuncIndex;
    int filterIndex;
    bool hasFilter;
    bool generateUpdateBefore;

    // ---- persistent layer (Scheme B) ----
    StateDataViewStore* store = nullptr;
    KeyedStateMapViewWithKeysNotNull<VoidNamespace, long, long>* membersView = nullptr;

    // ---- in-memory working copy: rank-ordered board of the current group ----
    std::vector<LeaderboardMember> sortedMembers;

    /** typeIds of the emitted aggregate row [sortKey, rn] (both BIGINT in v1). */
    std::vector<int> aggValueTypeIds;
};

#endif // FLINK_TNEL_ROWNUMBERFUNCTION_H
