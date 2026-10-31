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
 *   - keyed MapView<VoidNamespace, long, std::string> : insertion seqNo -> serialized member
 *     ("uniqueId \x1f sortKey"); double backend (heap / RocksDB) and checkpoint recovery are
 *     inherited from the keyed state itself.
 *   - NO BinaryRowData accumulator slot is used (accumulatorSlots()==0), so the operator's
 *     accStartingIndex bookkeeping stays aligned with the Flink RAW-accType plan.
 *
 * In-memory working copy: sortedMembers holds the current group's leaderboard in rank order
 * (index 0 = rank 1). It is rebuilt from the MapView in setAccumulators (per-key switch, also
 * the checkpoint-recovery path) and kept in sync by accumulate/retract.
 *
 * Semantics (v1 scope, documented deviations fall back to Java):
 *   - sort key: BIGINT only (POC auction bids); other types throw at construction.
 *   - unique key: STRING / BIGINT / INT column, required for retract matching and idempotence.
 *   - Top-N bounds: rankStart fixed at 1; rankEnd = N. Members beyond rankEnd are evicted from
 *     both the in-memory board and the MapView (bounded state, Flink Top-N parity).
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
    /** One leaderboard member. seqNo is the MapView key (insertion order). */
    struct LeaderboardMember {
        long seqNo = 0;
        long long sortKey = 0; // BIGINT sort key (v1 scope)
        std::string uniqueId;
    };

    /** Strict ordering: does candidate `a` rank strictly before `b`? */
    static bool betterThan(long long a, long long b, bool ascending)
    {
        return ascending ? a < b : a > b;
    }

    static std::string serializeMember(const LeaderboardMember& member);
    static LeaderboardMember deserializeMember(long seqNo, const std::string& text);

    /** Extract the unique-key column value as text (STRING / BIGINT / INT in v1). */
    std::string extractUniqueId(RowData* input) const;

    /** Rebuild sortedMembers + nextSeqNo from the MapView of the current key. */
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
    KeyedStateMapViewWithKeysNotNull<VoidNamespace, long, std::string>* membersView = nullptr;

    // ---- in-memory working copy: rank-ordered board of the current group ----
    std::vector<LeaderboardMember> sortedMembers;
    long nextSeqNo = 0;

    /** typeIds of the emitted aggregate row [sortKey, rn] (both BIGINT in v1). */
    std::vector<int> aggValueTypeIds;
};

#endif // FLINK_TNEL_ROWNUMBERFUNCTION_H
