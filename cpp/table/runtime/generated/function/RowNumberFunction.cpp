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

#include "RowNumberFunction.h"

#include <algorithm>
#include <optional>

#include "core/include/common.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "table/data/GenericRowData.h"
#include "table/data/JoinedRowData.h"

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
      uniqueKeyIndex(uniqueKeyIndex),
      uniqueKeyTypeName(uniqueKeyType),
      aggFuncIndex(aggFuncIndex),
      filterIndex(filterIndex),
      hasFilter(filterIndex != -1),
      generateUpdateBefore(generateUpdateBefore)
{
    // v1 scope: BIGINT sort key only (POC auction bids). Other types make the plan fall back
    // to Java, mirroring the JsonArrayAggFunction retraction fallback precedent.
    if (sortKeyType != "BIGINT") {
        throw std::runtime_error("RowNumberFunction v1 only supports BIGINT sort key, got: " + sortKeyType);
    }
    // v1 scope: numeric unique key only (BIGINT / INT). The ledger is a (BIGINT, BIGINT)
    // MapView (the only serializer-symmetric combination on both backends -- see the class
    // comment), so a STRING unique key cannot be stored exactly and must fall back to Java.
    if (uniqueKeyType != "BIGINT" && uniqueKeyType != "INT") {
        throw std::runtime_error("RowNumberFunction v1 only supports BIGINT/INT unique key, got: " + uniqueKeyType);
    }
    if (rankEnd < 1) {
        throw std::runtime_error("RowNumberFunction rankEnd must be >= 1, got: " + std::to_string(rankEnd));
    }
    sortKeyId = LogicalType::flinkTypeToOmniTypeId(sortKeyType);

    // Emitted aggregate row is [sortKey, rn], both BIGINT in v1.
    aggValueTypeIds.push_back(sortKeyId);
    aggValueTypeIds.push_back(LogicalType::flinkTypeToOmniTypeId("BIGINT"));
}

void RowNumberFunction::open(StateDataViewStore* store)
{
    this->store = store;
    auto* perKeyViewStore = reinterpret_cast<PerKeyStateDataViewStore<RowData*>*>(store);
    // (BIGINT, BIGINT) -> <int64_t, int64_t>: the only MapView combination registered on both
    // backends whose serializer (LongSerializer) round-trips symmetrically. See class comment.
    membersView = reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, long, long>*>(
        perKeyViewStore->getStateMapView<VoidNamespace, long, long>(
            "row_number_" + std::to_string(aggFuncIndex),
            false,
            new LongSerializer(),
            new LongSerializer()));
}

long long RowNumberFunction::extractUniqueKey(RowData* input) const
{
    if (input->isNullAt(uniqueKeyIndex)) {
        throw std::runtime_error("RowNumberFunction: unique key column must not be NULL");
    }
    if (uniqueKeyTypeName == "BIGINT") {
        return static_cast<long long>(*(input->getLong(uniqueKeyIndex)));
    }
    if (uniqueKeyTypeName == "INT") {
        return static_cast<long long>(*(input->getInt(uniqueKeyIndex)));
    }
    throw std::runtime_error("RowNumberFunction: unsupported unique key type: " + uniqueKeyTypeName);
}

void RowNumberFunction::accumulate(RowData* input)
{
    if (hasFilter) {
        const bool filterNull = input->isNullAt(filterIndex);
        if (filterNull || !*(input->getBool(filterIndex))) {
            return;
        }
    }
    const long long uniqueId = extractUniqueKey(input);
    if (input->isNullAt(sortKeyIndex)) {
        // NULL sort key never ranks: skip (Flink ORDER BY NULLS placement is a v2 topic).
        return;
    }
    const long long sortKey = static_cast<long long>(*(input->getLong(sortKeyIndex)));

    // Idempotence: a replayed/duplicated member is dropped instead of double-counted.
    for (const auto& member : sortedMembers) {
        if (member.uniqueId == uniqueId) {
            LOG("RowNumberFunction: duplicate member " << uniqueId << " ignored");
            return;
        }
    }

    // Admission check: only enter if the board is not full or we rank before the worst member.
    const bool boardNotFull = static_cast<long>(sortedMembers.size()) < rankEnd;
    const bool beatsWorst = !sortedMembers.empty()
        && ranksBefore(sortKey, uniqueId, sortedMembers.back().sortKey, sortedMembers.back().uniqueId, sortAscending);
    if (!boardNotFull && !beatsWorst) {
        return; // ranks below the whole Top-N board: no state change, no output change
    }

    // Insert in rank order (index 0 = rank 1), full (sortKey, uniqueId) order.
    const LeaderboardMember member{uniqueId, sortKey};
    size_t pos = 0;
    while (pos < sortedMembers.size()
           && ranksBefore(
               sortedMembers[pos].sortKey, sortedMembers[pos].uniqueId, sortKey, uniqueId, sortAscending)) {
        ++pos;
    }
    sortedMembers.insert(sortedMembers.begin() + static_cast<long>(pos), member);
    membersView->put(std::optional<long>{static_cast<long>(uniqueId)}, static_cast<long>(sortKey));

    // Eviction: the worst member leaves both the in-memory board and the MapView (bounded state).
    if (static_cast<long>(sortedMembers.size()) > rankEnd) {
        const LeaderboardMember evicted = sortedMembers.back();
        membersView->remove(std::optional<long>{static_cast<long>(evicted.uniqueId)});
        sortedMembers.pop_back();
        LOG("RowNumberFunction: member "
            << evicted.uniqueId << " evicted (sortKey=" << evicted.sortKey << ", rankEnd=" << rankEnd << ")");
    }
}

void RowNumberFunction::retract(RowData* input)
{
    if (hasFilter) {
        const bool filterNull = input->isNullAt(filterIndex);
        if (filterNull || !*(input->getBool(filterIndex))) {
            return;
        }
    }
    const long long uniqueId = extractUniqueKey(input);
    for (auto it = sortedMembers.begin(); it != sortedMembers.end(); ++it) {
        if (it->uniqueId == uniqueId) {
            membersView->remove(std::optional<long>{static_cast<long>(uniqueId)});
            sortedMembers.erase(it);
            return;
        }
    }
    // Retracting a member that was already evicted is a no-op (Flink Top-N retract parity).
    LOG("RowNumberFunction: retract of non-member " << uniqueId << " ignored (already evicted)");
}

void RowNumberFunction::createAccumulators(BinaryRowData* accumulators)
{
    // No BinaryRowData accumulator slot (arity 0); state is created lazily in the keyed MapView.
}

void RowNumberFunction::setAccumulators(RowData* /*accumulators*/)
{
    // Per-key switch (also the checkpoint-recovery path): rebuild the in-memory board from the
    // MapView of the current key. v1 rebuilds on every call for simplicity; caching keyed by
    // current group key is a follow-up optimization.
    rebuildFromView();
}

void RowNumberFunction::getAccumulators(BinaryRowData* accumulators)
{
    // No BinaryRowData accumulator slot (arity 0); state is persisted in the MapView on put/remove.
}

void RowNumberFunction::cleanup()
{
    // Group death: drop the whole board of the current key from memory and the MapView.
    sortedMembers.clear();
    if (membersView == nullptr) {
        return;
    }
    // Same cache-invalidation rule as rebuildFromView: the cached entries may be stale
    // after this frame's put()/remove() calls, and cleanup must see the true members.
    membersView->clearEntriesCache();
    auto* entries = membersView->entries();
    if (entries == nullptr) {
        return;
    }
    std::vector<long> keysToRemove;
    keysToRemove.reserve(entries->size());
    for (const auto& entry : *entries) {
        keysToRemove.push_back(entry.first);
    }
    for (const long key : keysToRemove) {
        membersView->remove(std::optional<long>{key});
    }
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

    long rn = 1;
    for (const auto& member : sortedMembers) {
        auto* aggRow = new GenericRowData(aggValueTypeIds);
        aggRow->setField(0, static_cast<long>(member.sortKey));
        aggRow->setField(1, rn);
        // TODO(perf): reuse a member JoinedRowData/aggRow pair and only refresh fields.
        auto* resultRow = new JoinedRowData(kind, currentKey, aggRow);
        out->collect(resultRow);
        ++rn;
    }
}

void RowNumberFunction::rebuildFromView()
{
    sortedMembers.clear();
    if (membersView == nullptr) {
        return;
    }
    // RocksDB: entries() serves a per-key cache that put()/remove() do not invalidate,
    // so clear it before rebuilding (AppendOnlyTopNFunction does the same on rocksBackend;
    // the heap backend's clearEntriesCache is a no-op).
    membersView->clearEntriesCache();
    auto* entries = membersView->entries();
    if (entries == nullptr) {
        return;
    }
    for (const auto& entry : *entries) {
        sortedMembers.push_back(LeaderboardMember{
            static_cast<long long>(entry.first), static_cast<long long>(entry.second)});
    }
    // Full (sortKey, uniqueId) order; MapView iteration order is unspecified and must not leak
    // into the board (deterministic output on both backends and across restarts).
    std::sort(
        sortedMembers.begin(),
        sortedMembers.end(),
        [this](const LeaderboardMember& a, const LeaderboardMember& b) {
            return ranksBefore(a.sortKey, a.uniqueId, b.sortKey, b.uniqueId, sortAscending);
        });
}
