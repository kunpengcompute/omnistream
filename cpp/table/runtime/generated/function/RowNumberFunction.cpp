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
#include "core/typeutils/StringSerializer.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "table/data/GenericRowData.h"
#include "table/data/JoinedRowData.h"

namespace {
constexpr char kFieldSep = '\x1f'; // unit separator between uniqueId and sortKey
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
    membersView = reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, long, std::string>*>(
        perKeyViewStore->getStateMapView<VoidNamespace, long, std::string>(
            "row_number_" + std::to_string(aggFuncIndex),
            false,
            new LongSerializer(),
            new StringSerializer()));
}

std::string RowNumberFunction::serializeMember(const LeaderboardMember& member)
{
    // Length-prefixed uniqueId so any separator characters inside the id stay safe.
    std::string text = std::to_string(member.uniqueId.size());
    text += ':';
    text += member.uniqueId;
    text += kFieldSep;
    text += std::to_string(member.sortKey);
    return text;
}

RowNumberFunction::LeaderboardMember RowNumberFunction::deserializeMember(long seqNo, const std::string& text)
{
    const size_t colon = text.find(':');
    if (colon == std::string::npos) {
        throw std::runtime_error("RowNumberFunction: corrupted member state (no length prefix)");
    }
    const size_t idLen = static_cast<size_t>(std::stoul(text.substr(0, colon)));
    const size_t idStart = colon + 1;
    LeaderboardMember member;
    member.seqNo = seqNo;
    member.uniqueId = text.substr(idStart, idLen);
    const size_t sep = text.find(kFieldSep, idStart + idLen);
    if (sep == std::string::npos) {
        throw std::runtime_error("RowNumberFunction: corrupted member state (no sortKey)");
    }
    member.sortKey = std::stoll(text.substr(sep + 1));
    return member;
}

std::string RowNumberFunction::extractUniqueId(RowData* input) const
{
    if (input->isNullAt(uniqueKeyIndex)) {
        throw std::runtime_error("RowNumberFunction: unique key column must not be NULL");
    }
    if (uniqueKeyTypeName == "STRING") {
        return std::string(input->getStringView(uniqueKeyIndex));
    }
    if (uniqueKeyTypeName == "BIGINT") {
        return std::to_string(*(input->getLong(uniqueKeyIndex)));
    }
    if (uniqueKeyTypeName == "INT") {
        return std::to_string(*(input->getInt(uniqueKeyIndex)));
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
    const std::string uniqueId = extractUniqueId(input);
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

    // Admission check: only enter if the board is not full or the worst member ranks after us.
    const bool boardNotFull = static_cast<long>(sortedMembers.size()) < rankEnd;
    const bool beatsWorst =
        !sortedMembers.empty() && betterThan(sortKey, sortedMembers.back().sortKey, sortAscending);
    if (!boardNotFull && !beatsWorst) {
        return; // ranks below the whole Top-N board: no state change, no output change
    }

    // Insert in rank order (index 0 = rank 1).
    LeaderboardMember member;
    member.seqNo = nextSeqNo;
    member.sortKey = sortKey;
    member.uniqueId = uniqueId;
    size_t pos = 0;
    while (pos < sortedMembers.size() && betterThan(sortedMembers[pos].sortKey, sortKey, sortAscending)) {
        ++pos;
    }
    sortedMembers.insert(sortedMembers.begin() + static_cast<long>(pos), member);
    membersView->put(std::optional<long>{nextSeqNo}, serializeMember(member));
    ++nextSeqNo;

    // Eviction: the worst member leaves both the in-memory board and the MapView (bounded state).
    if (static_cast<long>(sortedMembers.size()) > rankEnd) {
        const LeaderboardMember evicted = sortedMembers.back();
        membersView->remove(std::optional<long>{evicted.seqNo});
        sortedMembers.pop_back();
        LOG("RowNumberFunction: member "
            << evicted.uniqueId << " evicted (sortKey=" << evicted.sortKey
            << ", rankEnd=" << rankEnd << ")");
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
    const std::string uniqueId = extractUniqueId(input);
    for (auto it = sortedMembers.begin(); it != sortedMembers.end(); ++it) {
        if (it->uniqueId == uniqueId) {
            membersView->remove(std::optional<long>{it->seqNo});
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
    nextSeqNo = 0;
    if (membersView == nullptr) {
        return;
    }
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
    nextSeqNo = 0;
    if (membersView == nullptr) {
        return;
    }
    auto* entries = membersView->entries();
    if (entries == nullptr) {
        return;
    }
    for (const auto& entry : *entries) {
        sortedMembers.push_back(deserializeMember(entry.first, entry.second));
        nextSeqNo = std::max(nextSeqNo, entry.first + 1);
    }
    // stable_sort keeps insertion (seqNo) order among equal sort keys.
    std::stable_sort(
        sortedMembers.begin(),
        sortedMembers.end(),
        [this](const LeaderboardMember& a, const LeaderboardMember& b) {
            return betterThan(a.sortKey, b.sortKey, sortAscending);
        });
}
