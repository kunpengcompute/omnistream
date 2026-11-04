/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_RETRACTABLETOPNFUNCTION_H
#define OMNISTREAM_RETRACTABLETOPNFUNCTION_H

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "AbstractTopNFunction.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

// Streaming Limit retract path, native port of
// org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction.
//
// Selected by the planner when the Limit input contains updates/deletes
// (needRetraction -> RetractStrategy). Only reachable in the streaming Limit
// shape: empty partition key (ALL_IN_ONE) and empty sort key (SortSpec.ANY).
//
// Registered states mirror Flink's two descriptors (names and backend types):
// 1. data-state: MapState<sortKey RowData, List<input RowData>>
//    (Flink: MapStateDescriptor<RowData, List<RowData>>("data-state",
//     sortKeyType, valueTypeInfo)); backend ids ROW_BK/ROW_LIST_BK.
// 2. sorted-map: ValueState<int64_t> holding the per-group record count
//    (Flink: ValueStateDescriptor<SortedMap<RowData, Long>>("sorted-map",
//     SortedMapTypeInfo(sortKeyType, LONG_TYPE_INFO, comparator)); backend id
//     BIGINT_BK). In the Limit shape the sorted map degenerates to ONE entry
//     {empty sortKey -> count}, so the int64 value is that entry's count; the
//     registered name and the count bookkeeping follow Flink exactly.
//
// Semantics (outputRankNumber is always false for Limit; the rank only feeds
// range filtering; dispatch mirrors Flink's outputRankNumber || hasOffset()):
// - accumulate: treeMap count += 1 first, rank = new count; append the row to
//   the data-state list; INSERT only when rank in [rankStart, rankEnd],
//   otherwise counted silently (rows beyond the window MUST still enter state
//   - they promote later).
// - retract: two-phase target location; the treeMap count is decremented for
//   EVERY retract message (found or not), exactly like Flink's processElement
//   which updates the sorted map after the retract regardless of the search
//   outcome - a not-found retract therefore leaves count and list drifted,
//   which is Flink's documented lenient tolerance for TTL-staled state.
//   When the count is already empty the retract is a sorted-map miss:
//   lenient warn (stateStaledErrorHandle) and no decrement.
// - retract without offset (rankStart == 1): DELETE the removed row; when the
//   window stays full after removal, INSERT the row promoted into the rankEnd
//   slot; intermediate shifts emit nothing (identical row content).
// - retract with offset (rankStart > 1): Flink's with-row-number cascade - for
//   every covered new rank k emit UPDATE_BEFORE of the previous occupant +
//   UPDATE_AFTER of the new occupant (both filtered by isInRankRange; UB also
//   gated by generateUpdateBefore); when the cascade exhausts the rows with k
//   still inside the window, DELETE the last displaced occupant.
// - retract search: the primary scan covers 1-based ranks 1..rankEnd (no
//   offset) / 1..rankEnd+1 (offset, it must see the row promoting into the
//   last window slot); when it misses, the fallback scans the whole list and
//   removes the equal row silently (Flink's !stateRemoved branch).
template <typename K>
class RetractableTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit RetractableTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices)
    {
        // This class serves the streaming Limit retract shape only: empty
        // partition key (ALL_IN_ONE) + empty sort key (SortSpec.ANY).
        if (!this->partitionKeyIndices.empty() || !this->sortKeyIndices.empty()) {
            NOT_IMPL_EXCEPTION;
        }
    }

    ~RetractableTopNFunction() override = default;

    // Variable names mirror Flink's RetractableTopNFunction.open(Configuration
    // parameters): parameters, sortKeyType, valueTypeInfo, mapStateDescriptor,
    // valueStateDescriptor, dataState, treeMap.
    void open(const Configuration& parameters) override
    {
        // Flink: super.open(parameters) compiles the codegen sort-key comparator
        // and the equaliser; the C++ base class declares open() pure (nothing to
        // call), and this port compares rows at runtime in rowsEqual().
        omnistream::RowType sortKeyType(false, std::vector<std::string>{});
        omnistream::RowType inputType(false, *this->inputTypeNames);
        // Flink: ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        // The C++ descriptor takes serializers in place of Flink's type infos.
        auto* valueTypeInfo =
            new BinaryRowDataListSerializer(static_cast<int>(this->inputRowType->size()), new RowDataSerializer(&inputType));
        // Flink: MapStateDescriptor<RowData, List<RowData>> mapStateDescriptor =
        //     new MapStateDescriptor<>("data-state", sortKeyType, valueTypeInfo);
        auto* mapStateDescriptor = new MapStateDescriptor<RowData*, std::vector<RowData*>*>(
            "data-state", new RowDataSerializer(&sortKeyType), valueTypeInfo);
        mapStateDescriptor->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        dataState = runtimeContext->template getMapState<RowData*, std::vector<RowData*>*>(mapStateDescriptor);

        // Flink: ValueStateDescriptor<SortedMap<RowData, Long>> valueStateDescriptor =
        //     new ValueStateDescriptor<>("sorted-map",
        //         new SortedMapTypeInfo<>(sortKeyType, LONG_TYPE_INFO, comparator));
        // Limit single-group degenerate: the sorted map holds one entry
        // {empty sortKey -> count}; the registered name matches Flink and the
        // int64 value is that entry's count.
        auto* valueStateDescriptor = new ValueStateDescriptor<int64_t>("sorted-map", new LongSerializer());
        treeMap = runtimeContext->template getState<int64_t>(valueStateDescriptor);

        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "RetractableTopNFunction");
        limitSortKey = BinaryRowData::createBinaryRowDataWithMem(0);
    }

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        this->initRankEnd(nullptr);
        int rowCnt = inputBatchGuard->GetRowCount();
        if (rowCnt == 0) {
            return;
        }

        // All rows share the (empty) partition key; address the keyed state
        // slot once. The state backend addresses keys by content.
        K currentKey = partitionKeySelector.getKey(inputBatchGuard.get(), 0);
        ctx.setCurrentKey(currentKey);

        // Load the single group list. RocksDB entries() returns owned copies;
        // HashMap entries() returns the live map (mutations are visible).
        std::vector<RowData*>* rows = nullptr;
        RowData* stateKey = limitSortKey;
        emhash7::HashMap<RowData*, std::vector<RowData*>*>* encoded = dataState->entries();
        if (encoded != nullptr && !encoded->empty()) {
            auto it = encoded->begin();
            stateKey = it->first;
            rows = it->second;
        }
        bool freshList = rows == nullptr;
        if (freshList) {
            rows = new std::vector<RowData*>();
        }
        bool listChanged = false;
        // treeMap bookkeeping: count of the single group entry, kept in lockstep
        // with the list (invariant: count == rows->size() while state is
        // consistent; a not-found retract intentionally drifts it, like Flink).
        int64_t count = treeMap->value();
        int64_t originalCount = count;
        // Rows we extracted from the batch and must free once the output has
        // been assembled: retract inputs (never stored) and, on RocksDB, the
        // stored copies after the write-back.
        std::vector<RowData*> scratchRows;
        // Rows removed from a live (HEAP) state list: we became the owner.
        std::vector<RowData*> removedStateRows;

        for (int i = 0; i < rowCnt; ++i) {
            int64_t timestamp = inputBatchGuard->getTimestamp(i);
            if (RowDataUtil::isAccumulateMsg(inputBatchGuard->getRowKind(i))) {
                RowData* row = inputBatchGuard->extractRowData(i);
                rows->push_back(row);
                listChanged = true;
                // Flink updates the sorted map before emitting: rank = new count.
                count += 1;
                int64_t rank = count;
                if (this->isInRankRange(static_cast<int>(rank))) {
                    this->collectInsert(row, static_cast<int>(rank), timestamp);
                }
            } else {
                RowData* row = inputBatchGuard->extractRowData(i);
                scratchRows.push_back(row);
                handleRetract(rows, row, timestamp, removedStateRows, listChanged);
                // Flink decrements the sorted map for every retract message,
                // regardless of the search outcome; a count that is already
                // empty is a sorted-map miss -> lenient state-staled warn.
                if (count > 0) {
                    count -= 1;
                } else {
                    WARN_RELEASE(
                        "RetractableTopNFunction: the state is cleared because of state ttl. "
                        "This will result in incorrect result. You can increase the state ttl to avoid this.");
                }
            }
        }

        bool emit = this->hasOutputRows();
        if (emit) {
            omnistream::VectorBatch* outputBatch = this->createOutputBatch();
            this->collectOutputBatch(out, outputBatch);
        }

        // Write-back. HEAP: a pre-existing list is the live state value, the
        // in-place mutations are already visible; only a fresh (or emptied)
        // list needs put()/remove(). RocksDB: put the (copied) list back and
        // free the copies afterwards - the emission above already read them.
        if (stateType_ == omnistream::StateType::ROCKSDB) {
            if (listChanged && !rows->empty()) {
                dataState->put(stateKey, rows);
            } else if (listChanged && rows->empty() && !freshList) {
                dataState->remove(stateKey);
            }
            for (RowData* row : *rows) {
                scratchRows.push_back(row);
            }
            delete rows;
            dataState->clearEntriesCache();
        } else {
            if (freshList) {
                if (!rows->empty()) {
                    dataState->put(stateKey, rows);
                } else {
                    delete rows;
                }
            } else if (rows->empty()) {
                dataState->remove(stateKey);
                delete rows;
            }
        }

        // sorted-map write-back: update the count, or clear when the single
        // group entry dropped to zero (Flink removes the key from the map).
        if (count != originalCount) {
            if (count > 0) {
                treeMap->update(count);
            } else {
                treeMap->clear();
            }
        }

        for (RowData* row : scratchRows) {
            delete row;
        }
        for (RowData* row : removedStateRows) {
            delete row;
        }
        if constexpr (std::is_same<K, RowData*>::value) {
            delete currentKey;
        }
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    // Flink's GeneratedRecordEqualiser: full-row value equality ignoring
    // RowKind (two nulls compare equal).
    bool rowsEqual(RowData* left, RowData* right) const
    {
        int arity = static_cast<int>(this->inputRowType->size());
        for (int col = 0; col < arity; ++col) {
            bool leftNull = left->isNullAt(col);
            bool rightNull = right->isNullAt(col);
            if (leftNull && rightNull) {
                continue;
            }
            if (leftNull != rightNull) {
                return false;
            }
            switch (this->inputRowType->at(col)) {
                case DataTypeId::OMNI_INT: {
                    if (*left->getInt(col) != *right->getInt(col)) {
                        return false;
                    }
                    break;
                }
                case DataTypeId::OMNI_LONG:
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP: {
                    if (*left->getLong(col) != *right->getLong(col)) {
                        return false;
                    }
                    break;
                }
                case DataTypeId::OMNI_DOUBLE: {
                    if (*reinterpret_cast<const double*>(left->getLong(col)) !=
                        *reinterpret_cast<const double*>(right->getLong(col))) {
                        return false;
                    }
                    break;
                }
                case DataTypeId::OMNI_BOOLEAN: {
                    if (left->getBool(col) != right->getBool(col)) {
                        return false;
                    }
                    break;
                }
                case DataTypeId::OMNI_CHAR:
                case DataTypeId::OMNI_VARCHAR: {
                    if (left->getStringView(col) != right->getStringView(col)) {
                        return false;
                    }
                    break;
                }
                default:
                    THROW_LOGIC_EXCEPTION("RetractableTopNFunction: unsupported equaliser column type");
            }
        }
        return true;
    }

    // Retract target location + emissions, registered on the base-class
    // collect* machinery. Mirrors Flink's two-phase search: the primary scan
    // bound echoes the with/without-row-number scan depths.
    void handleRetract(
        std::vector<RowData*>* rows,
        RowData* inputRow,
        int64_t timestamp,
        std::vector<RowData*>& removedStateRows,
        bool& listChanged)
    {
        // Primary scan bound mirrors Flink: the with-row-number scan (taken
        // when outputRankNumber || hasOffset; Limit only ever sets hasOffset
        // here) visits 1-based ranks 1..rankEnd+1 - it must also see the row
        // promoting into the last window slot - while the without-row-number
        // scan visits 1..rankEnd.
        const int64_t scanBound = this->hasOffset() ? this->rankEnd + 1 : this->rankEnd;
        size_t primaryLimit = static_cast<size_t>(scanBound) < rows->size()
                                  ? static_cast<size_t>(scanBound)
                                  : rows->size();
        int foundIndex = -1;
        for (size_t idx = 0; idx < primaryLimit; idx++) {
            if (rowsEqual((*rows)[idx], inputRow)) {
                foundIndex = static_cast<int>(idx);
                break;
            }
        }
        if (foundIndex < 0) {
            // processElement's !stateRemoved fallback: the row sits beyond the
            // scan bound. Flink re-searches the whole list by equaliser and
            // removes it silently - it was never inside the window so nothing
            // is emitted, but it must still leave state, otherwise a later
            // promotion would resurrect an already retracted row.
            for (size_t idx = primaryLimit; idx < rows->size(); idx++) {
                if (rowsEqual((*rows)[idx], inputRow)) {
                    removedStateRows.push_back((*rows)[idx]);
                    rows->erase(rows->begin() + static_cast<long>(idx));
                    listChanged = true;
                    return;
                }
            }
            // Not found anywhere: state staled (e.g. cleared by TTL) - lenient
            // skip; the treeMap count is still decremented by the caller,
            // exactly like Flink's post-retract sorted map update.
            return;
        }
        const int64_t rank = foundIndex + 1; // 1-based rank of the removed row, rank <= scanBound
        RowData* removed = (*rows)[static_cast<size_t>(foundIndex)];
        rows->erase(rows->begin() + static_cast<long>(foundIndex));
        removedStateRows.push_back(removed);
        listChanged = true;
        const int64_t newSize = static_cast<int64_t>(rows->size());
        if (!this->hasOffset()) {
            // Without-row-number path (rankStart == 1): boundary DELETE +
            // promoted INSERT only; intermediate shifts emit nothing.
            if (this->isInRankRange(static_cast<int>(rank))) {
                this->collectDelete(removed, static_cast<int>(rank), timestamp);
            }
            if (newSize >= this->rankEnd) {
                RowData* promoted = (*rows)[static_cast<size_t>(this->rankEnd - 1)];
                this->collectInsert(promoted, static_cast<int>(this->rankEnd), timestamp);
            }
            return;
        }
        // With-row-number path (offset): Flink's update cascade. After the
        // removal, the row previously at index j+1 sits at index j with new
        // rank j+1; for every covered new rank k, retract the previous occupant
        // (UPDATE_BEFORE) and emit the new occupant (UPDATE_AFTER), both
        // filtered by isInRankRange inside the base collect* helpers. A removal
        // at rank rankEnd+1 (inside the scan bound but outside the window)
        // falls through with no emission.
        RowData* prevRow = removed;
        int64_t k = rank;
        for (int64_t j = foundIndex; j < newSize && k <= this->rankEnd; j++, k++) {
            RowData* current = (*rows)[static_cast<size_t>(j)];
            this->collectUpdateBefore(prevRow, static_cast<int>(k), timestamp);
            this->collectUpdateAfter(current, static_cast<int>(k), timestamp);
            prevRow = current;
        }
        if (this->isInRankRange(static_cast<int>(k))) {
            // The cascade exhausted the remaining rows while the hole is still
            // inside the window: delete the last shifted row's old output (or
            // the removed row itself when nothing shifted).
            this->collectDelete(prevRow, static_cast<int>(k), timestamp);
        }
    }

    MapState<RowData*, std::vector<RowData*>*>* dataState = nullptr;
    ValueState<int64_t>* treeMap = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
    KeySelector<K> partitionKeySelector;
    RowData* limitSortKey = nullptr; // constant arity-0 sort key (the single group)
};

#endif // OMNISTREAM_RETRACTABLETOPNFUNCTION_H
