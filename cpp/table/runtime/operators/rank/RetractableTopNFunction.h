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

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "AbstractTopNFunction.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/include/common.h"
#include "core/include/outputbuffer.h"
#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/SortedMapRowDataLongSerializer.h"
#include "table/typeutils/SortedMapValue.h"
#include "table/typeutils/RowDataSerializer.h"

// General port of org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction.
// Selected by the planner when a Rank uses the RetractStrategy (input may carry
// INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE). Supports partition keys, sort keys,
// outputRankNumber, offsets and generateUpdateBefore. The empty partition/sort key
// shape (Streaming Limit) degenerates naturally to the previous single-group path.
//
// Registered states mirror Flink's two descriptors exactly (names and logical types):
// 1. data-state: MapState<sortKey RowData, List<input RowData>>
//    (Flink: MapStateDescriptor<RowData, List<RowData>>("data-state", sortKeyType,
//     ListTypeInfo(inputRowType))); backend ids ROW_BK / ROW_LIST_BK.
// 2. sorted-map: ValueState<SortedMap<RowData, Long>> ("sorted-map", SortedMapTypeInfo(
//    sortKeyType, LONG_TYPE_INFO, comparator)). The value is persisted with a serializer
//    whose wire format is byte-identical to Flink's SortedMapSerializer, so native
//    checkpoints stay Flink-compatible. The in-memory representation is a SortedMapValue
//    (std::map<sortKey, count>) loaded via treeMap->value() on first touch and written
//    back via treeMap->update() at batch end, exactly like Flink's processElement.
//
// Semantics mirror Flink's four algorithms:
// - accumulate: bump the sorted-map count for the row's sort key, emit via the
//   with/without-row-number path (selected by outputRankNumber || hasOffset()), then
//   append the row to the data-state list for that sort key.
// - retract: locate/remove the equal row from the data-state list via the equaliser
//   (rowsEqual ignores RowKind), emit UPDATE_BEFORE/UPDATE_AFTER/DELETE, then decrement
//   the sorted-map count for the row's sort key (remove the entry when it drops to 0).
//   A not-found retract leaves the count/list drifted (Flink's documented lenient
//   tolerance for TTL-staled state) and logs a warning.
//
// Output rows never alias state rows: emitRecordsWithoutRowNumber copies toDelete
// (Flink's inputRowSer.copy(toDelete)) and the copies are freed at batch end, so the
// batch-end conversion order is not a safety invariant.
template <typename K>
class RetractableTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit RetractableTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
          sortKeyCmp_(std::vector<omniruntime::type::DataTypeId>(this->sortKeyTypeIds.begin(),
                                                                  this->sortKeyTypeIds.end()),
                      this->sortOrder,
                      this->sortNullsIsLast)
    {
    }

    ~RetractableTopNFunction() override = default;

    void open(const Configuration& parameters) override
    {
        // sortKeyType: RowType over the sort key columns (arity-0 for the Limit shape).
        std::vector<std::string> sortTypeNames;
        for (int col : this->sortKeyIndices) {
            sortTypeNames.push_back((*this->inputTypeNames)[col]);
        }
        omnistream::RowType sortKeyType(false, sortTypeNames);
        omnistream::RowType inputType(false, *this->inputTypeNames);

        // Flink: ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        //         MapStateDescriptor<RowData, List<RowData>>("data-state", sortKeyType, valueTypeInfo);
        auto* valueTypeInfo = new BinaryRowDataListSerializer(
            static_cast<int>(this->inputRowType->size()), new RowDataSerializer(&inputType));
        auto* mapStateDescriptor = new MapStateDescriptor<RowData*, std::vector<RowData*>*>(
            "data-state", new RowDataSerializer(&sortKeyType), valueTypeInfo);
        mapStateDescriptor->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        dataState = runtimeContext->template getMapState<RowData*, std::vector<RowData*>*>(mapStateDescriptor);

        // Flink: ValueStateDescriptor<SortedMap<RowData, Long>>("sorted-map",
        //         SortedMapTypeInfo<>(sortKeyType, LONG_TYPE_INFO, comparator)).
        // The native serializer emits Flink's SortedMapSerializer bytes.
        auto* sortedMapSer = new SortedMapRowDataLongSerializer(static_cast<int>(this->sortKeyIndices.size()),
                                                                std::vector<omniruntime::type::DataTypeId>(
                                                                    this->sortKeyTypeIds.begin(),
                                                                    this->sortKeyTypeIds.end()),
                                                                this->sortOrder,
                                                                this->sortNullsIsLast);
        auto* valueStateDescriptor = new ValueStateDescriptor<SortedMapValue*>("sorted-map", sortedMapSer);
        treeMap = runtimeContext->template getState<SortedMapValue*>(valueStateDescriptor);

        // Flink: AbstractTopNFunction.open() registers ValueStateDescriptor<Long>
        // ("rankEnd", Types.LONG), but only when the rank range is variable.
        if (!this->isConstantRankEnd) {
            this->rankEndState.open(runtimeContext);

            // Fail-fast parity with Flink's AbstractTopNFunction.open(): there the
            // rankEndFetcher is chosen from the rank-end column's type root and an
            // unsupported type throws immediately. Mirror that here so an illegal
            // variable rank-end column fails at open() (before any record) instead of
            // lazily inside fetchRankEnd() on the first record (which lets the job
            // reach RUNNING and only crash once data arrives). Flink also accepts
            // SMALLINT; the native RowData has no getShort accessor, so the native
            // operator intentionally rejects it (see fetchRankEnd).
            const size_t rankEndIdx = static_cast<size_t>(this->rankEndIndex);
            const omniruntime::type::DataTypeId rankEndType =
                this->inputRowType->at(rankEndIdx);
            if (rankEndType != omniruntime::type::DataTypeId::OMNI_LONG &&
                rankEndType != omniruntime::type::DataTypeId::OMNI_INT) {
                throw std::runtime_error(
                    "variable rank index column must be long or int type, while input type is " +
                    std::to_string(static_cast<int>(rankEndType)));
            }
        }

        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "RetractableTopNFunction");
    }

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        const int rowCnt = inputBatchGuard->GetRowCount();
        if (rowCnt == 0) {
            return;
        }

        // Per-partition working sets, keyed by the serialized partition key bytes. Rows are
        // processed strictly in arrival order so the emitted changelog matches Flink's
        // per-record order across partitions.
        std::unordered_map<std::string, PartitionWork> partitionWork;
        partitionWork.reserve(static_cast<size_t>(rowCnt));

        for (int i = 0; i < rowCnt; ++i) {
            K rawKey = partitionKeySelector.getKey(inputBatchGuard.get(), i);
            const std::string keyBytes = partitionKeyString(rawKey);
            PartitionWork* work = nullptr;
            auto it = partitionWork.find(keyBytes);
            if (it == partitionWork.end()) {
                auto& w = partitionWork.emplace(keyBytes, PartitionWork()).first->second;
                w.key = rawKey;
                work = &w;
            } else {
                work = &it->second;
                if constexpr (std::is_same_v<K, RowData*>) {
                    delete rawKey;
                }
            }
            if (!work->open) {
                loadPartition(*work, ctx);
                work->open = true;
            }
            processRow(inputBatchGuard.get(), i, *work, ctx);
        }

        if (this->hasOutputRows()) {
            omnistream::VectorBatch* outputBatch = this->createOutputBatch();
            this->collectOutputBatch(out, outputBatch);
        }

        for (auto& entry : partitionWork) {
            flushPartition(entry.second, ctx);
        }
        for (auto& entry : partitionWork) {
            PartitionWork& w = entry.second;
            for (RowData* row : w.removedStateRows) {
                delete row;
            }
            for (RowData* row : w.retractRows) {
                delete row;
            }
            for (RowData* row : w.emittedCopies) {
                delete row;
            }
            if constexpr (std::is_same_v<K, RowData*>) {
                delete w.key;
            }
        }
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    // Per-partition working set for one batch. The sorted map is loaded from the
    // "sorted-map" ValueState on first touch; the data-state lists are loaded from
    // the "data-state" MapState entries (live map on HEAP, owned copies on RocksDB).
    struct PartitionWork {
        bool open = false;
        K key{};                                                     // retained partition key
        SortedMapValue* sortedMap = nullptr;                         // count map (sorted-map state)
        bool sortedMapCreated = false;                               // created this batch (HEAP must update)
        // View of the "data-state" entries. HEAP: a re-resolved handle to the live map,
        // refreshed on every access - a retract that empties a partition's last list makes
        // HeapMapState::remove() delete the whole live map, so caching the pointer across
        // operations would dangle (use-after-free). ROCKSDB: normally a borrowed handle into
        // RocksdbMapStateTable::entriesCache (reused as-is; released by clearEntriesCache),
        // or - when the partition had no cached entries - a map the operator creates itself
        // (tracked by listsOwned) and must delete after flush.
        emhash7::HashMap<RowData*, std::vector<RowData*>*>* lists = nullptr; // data-state entries
        bool listsOwned = false;                                     // lists was created by this operator (ROCKSDB)
        std::vector<RowData*> removedStateRows;                      // rows erased from lists (free at batch end)
        std::vector<RowData*> retractRows;                           // retract input rows (free at batch end)
        std::vector<RowData*> emittedCopies;                         // defensive copies of emitted state rows (free at batch end)
    };

    // ===== equaliser =====
    // Full-row value equality ignoring RowKind (two nulls compare equal).
    // Mirrors Flink's GeneratedRecordEqualiser.
    bool rowsEqual(RowData* left, RowData* right) const
    {
        const int arity = static_cast<int>(this->inputRowType->size());
        for (int col = 0; col < arity; ++col) {
            const bool leftNull = left->isNullAt(col);
            const bool rightNull = right->isNullAt(col);
            if (leftNull && rightNull) {
                continue;
            }
            if (leftNull != rightNull) {
                return false;
            }
            switch (this->inputRowType->at(col)) {
                case omniruntime::type::DataTypeId::OMNI_INT:
                case omniruntime::type::DataTypeId::OMNI_DATE32: {
                    if (*left->getInt(col) != *right->getInt(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP: {
                    if (*left->getLong(col) != *right->getLong(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                    const omniruntime::type::Decimal128* lv = left->getDecimal128(col);
                    const omniruntime::type::Decimal128* rv = right->getDecimal128(col);
                    const bool isEqual = *lv == *rv;
                    delete lv;
                    delete rv;
                    if (!isEqual) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_DOUBLE: {
                    if (*left->getDouble(col) != *right->getDouble(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN: {
                    if (*left->getBool(col) != *right->getBool(col)) {
                        return false;
                    }
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR: {
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

    // ===== sort key helpers =====
    bool sortKeyEquals(RowData* left, RowData* right) const
    {
        return !sortKeyCmp_(left, right) && !sortKeyCmp_(right, left);
    }

    RowData* projectSortKey(RowData* row) const
    {
        const int arity = static_cast<int>(this->sortKeyIndices.size());
        auto* key = BinaryRowData::createBinaryRowDataWithMem(arity);
        for (size_t i = 0; i < this->sortKeyIndices.size(); ++i) {
            const int col = this->sortKeyIndices[i];
            if (row->isNullAt(col)) {
                key->setNullAt(static_cast<int>(i));
                continue;
            }
            switch (this->sortKeyTypeIds[i]) {
                case omniruntime::type::DataTypeId::OMNI_INT:
                case omniruntime::type::DataTypeId::OMNI_DATE32:
                    key->setInt(static_cast<int>(i), *row->getInt(col));
                    break;
                case omniruntime::type::DataTypeId::OMNI_LONG:
                case omniruntime::type::DataTypeId::OMNI_DECIMAL64:
                case omniruntime::type::DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                    key->setLong(static_cast<int>(i), *row->getLong(col));
                    break;
                case omniruntime::type::DataTypeId::OMNI_DOUBLE:
                    key->setDouble(static_cast<int>(i), *row->getDouble(col));
                    break;
                case omniruntime::type::DataTypeId::OMNI_DECIMAL128: {
                    const omniruntime::type::Decimal128* value = row->getDecimal128(col);
                    key->setDecimal128(static_cast<int>(i), value->LowBits(), value->HighBits());
                    delete value;
                    break;
                }
                case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                    key->setBool(static_cast<int>(i), *row->getBool(col));
                    break;
                case omniruntime::type::DataTypeId::OMNI_CHAR:
                case omniruntime::type::DataTypeId::OMNI_VARCHAR:
                    key->setStringView(static_cast<int>(i), row->getStringView(col));
                    break;
                default:
                    THROW_LOGIC_EXCEPTION("RetractableTopNFunction: unsupported sort key column type");
            }
        }
        return key;
    }

    template <typename K2 = K>
    std::string partitionKeyString(const K2& key) const
    {
        if constexpr (std::is_same_v<K2, RowData*>) {
            if (key == nullptr) {
                return {};
            }
            DataOutputSerializer output;
            OutputBufferStatus bufferStatus;
            output.setBackendBuffer(&bufferStatus);
            BinaryRowDataSerializer serializer(static_cast<int>(this->partitionKeyTypeIds.size()));
            serializer.serialize(key, output);
            return std::string(
                reinterpret_cast<const char*>(output.getData()), static_cast<size_t>(output.getPosition()));
        } else if constexpr (std::is_integral_v<K2>) {
            return std::to_string(key);
        } else {
            THROW_LOGIC_EXCEPTION("RetractableTopNFunction: unsupported partition key type");
        }
    }

    // ===== state loading / write-back =====
    void loadPartition(PartitionWork& work, typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx)
    {
        ctx.setCurrentKey(work.key);
        work.sortedMap = treeMap->value();
        if (work.sortedMap == nullptr) {
            // Flink: if (sortedMap == null) sortedMap = new TreeMap<>(sortKeyComparator);
            work.sortedMap = new SortedMapValue(sortKeyCmp_);
            work.sortedMapCreated = true;
        }
        work.lists = dataState->entries();
    }

    void flushPartition(PartitionWork& work, typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx)
    {
        ctx.setCurrentKey(work.key);

        // sorted-map write-back. Flink: treeMap.update(sortedMap) at the end of every
        // processElement. The map is always written back (not only when created).
        // HEAP: the loaded map is the state table's live object, put back as-is; the
        // created map is handed over (the state owns it from now on). ROCKSDB: the
        // operator owns the deserialized/created copy and frees it after the update.
        if (work.sortedMap != nullptr) {
            treeMap->update(work.sortedMap);
            if (stateType_ == omnistream::StateType::ROCKSDB) {
                delete work.sortedMap;
            }
            work.sortedMap = nullptr;
        }

        // data-state write-back.
        if (stateType_ == omnistream::StateType::ROCKSDB) {
            if (work.lists != nullptr) {
                for (auto& entry : *work.lists) {
                    if (entry.second == nullptr || entry.second->empty()) {
                        dataState->remove(entry.first);
                    } else {
                        dataState->put(entry.first, entry.second);
                    }
                }
                // Values (rows + list) are operator-owned and freed here. Keys are
                // owned by the containing emhash7 map and freed exactly once by its
                // destructor (emhash7.hpp:~HashMap), so they must NOT be freed here:
                //   - borrowed entriesCache map: released by clearEntriesCache() below;
                //   - operator-created map (listsOwned): released by delete below.
                // Freeing keys manually here (or deleting the borrowed cache map) is
                // the flushPartition double-free that crashes at emhash7.hpp:759.
                for (auto& entry : *work.lists) {
                    if (entry.second != nullptr) {
                        for (RowData* row : *entry.second) {
                            delete row;
                        }
                        delete entry.second;
                    }
                }
                if (work.listsOwned) {
                    delete work.lists;
                }
                work.lists = nullptr;
                work.listsOwned = false;
            }
            dataState->clearEntriesCache();
        }
    }

    // ===== data-state list access =====
    // Content-based lookup: the sorted-map keys and the data-state keys are independent
    // objects, so match by sort-key content.
    //
    // HEAP: re-resolve the live map on every access. A retract that empties a partition's
    // last data-state list makes HeapMapState::remove() delete the whole live map, so the
    // pointer cached by loadPartition() would dangle (use-after-free). entries() re-reads
    // the current live map (nullptr once deleted) and every put()/remove() refreshes the
    // state table first, so a re-resolved view always matches the state.
    // ROCKSDB: work.lists is the stable batch-owned copy (never freed mid-batch); it is
    // reused as-is to keep entries the operator added to its own batch copy visible.
    std::vector<RowData*>* findListBySortKey(PartitionWork& work, RowData* sortKey)
    {
        if (stateType_ == omnistream::StateType::HEAP) {
            work.lists = dataState->entries();
        }
        if (work.lists == nullptr) {
            return nullptr;
        }
        for (auto& entry : *work.lists) {
            if (sortKeyEquals(entry.first, sortKey)) {
                return entry.second;
            }
        }
        return nullptr;
    }

    void appendToDataState(PartitionWork& work, RowData* sortKey, RowData* row)
    {
        std::vector<RowData*>* list = findListBySortKey(work, sortKey);
        if (list == nullptr) {
            RowData* dataStateKey = sortKey->copy();
            list = new std::vector<RowData*>();
            if (stateType_ == omnistream::StateType::ROCKSDB) {
                if (work.lists == nullptr) {
                    work.lists = new emhash7::HashMap<RowData*, std::vector<RowData*>*>(1);
                    work.listsOwned = true;
                }
                work.lists->emplace(dataStateKey, list);
            } else {
                // HEAP: the live map owns the key and list; refresh our view of it.
                dataState->put(dataStateKey, list);
                work.lists = dataState->entries();
            }
        }
        list->push_back(row);
    }

    // Called when a data-state list becomes empty: Flink removes the sort key from the
    // data-state MapState. On RocksDB the (empty) entry is kept in the batch copies and
    // removed by flushPartition; on HEAP the live entry is removed here.
    void removeListEntry(PartitionWork& work, RowData* sortKey)
    {
        if (work.lists == nullptr) {
            return;
        }
        for (auto it = work.lists->begin(); it != work.lists->end(); ++it) {
            if (sortKeyEquals(it->first, sortKey)) {
                RowData* key = it->first;
                std::vector<RowData*>* list = it->second;
                if (stateType_ == omnistream::StateType::ROCKSDB) {
                    return;
                }
                dataState->remove(key);
                // HEAP: remove() frees the whole live map once it becomes empty; re-resolve
                // the view so later lookups (findListBySortKey) never touch freed memory.
                work.lists = dataState->entries();
                delete list;
                delete key;
                return;
            }
        }
    }

    // ===== Flink's emitRecordsWithRowNumber =====
    void emitRecordsWithRowNumber(PartitionWork& work, RowData* sortKey, RowData* inputRow, int64_t timestamp)
    {
        auto& sm = work.sortedMap->map();
        int64_t currentRank = 0;
        RowData* currentRow = nullptr;
        bool findsSortKey = false;
        auto it = sm.begin();
        while (it != sm.end() && this->isInRankEnd(currentRank)) {
            RowData* key = it->first;
            if (!findsSortKey && sortKeyEquals(key, sortKey)) {
                currentRank += it->second;
                currentRow = inputRow;
                findsSortKey = true;
                ++it;
            } else if (findsSortKey) {
                std::vector<RowData*>* inputs = findListBySortKey(work, key);
                if (inputs == nullptr) {
                    processStateStaled(sm, it);
                } else {
                    int i = 0;
                    while (i < static_cast<int>(inputs->size()) &&
                           this->isInRankEnd(currentRank)) {
                        RowData* prevRow = (*inputs)[i];
                        this->collectUpdateBefore(prevRow, currentRank, timestamp);
                        this->collectUpdateAfter(currentRow, currentRank, timestamp);
                        currentRow = prevRow;
                        currentRank += 1;
                        ++i;
                    }
                    ++it;
                }
            } else {
                currentRank += it->second;
                ++it;
            }
        }
        if (this->isInRankEnd(currentRank)) {
            this->collectInsert(currentRow, currentRank, timestamp);
        }
    }

    // ===== Flink's emitRecordsWithoutRowNumber =====
    void emitRecordsWithoutRowNumber(PartitionWork& work, RowData* sortKey, RowData* inputRow, int64_t timestamp)
    {
        auto& sm = work.sortedMap->map();
        int64_t curRank = 0;
        bool findsSortKey = false;
        RowData* toCollect = nullptr;
        RowData* toDelete = nullptr;
        auto it = sm.begin();
        while (it != sm.end() && this->isInRankEnd(curRank)) {
            RowData* key = it->first;
            if (!findsSortKey && sortKeyEquals(key, sortKey)) {
                curRank += it->second;
                if (this->isInRankRange(curRank)) {
                    toCollect = inputRow;
                }
                findsSortKey = true;
                ++it;
            } else if (findsSortKey) {
                std::vector<RowData*>* inputs = findListBySortKey(work, key);
                if (inputs == nullptr) {
                    processStateStaled(sm, it);
                } else {
                    const int64_t count = it->second;
                    const int64_t rankOfLastRecord = curRank + count;
                    if (this->isInRankEnd(rankOfLastRecord)) {
                        curRank = rankOfLastRecord;
                    } else {
                        const int64_t index = this->rankEnd - curRank;
                        toDelete = (*inputs)[static_cast<size_t>(index)];
                        break;
                    }
                    ++it;
                }
            } else {
                curRank += it->second;
                ++it;
            }
        }
        if (toDelete != nullptr) {
            // Flink: collectDelete(out, inputRowSer.copy(toDelete)). The emitted row
            // must not alias a data-state row; a defensive copy matches Flink's
            // contract and removes the batch-end conversion-order dependency. The
            // copy is operator-owned and freed via emittedCopies at batch end.
            RowData* toDeleteCopy = toDelete->copy();
            work.emittedCopies.push_back(toDeleteCopy);
            this->collectDelete(toDeleteCopy, timestamp);
        }
        if (toCollect != nullptr) {
            this->collectInsert(inputRow, timestamp);
        }
    }

    // ===== Flink's retractRecordWithRowNumber =====
    // Returns true if the target row was removed from the data-state list.
    bool retractRecordWithRowNumber(PartitionWork& work, RowData* sortKey, RowData* inputRow, int64_t timestamp)
    {
        auto& sm = work.sortedMap->map();
        int64_t currentRank = 0;
        RowData* prevRow = nullptr;
        bool findsSortKey = false;
        auto it = sm.begin();
        while (it != sm.end() && this->isInRankEnd(currentRank)) {
            RowData* key = it->first;
            if (!findsSortKey && sortKeyEquals(key, sortKey)) {
                std::vector<RowData*>* inputs = findListBySortKey(work, key);
                if (inputs == nullptr) {
                    processStateStaled(sm, it);
                } else {
                    auto lit = inputs->begin();
                    while (lit != inputs->end() && this->isInRankEnd(currentRank)) {
                        RowData* currentRow = *lit;
                        if (!findsSortKey && rowsEqual(currentRow, inputRow)) {
                            prevRow = currentRow;
                            findsSortKey = true;
                            work.removedStateRows.push_back(currentRow);
                            lit = inputs->erase(lit);
                        } else if (findsSortKey) {
                            this->collectUpdateBefore(prevRow, currentRank, timestamp);
                            this->collectUpdateAfter(currentRow, currentRank, timestamp);
                            prevRow = currentRow;
                            ++lit;
                        } else {
                            ++lit;
                        }
                        currentRank += 1;
                    }
                    if (inputs->empty()) {
                        removeListEntry(work, key);
                    }
                    ++it;
                }
            } else if (findsSortKey) {
                std::vector<RowData*>* inputs = findListBySortKey(work, key);
                if (inputs == nullptr) {
                    processStateStaled(sm, it);
                } else {
                    int i = 0;
                    while (i < static_cast<int>(inputs->size()) &&
                           this->isInRankEnd(currentRank)) {
                        RowData* currentRow = (*inputs)[i];
                        this->collectUpdateBefore(prevRow, currentRank, timestamp);
                        this->collectUpdateAfter(currentRow, currentRank, timestamp);
                        prevRow = currentRow;
                        currentRank += 1;
                        ++i;
                    }
                    ++it;
                }
            } else {
                currentRank += it->second;
                ++it;
            }
        }
        if (this->isInRankEnd(currentRank)) {
            if (!findsSortKey && prevRow == nullptr) {
                stateStaledErrorHandle();
            } else {
                this->collectDelete(prevRow, currentRank, timestamp);
            }
        }
        return findsSortKey;
    }

    // ===== Flink's retractRecordWithoutRowNumber =====
    // Returns true if the target row was removed from the data-state list.
    bool retractRecordWithoutRowNumber(PartitionWork& work, RowData* sortKey, RowData* inputRow, int64_t timestamp)
    {
        auto& sm = work.sortedMap->map();
        int64_t nextRank = 1;
        bool findsSortKey = false;
        auto it = sm.begin();
        while (it != sm.end() && this->isInRankEnd(nextRank)) {
            RowData* key = it->first;
            if (!findsSortKey && sortKeyEquals(key, sortKey)) {
                std::vector<RowData*>* inputs = findListBySortKey(work, key);
                if (inputs == nullptr) {
                    processStateStaled(sm, it);
                } else {
                    auto lit = inputs->begin();
                    while (lit != inputs->end() && this->isInRankEnd(nextRank)) {
                        RowData* prevRow = *lit;
                        if (!findsSortKey && rowsEqual(prevRow, inputRow)) {
                            this->collectDelete(prevRow, nextRank, timestamp);
                            nextRank -= 1;
                            findsSortKey = true;
                            work.removedStateRows.push_back(prevRow);
                            lit = inputs->erase(lit);
                        } else if (findsSortKey) {
                            if (nextRank == this->rankEnd) {
                                this->collectInsert(prevRow, nextRank, timestamp);
                            }
                            ++lit;
                        } else {
                            ++lit;
                        }
                        nextRank += 1;
                    }
                    if (inputs->empty()) {
                        removeListEntry(work, key);
                    }
                    ++it;
                }
            } else if (findsSortKey) {
                const int64_t count = it->second;
                const int64_t rankOfLastRecord = nextRank + count - 1;
                if (rankOfLastRecord < this->rankEnd) {
                    nextRank = rankOfLastRecord + 1;
                    ++it;
                } else {
                    const int64_t index = this->rankEnd - nextRank;
                    std::vector<RowData*>* inputs = findListBySortKey(work, key);
                    if (inputs == nullptr) {
                        processStateStaled(sm, it);
                    } else {
                        RowData* toAdd = (*inputs)[static_cast<size_t>(index)];
                        this->collectInsert(toAdd, nextRank, timestamp);
                        break;
                    }
                }
            } else {
                nextRank += it->second;
                ++it;
            }
        }
        return findsSortKey;
    }

    // Flink's processStateStaled: the sorted map holds a key that no longer has a
    // data-state list (e.g. TTL cleaned the list) - drop the stale sorted-map entry and
    // warn leniently. `it` is updated to the erased/next iterator.
    void processStateStaled(typename SortedMapValue::Map& sm,
                            typename SortedMapValue::Map::iterator& it)
    {
        RowData* staleKey = it->first;
        it = sm.erase(it);
        delete staleKey;
        stateStaledErrorHandle();
    }

    void stateStaledErrorHandle() const
    {
        WARN_RELEASE(
            "RetractableTopNFunction: the state is cleared because of state ttl. "
            "This will result in incorrect result. You can increase the state ttl to avoid this.");
    }

    // ===== per-row dispatch =====
    void processRow(omnistream::VectorBatch* batch, int row, PartitionWork& work,
                    typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx)
    {
        ctx.setCurrentKey(work.key);
        const int64_t timestamp = batch->getTimestamp(row);
        if (RowDataUtil::isAccumulateMsg(batch->getRowKind(row))) {
            RowData* inputRow = batch->extractRowData(row);
            // Mirror Flink's RetractableTopNFunction.processElement: erase the RowKind
            // before the row enters the data-state / sorted-map so state bytes stay
            // byte-identical to Flink (BinaryRowData equals/checkpoint compatibility).
            inputRow->setRowKind(RowKind::INSERT);
            // initRankEnd mirrors Flink's per-record call: for a variable rank range
            // it reads (and on the first record per partition, freezes) the rank-end
            // column under the partition's key, so this->rankEnd is correct for the
            // emit/retract algorithms below. For a constant range it is a no-op.
            this->initRankEnd(inputRow);
            RowData* sortKey = projectSortKey(inputRow);
            auto& sm = work.sortedMap->map();
            auto it = sm.find(sortKey);
            if (it == sm.end()) {
                // Flink: sortedMap.put(sortKey, 1L) for a new sort key.
                it = sm.emplace(sortKey, 1L).first;
            } else {
                // Flink: sortedMap.put(sortKey, sortedMap.get(sortKey) + 1).
                it->second += 1;
                delete sortKey;
            }
            if (this->outputRankNumber || this->hasOffset()) {
                emitRecordsWithRowNumber(work, it->first, inputRow, timestamp);
            } else {
                emitRecordsWithoutRowNumber(work, it->first, inputRow, timestamp);
            }
            // Flink: append input to dataState.get(sortKey) and put back.
            appendToDataState(work, it->first, inputRow);
        } else {
            RowData* inputRow = batch->extractRowData(row);
            work.retractRows.push_back(inputRow);
            // Erase the RowKind like Flink does before the equaliser / state access
            // (output RowKinds are tracked separately via collectedRowKinds).
            inputRow->setRowKind(RowKind::INSERT);
            this->initRankEnd(inputRow);
            RowData* sortKey = projectSortKey(inputRow);
            const bool stateRemoved =
                (this->outputRankNumber || this->hasOffset())
                    ? retractRecordWithRowNumber(work, sortKey, inputRow, timestamp)
                    : retractRecordWithoutRowNumber(work, sortKey, inputRow, timestamp);

            // Flink updates the sorted map for every retract message, regardless of the
            // search outcome; a missing sort key is a sorted-map miss -> lenient warn.
            auto& sm = work.sortedMap->map();
            auto it = sm.find(sortKey);
            if (it != sm.end()) {
                it->second -= 1;
                if (it->second == 0) {
                    RowData* staleKey = it->first;
                    sm.erase(it);
                    delete staleKey;
                }
            } else {
                stateStaledErrorHandle();
            }

            if (!stateRemoved) {
                // Flink's !stateRemoved fallback: the row sits beyond the scan bound;
                // remove it from the sort key's data-state list by equaliser silently.
                std::vector<RowData*>* list = findListBySortKey(work, sortKey);
                if (list != nullptr) {
                    for (auto lit = list->begin(); lit != list->end(); ++lit) {
                        if (rowsEqual(*lit, inputRow)) {
                            work.removedStateRows.push_back(*lit);
                            list->erase(lit);
                            break;
                        }
                    }
                    if (list->empty()) {
                        removeListEntry(work, sortKey);
                    }
                }
            }
            delete sortKey;
        }
    }

    MapState<RowData*, std::vector<RowData*>*>* dataState = nullptr;
    ValueState<SortedMapValue*>* treeMap = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
    KeySelector<K> partitionKeySelector;
    SortKeyCmp sortKeyCmp_;
};

#endif // OMNISTREAM_RETRACTABLETOPNFUNCTION_H
