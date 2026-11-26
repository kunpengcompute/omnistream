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

#include "WindowRankProcessor.h"

#include <algorithm>

#include "table/data/binary/BinaryRowData.h"
#include "table/types/logical/LogicalType.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/utils/TimeWindowUtil.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/memory/DataOutputSerializer.h"

using namespace omniruntime::type;
using json = nlohmann::json;

WindowRankProcessor::WindowRankProcessor(const nlohmann::json& description, Output* output) : output(output)
{
    partitionKeyIndices_ = description.value("partitionKey", std::vector<int32_t>{});
    sortFieldIndices_ = description.value("sortFieldIndices", std::vector<int32_t>{});
    sortAscendingOrders_ = description.value("sortAscendingOrders", std::vector<bool>{});
    sortNullsIsLast_ = description.value("sortNullsIsLast", std::vector<bool>{});
    rankStart_ = description.value("rankStart", static_cast<int64_t>(1));
    rankEnd_ = description.value("rankEnd", static_cast<int64_t>(1));
    outputRankNumber_ = description.value("outputRankNumber", false);
    windowEndIndex_ = description.value("windowEndIndex", -1);
    shiftTimeZone_ = description.value("shiftTimeZone", std::string("UTC"));

    inputTypes_ = description.at("inputTypes").get<std::vector<std::string>>();
    for (const auto& typeStr : inputTypes_) {
        inputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }
    outputTypes_ = description.at("outputTypes").get<std::vector<std::string>>();
    for (const auto& typeStr : outputTypes_) {
        outputTypeIds_.push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
    }

    if (sortFieldIndices_.size() != sortAscendingOrders_.size() ||
        sortFieldIndices_.size() != sortNullsIsLast_.size()) {
        THROW_LOGIC_EXCEPTION("WindowRankProcessor: sort contract arrays must have the same size");
    }
    for (int32_t col : sortFieldIndices_) {
        if (col < 0 || col >= static_cast<int32_t>(inputTypes_.size())) {
            THROW_LOGIC_EXCEPTION("WindowRankProcessor: sort field index out of range: " + std::to_string(col));
        }
        sortFieldTypeIds_.push_back(inputTypeIds_[col]);
    }
    if (rankStart_ < 1 || rankEnd_ < 1 || rankEnd_ < rankStart_) {
        THROW_LOGIC_EXCEPTION(
            "WindowRankProcessor: illegal rank range [" + std::to_string(rankStart_) + ", " +
            std::to_string(rankEnd_) + "]");
    }
    if (windowEndIndex_ < 0 || windowEndIndex_ >= static_cast<int32_t>(inputTypes_.size())) {
        THROW_LOGIC_EXCEPTION("WindowRankProcessor: illegal windowEndIndex " + std::to_string(windowEndIndex_));
    }
    if (inputTypeIds_[windowEndIndex_] != DataTypeId::OMNI_LONG &&
        inputTypeIds_[windowEndIndex_] != DataTypeId::OMNI_TIMESTAMP &&
        inputTypeIds_[windowEndIndex_] != DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE &&
        inputTypeIds_[windowEndIndex_] != DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        THROW_LOGIC_EXCEPTION("WindowRankProcessor: window end column must be TIMESTAMP(3) rowtime (long millis)");
    }
    const size_t expectedOutput = inputTypes_.size() + (outputRankNumber_ ? 1 : 0);
    if (outputTypes_.size() != expectedOutput) {
        THROW_LOGIC_EXCEPTION(
            "WindowRankProcessor: output arity " + std::to_string(outputTypes_.size()) +
            " does not match input arity + optional rank column " + std::to_string(expectedOutput));
    }

    std::vector<int32_t> keyedTypes;
    for (int32_t index : partitionKeyIndices_) {
        if (index < 0 || index >= static_cast<int32_t>(inputTypes_.size())) {
            THROW_LOGIC_EXCEPTION("WindowRankProcessor: partition key index out of range: " + std::to_string(index));
        }
        keyedTypes.push_back(static_cast<int32_t>(inputTypeIds_[index]));
    }
    keySelector_ = std::make_unique<KeySelector<KeyType>>(keyedTypes, partitionKeyIndices_);
    collector_ = std::make_unique<TimestampedCollector>(this->output);
}

void WindowRankProcessor::open(
    AbstractKeyedStateBackend<WindowRankProcessor::KeyType>* keyedStateBackend,
    const nlohmann::json& config,
    StreamingRuntimeContext<WindowRankProcessor::KeyType>* runtimeCtx,
    InternalTimerServiceImpl<WindowRankProcessor::KeyType, int64_t>* internalTimerService)
{
    this->stateBackend_ = keyedStateBackend;
    this->internalTimerService_ = internalTimerService;
    omnistream::checkStateType(keyedStateBackend->getStateType(), "WindowRankProcessor");

    // state schema mirrors Flink WindowRankProcessor.open():
    //   MapStateDescriptor<RowData, List<RowData>>("window_rank", sortKeySerializer,
    //   ListSerializer(inputSerializer)) with the LongSerializer namespace.
    std::vector<std::string> sortTypeNames;
    for (int32_t col : sortFieldIndices_) {
        sortTypeNames.push_back(inputTypes_[col]);
    }
    omnistream::RowType sortKeyType(false, sortTypeNames);
    omnistream::RowType inputRowType(false, inputTypes_);

    auto* valueSerializer = new BinaryRowDataListSerializer(
        static_cast<int>(inputTypes_.size()), new RowDataSerializer(&inputRowType));
    auto* mapStateDescriptor = new MapStateDescriptor<UK, UV>(
        "window_rank", new RowDataSerializer(&sortKeyType), valueSerializer);
    mapStateDescriptor->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);

    auto* mapState = keyedStateBackend->template getOrCreateKeyedState<int64_t, MapState<UK, UV>, StateMap*>(
        new LongSerializer(), mapStateDescriptor);
    auto* kvState = dynamic_cast<InternalKvState<KeyType, int64_t, StateMap*>*>(mapState);
    if (kvState == nullptr) {
        THROW_LOGIC_EXCEPTION("WindowRankProcessor: keyed state backend returned a state without namespace support");
    }
    windowState_ = std::make_unique<WindowMapState<KeyType, int64_t, UK, UV>>(kvState, mapState);

    backendType_ = stateBackend_->getStateType();
}

void WindowRankProcessor::initializeWatermark(int64_t watermark)
{
    currentProgress = watermark;
}

void WindowRankProcessor::advanceProgress(long progress)
{
    // Flink forwards this to the RecordsWindowBuffer to flush windows that are
    // about to fire; the native processor writes rows to the state directly, so
    // only the progress bookkeeping is needed here.
    if (progress > currentProgress) {
        currentProgress = progress;
    }
}

void WindowRankProcessor::prepareCheckpoint() {}

bool WindowRankProcessor::processBatch(omnistream::VectorBatch* vectorbatch)
{
    if (vectorbatch == nullptr || vectorbatch->GetRowCount() == 0) {
        return false;
    }
    const int rowCount = vectorbatch->GetRowCount();

    // Stage 1: per-row late filtering, kind check, key/sort-key/row extraction.
    // (Flink: WindowRankProcessor.processElement + TopNRecordsCombiner.combine,
    // merged because the native side has no RecordsWindowBuffer.)
    std::vector<PendingRow> pendings;
    pendings.reserve(static_cast<size_t>(rowCount));
    for (int i = 0; i < rowCount; ++i) {
        RowKind kind = vectorbatch->getRowKind(i);
        if (kind != RowKind::INSERT) {
            THROW_RUNTIME_ERROR(
                "Window rank does not support input RowKind: " + std::to_string(static_cast<int>(kind)));
        }
        int64_t windowEnd =
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(vectorbatch->Get(windowEndIndex_))->GetValue(i);
        if (TimeWindowUtil::isWindowFired(windowEnd, currentProgress, shiftTimeZone_)) {
            // element is late and should be dropped
            continue;
        }
        PendingRow pending;
        pending.key = keySelector_->getKey(vectorbatch, i);
        pending.windowEnd = windowEnd;
        RowData* row = vectorbatch->extractRowData(i);
        pending.sortKey = projectSortKey(row);
        pending.row = row;
        pendings.push_back(std::move(pending));
    }

    // Stage 2: group the rows by (key content, window). This stage only
    // partitions the pending rows; it must not touch the state — on RocksDB the
    // per-key entries cache is dropped on every load (the cache is keyed by
    // current key only), so loading a later group would free the cached map a
    // still-open earlier group is holding. All state access happens in stage 3,
    // where each group's load → append → flush lifecycle closes before the next
    // group starts.
    std::unordered_map<std::string, size_t> groupIndex;
    std::vector<WindowGroup> groups;
    for (auto& pending : pendings) {
        DataOutputSerializer keyOutput;
        OutputBufferStatus bufferStatus;
        keyOutput.setBackendBuffer(&bufferStatus);
        BinaryRowDataSerializer keySerializer(static_cast<int>(partitionKeyIndices_.size()));
        keySerializer.serialize(pending.key.get(), keyOutput);
        std::string groupKey(reinterpret_cast<const char*>(keyOutput.getData()), keyOutput.length());
        groupKey.append(reinterpret_cast<const char*>(&pending.windowEnd), sizeof(pending.windowEnd));

        auto it = groupIndex.find(groupKey);
        if (it == groupIndex.end()) {
            WindowGroup group;
            group.key = pending.key;
            group.windowEnd = pending.windowEnd;
            groups.push_back(std::move(group));
            groupIndex.emplace(groupKey, groups.size() - 1);
            it = groupIndex.find(groupKey);
        }
        groups[it->second].pendings.push_back(std::move(pending));
    }

    // Stage 3: process each group as a closed unit — the state access below
    // resolves the current key from the state backend's key context, so switch
    // it before touching the state, append all rows, flush to RocksDB (which
    // releases the cached map), then register the window timer. Flink registers
    // the timer from TopNRecordsCombiner.combine; registering it here on the
    // first touch of the window is equivalent, the timer service dedups.
    for (auto& group : groups) {
        stateBackend_->setCurrentKey(group.key);
        for (auto& pending : group.pendings) {
            appendRowToGroup(group, pending);
        }
        flushGroup(group);
        internalTimerService_->registerEventTimeTimer(
            group.windowEnd, TimeWindowUtil::toEpochMillsForTimer(group.windowEnd - 1, shiftTimeZone_));
    }
    return false;
}

void WindowRankProcessor::loadGroupMap(WindowGroup& group)
{
    if (group.map != nullptr) {
        return;
    }
    if (backendType_ == omnistream::StateType::ROCKSDB) {
        // Drop a stale per-key cache so entries() re-reads this (key, window)
        // from RocksDB (the cache is keyed by current key only).
        windowState_->clearEntriesCache();
        group.map = windowState_->entries(group.windowEnd);
        group.mapOwned = false;
        if (group.map == nullptr) {
            group.map = new StateMap();
            group.mapOwned = true;
        }
    } else {
        group.map = windowState_->entries(group.windowEnd);
        group.mapOwned = false;
    }
}

void WindowRankProcessor::appendRowToGroup(WindowGroup& group, PendingRow& pending)
{
    loadGroupMap(group);
    if (group.map != nullptr) {
        for (auto& entry : *group.map) {
            if (*entry.first == *pending.sortKey) {
                entry.second->push_back(pending.row);
                delete pending.sortKey; // not adopted by the state, the stored key wins
                return;
            }
        }
    }
    auto* list = new std::vector<RowData*>();
    list->push_back(pending.row);
    if (backendType_ == omnistream::StateType::ROCKSDB) {
        // The key is adopted by the in-memory copy; it is freed together with
        // the map (owned delete or clearEntriesCache).
        if (group.map == nullptr) {
            group.map = new StateMap();
            group.mapOwned = true;
        }
        group.map->emplace(pending.sortKey, list);
    } else {
        // HEAP: the live map owns key and list; ownership of the sort key moves
        // to the state table (the emhash7 map destructor deletes its keys).
        windowState_->put(group.windowEnd, pending.sortKey, list);
        group.map = windowState_->entries(group.windowEnd);
    }
}

void WindowRankProcessor::flushGroup(WindowGroup& group)
{
    if (backendType_ != omnistream::StateType::ROCKSDB || group.map == nullptr) {
        return; // HEAP rows already live in the state table
    }
    for (auto& entry : *group.map) {
        if (entry.second == nullptr || entry.second->empty()) {
            windowState_->remove(group.windowEnd, entry.first);
        } else {
            windowState_->put(group.windowEnd, entry.first, entry.second);
        }
    }
    // Rows are serialized into RocksDB; free the in-memory copies. The sort
    // keys (RowData*) are NOT freed here on purpose: both the owned map's
    // `delete` and the borrowed cache's clearEntriesCache() run the emhash7
    // HashMap destructor, which deletes every pointer key (emhash7.hpp
    // ~HashMap: `else if constexpr (std::is_pointer_v<KeyT>) ... delete
    // it->first`), so freeing them here would double-delete.
    for (auto& entry : *group.map) {
        if (entry.second != nullptr) {
            for (RowData* row : *entry.second) {
                delete row;
            }
            delete entry.second;
        }
    }
    if (group.mapOwned) {
        delete group.map;
        group.mapOwned = false;
    } else {
        windowState_->clearEntriesCache();
    }
    group.map = nullptr;
}

void WindowRankProcessor::fireWindow(int64_t windowEnd)
{
    // Flink: load the state into a TopNBuffer and emit [rankStart, rankEnd].
    // Native: sort the entries in place and walk the ranks; identical output
    // because the range filter is applied on the full window either way.
    if (backendType_ == omnistream::StateType::ROCKSDB) {
        windowState_->clearEntriesCache();
    }
    StateMap* map = windowState_->entries(windowEnd);
    if (map == nullptr) {
        return;
    }

    std::vector<std::pair<UK, UV>> entries;
    entries.reserve(map->size());
    for (auto it = map->begin(); it != map->end(); ++it) {
        entries.emplace_back(it->first, it->second);
    }
    std::stable_sort(
        entries.begin(), entries.end(), [this](const std::pair<UK, UV>& a, const std::pair<UK, UV>& b) {
            return sortKeyLess(a.first, b.first);
        });

    std::vector<std::pair<RowData*, int64_t>> collected;
    int64_t currentRank = 1;
    for (const auto& entry : entries) {
        if (currentRank > rankEnd_) {
            break;
        }
        for (RowData* row : *entry.second) {
            if (currentRank > rankEnd_) {
                break;
            }
            if (currentRank >= rankStart_) {
                collected.emplace_back(row, currentRank);
            }
            currentRank += 1;
        }
    }
    if (!collected.empty()) {
        collector_->collect(buildOutputBatch(collected));
    }

    if (backendType_ == omnistream::StateType::ROCKSDB) {
        // The deserialized copies are ours; free rows, lists and keys (the
        // keys go with the borrowed cache map, cleared below).
        for (const auto& entry : entries) {
            if (entry.second != nullptr) {
                for (RowData* row : *entry.second) {
                    delete row;
                }
                delete entry.second;
            }
        }
        windowState_->clearEntriesCache();
    }
    // HEAP keeps the rows alive for clearWindow (which runs right after).
}

void WindowRankProcessor::clearWindow(int64_t windowEnd)
{
    if (backendType_ == omnistream::StateType::ROCKSDB) {
        // Flink: windowState.clear(windowEnd) — physical delete in RocksDB.
        windowState_->clear(windowEnd);
        windowState_->clearEntriesCache();
        return;
    }
    // HEAP: the live map owns nothing itself — rows and lists are freed here
    // before the state table removes (and deletes) the map and its keys.
    StateMap* map = windowState_->entries(windowEnd);
    if (map != nullptr) {
        for (auto& entry : *map) {
            if (entry.second != nullptr) {
                for (RowData* row : *entry.second) {
                    delete row;
                }
                delete entry.second;
            }
        }
    }
    windowState_->clear(windowEnd);
}

void WindowRankProcessor::close() {}

TypeSerializer* WindowRankProcessor::createWindowSerializer()
{
    return LongSerializer::INSTANCE;
}

Output* WindowRankProcessor::getOutput()
{
    return output;
}

WindowRankProcessor::UK WindowRankProcessor::projectSortKey(RowData* row) const
{
    const int arity = static_cast<int>(sortFieldIndices_.size());
    auto* key = BinaryRowData::createBinaryRowDataWithMem(arity);
    for (size_t i = 0; i < sortFieldIndices_.size(); ++i) {
        const int col = sortFieldIndices_[i];
        if (row->isNullAt(col)) {
            key->setNullAt(static_cast<int>(i));
            continue;
        }
        switch (sortFieldTypeIds_[i]) {
            case DataTypeId::OMNI_INT:
            case DataTypeId::OMNI_DATE32:
                key->setInt(static_cast<int>(i), *row->getInt(col));
                break;
            case DataTypeId::OMNI_LONG:
            case DataTypeId::OMNI_DECIMAL64:
            case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP:
                key->setLong(static_cast<int>(i), *row->getLong(col));
                break;
            case DataTypeId::OMNI_DOUBLE:
                key->setDouble(static_cast<int>(i), *row->getDouble(col));
                break;
            case DataTypeId::OMNI_BOOLEAN:
                key->setBool(static_cast<int>(i), *row->getBool(col));
                break;
            case DataTypeId::OMNI_CHAR:
            case DataTypeId::OMNI_VARCHAR:
                key->setStringView(static_cast<int>(i), row->getStringView(col));
                break;
            default:
                THROW_LOGIC_EXCEPTION("WindowRankProcessor: unsupported sort key column type");
        }
    }
    return key;
}

int WindowRankProcessor::compareSortKeyColumn(UK a, UK b, size_t pos) const
{
    // Mirrors Flink's generated comparator (GenerateUtils.generateRowCompare):
    // the null branch is direction-independent (nullIsLast decides whether a
    // null is the largest value), the value branch is reversed for DESC.
    const bool nullA = a->isNullAt(static_cast<int>(pos));
    const bool nullB = b->isNullAt(static_cast<int>(pos));
    if (nullA && nullB) {
        return 0;
    }
    const int nullRet = sortNullsIsLast_[pos] ? 1 : -1;
    if (nullA) {
        return nullRet;
    }
    if (nullB) {
        return -nullRet;
    }
    switch (sortFieldTypeIds_[pos]) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_DATE32: {
            int32_t av = *a->getInt(static_cast<int>(pos));
            int32_t bv = *b->getInt(static_cast<int>(pos));
            return av < bv ? -1 : (av > bv ? 1 : 0);
        }
        case DataTypeId::OMNI_LONG:
        case DataTypeId::OMNI_DECIMAL64:
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP: {
            int64_t av = *a->getLong(static_cast<int>(pos));
            int64_t bv = *b->getLong(static_cast<int>(pos));
            return av < bv ? -1 : (av > bv ? 1 : 0);
        }
        case DataTypeId::OMNI_DOUBLE: {
            double av = *a->getDouble(static_cast<int>(pos));
            double bv = *b->getDouble(static_cast<int>(pos));
            return av < bv ? -1 : (av > bv ? 1 : 0);
        }
        case DataTypeId::OMNI_BOOLEAN: {
            bool av = *a->getBool(static_cast<int>(pos));
            bool bv = *b->getBool(static_cast<int>(pos));
            return av == bv ? 0 : (av ? 1 : -1);
        }
        case DataTypeId::OMNI_CHAR:
        case DataTypeId::OMNI_VARCHAR: {
            std::string_view av = a->getStringView(static_cast<int>(pos));
            std::string_view bv = b->getStringView(static_cast<int>(pos));
            int cmp = av.compare(bv);
            return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
        }
        default:
            THROW_LOGIC_EXCEPTION("WindowRankProcessor: unsupported sort key column type");
    }
}

bool WindowRankProcessor::sortKeyLess(UK a, UK b) const
{
    for (size_t pos = 0; pos < sortFieldIndices_.size(); ++pos) {
        const bool nullA = a->isNullAt(static_cast<int>(pos));
        const bool nullB = b->isNullAt(static_cast<int>(pos));
        if (nullA != nullB) {
            // Flink's generated comparator (GenerateUtils.generateRowCompare) returns
            // the direction-independent null order from the null branch — the null
            // position in the output does NOT flip with ASC/DESC.
            if (sortNullsIsLast_[pos]) {
                return nullB; // nulls last: only (non-null, null) is ordered before
            }
            return nullA; // nulls first: (null, non-null) is ordered before
        }
        if (nullA) {
            continue; // both null: tie on this column, next column decides
        }
        int cmp = compareSortKeyColumn(a, b, pos);
        if (cmp != 0) {
            return sortAscendingOrders_[pos] ? cmp < 0 : cmp > 0;
        }
    }
    return false;
}

omnistream::VectorBatch* WindowRankProcessor::buildOutputBatch(
    const std::vector<std::pair<RowData*, int64_t>>& collected) const
{
    const int numRows = static_cast<int>(collected.size());
    auto* batch = omnistream::VectorBatch::CreateVectorBatch(numRows, outputTypeIds_);
    const int inputCols = static_cast<int>(inputTypeIds_.size());
    for (int i = 0; i < numRows; ++i) {
        for (int col = 0; col < inputCols; ++col) {
            copyRowColumn(batch, col, i, collected[i].first);
        }
        if (outputRankNumber_) {
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(inputCols))
                ->SetValue(i, collected[i].second);
        }
        batch->setRowKind(i, RowKind::INSERT);
    }
    return batch;
}

void WindowRankProcessor::copyRowColumn(omnistream::VectorBatch* batch, int col, int rowIdx, RowData* row) const
{
    switch (inputTypeIds_[col]) {
        case DataTypeId::OMNI_LONG:
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case DataTypeId::OMNI_TIMESTAMP: {
            auto* vector = static_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(col));
            if (row->isNullAt(col)) {
                vector->SetNull(rowIdx);
            } else {
                vector->SetValue(rowIdx, *row->getLong(col));
            }
            break;
        }
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_DATE32: {
            auto* vector = static_cast<omniruntime::vec::Vector<int32_t>*>(batch->Get(col));
            if (row->isNullAt(col)) {
                vector->SetNull(rowIdx);
            } else {
                vector->SetValue(rowIdx, *row->getInt(col));
            }
            break;
        }
        case DataTypeId::OMNI_BOOLEAN: {
            auto* vector = static_cast<omniruntime::vec::Vector<bool>*>(batch->Get(col));
            if (row->isNullAt(col)) {
                vector->SetNull(rowIdx);
            } else {
                vector->SetValue(rowIdx, *row->getBool(col));
            }
            break;
        }
        case DataTypeId::OMNI_DOUBLE: {
            auto* vector = static_cast<omniruntime::vec::Vector<double>*>(batch->Get(col));
            if (row->isNullAt(col)) {
                vector->SetNull(rowIdx);
            } else {
                vector->SetValue(rowIdx, *row->getDouble(col));
            }
            break;
        }
        case DataTypeId::OMNI_CHAR:
        case DataTypeId::OMNI_VARCHAR: {
            using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
            auto* vector = static_cast<VarcharVector*>(batch->Get(col));
            if (row->isNullAt(col)) {
                vector->SetNull(rowIdx);
            } else {
                vector->SetValue(rowIdx, row->getStringView(col));
            }
            break;
        }
        default:
            THROW_LOGIC_EXCEPTION("WindowRankProcessor: unsupported output column type");
    }
}
