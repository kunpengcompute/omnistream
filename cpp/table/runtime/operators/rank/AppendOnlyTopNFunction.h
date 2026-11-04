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

#ifndef OMNISTREAM_APPENDONLYTOPNFUNCTION_H
#define OMNISTREAM_APPENDONLYTOPNFUNCTION_H

#include <algorithm>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "AbstractTopNFunction.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

// One top-N per partition key. The only persisted state is Flink's
// data-state-with-append: MapState<sortKey RowData, List<input RowData>>.
// Rows that share a sort key stay in one list, in insertion order. Sort keys are
// ordered by the rank comparator. A batch keeps touched partitions in memory.
// The first touch reads that map. A partition whose membership does not change
// is not written. A changed partition puts each changed sort key and removes
// sort keys that fell out of the top-N. Nothing is kept for the next batch.
template <typename K>
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit AppendOnlyTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices)
    {
        // An empty sort key list is the streaming Limit shape (SortSpec.ANY):
        // every row ranks in arrival order within one constant group - all sort
        // comparisons degenerate to "equal" (FirstN semantics). The general
        // shape supports a single BIGINT descending key only.
        if (this->sortKeyTypeIds.size() > 1) {
            NOT_IMPL_EXCEPTION;
        }
        if (this->sortKeyTypeIds.size() == 1 &&
            (this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0])) {
            NOT_IMPL_EXCEPTION;
        }
        limitShape = this->partitionKeyIndices.empty();
    }

    ~AppendOnlyTopNFunction() = default;

    void open(const Configuration& context) override
    {
        int inputArity = static_cast<int>(this->inputRowType->size());
        // Streaming Limit shape: arity-0 sort key type (the single constant group).
        std::vector<std::string> sortTypeNames{};
        if (!this->sortKeyIndices.empty()) {
            sortTypeNames.push_back((*this->inputTypeNames)[this->sortKeyIndices[0]]);
        }
        omnistream::RowType sortKeyType(false, sortTypeNames);
        auto* sortKeySer = new RowDataSerializer(&sortKeyType);
        omnistream::RowType inputType(false, *this->inputTypeNames);
        auto* rowListSerializer = new BinaryRowDataListSerializer(inputArity, new RowDataSerializer(&inputType));
        auto* desc = new MapStateDescriptor<RowData*, std::vector<RowData*>*>(
            "data-state-with-append", sortKeySer, rowListSerializer);
        desc->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        dataState = runtimeContext->template getMapState<RowData*, std::vector<RowData*>*>(desc);
        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "AppendOnlyTopNFunction");
    }

    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        this->initRankEnd(nullptr);
        omnistream::VectorBatch* batch = inputBatchGuard.get();
        int rowCnt = batch->GetRowCount();

        std::vector<OutRow> outputs;
        std::vector<RowData*> deleteAfterOutput;
        std::unordered_map<int64_t, RowData*> retainedKeys;
        std::unordered_map<K, PartitionWork, PartitionKeyHash, PartitionKeyEq> partitionWork(
            16, PartitionKeyHash{this}, PartitionKeyEq{this});
        std::vector<K> touched;

        for (int i = 0; i < rowCnt; ++i) {
            K rawKey = partitionKeySelector.getKey(batch, i);
            K partitionKey = retainKey(rawKey, retainedKeys);
            auto [it, created] = partitionWork.try_emplace(partitionKey);
            if (created) {
                ctx.setCurrentKey(partitionKey);
                loadPartition(it->second);
                it->second.key = partitionKey;
                touched.push_back(partitionKey);
            }
            considerRow(batch, i, it->second);
        }
        flushPartition(batch, ctx, touched, partitionWork, outputs, deleteAfterOutput);
        if (rocksBackend()) {
            dataState->clearEntriesCache();
        }

        if (!outputs.empty()) {
            this->collectOutputBatch(out, buildOutput(outputs));
        } else {
            INFO_RELEASE("AppendOnlyTopNFunction processBatch collected no rows");
        }

        for (RowData* row : deleteAfterOutput) {
            delete row;
        }
        for (auto& pair : retainedKeys) {
            delete pair.second;
        }
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    struct WorkItem {
        bool sortNull = false;
        int64_t sortValue = 0;
        bool fromBatch = false;
        int32_t batchRow = -1;
        int groupIndex = -1;
        int indexInGroup = -1;
        RowData* stateRow = nullptr;
        int oldRank = 0;
        int64_t timestamp = 0;
    };

    struct LoadedGroup {
        bool sortNull = false;
        int64_t sortValue = 0;
        RowData* sortKey = nullptr;
        std::vector<RowData*>* rows = nullptr;
        int rowCount = 0;
        bool kept = false;
    };

    struct OutRow {
        omnistream::VectorBatch* batch = nullptr;
        int32_t batchRow = -1;
        RowData* stateRow = nullptr;
        int64_t rank = 0;
        int64_t timestamp = 0;
        RowKind kind = RowKind::INSERT;
    };

    struct PartitionWork {
        bool open = false;
        K key{};
        std::vector<WorkItem> rows;
        std::vector<WorkItem> evicted;
        std::vector<LoadedGroup> loadedGroups;
        int loadedCount = 0;
        bool hadState = false;
    };

    MapState<RowData*, std::vector<RowData*>*>* dataState = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
    KeySelector<K> partitionKeySelector;
    // Streaming Limit shape (empty partition key, ALL_IN_ONE): one global
    // partition, the key row is the constant arity-0 row.
    bool limitShape = false;

    static bool samePartition(const K& left, const K& right)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            return left != nullptr && right != nullptr && *left->getLong(0) == *right->getLong(0);
        } else {
            return left == right;
        }
    }

    struct PartitionKeyHash {
        const AppendOnlyTopNFunction* fn = nullptr;
        size_t operator()(const K& key) const
        {
            if constexpr (std::is_same<K, RowData*>::value) {
                // Streaming Limit shape: every row shares the arity-0 key, so all
                // keys must hash (and compare) equal - reading getLong(0) on the
                // empty key row would throw IndexOutOfBoundsException.
                if (fn != nullptr && fn->limitShape) {
                    return 0;
                }
                return key == nullptr ? 0 : std::hash<int64_t>{}(*key->getLong(0));
            } else {
                return std::hash<K>{}(key);
            }
        }
    };

    struct PartitionKeyEq {
        const AppendOnlyTopNFunction* fn = nullptr;
        bool operator()(const K& left, const K& right) const
        {
            if (fn != nullptr && fn->limitShape) {
                if constexpr (std::is_same<K, RowData*>::value) {
                    return left == right || (left != nullptr && right != nullptr);
                } else {
                    return left == right;
                }
            }
            return samePartition(left, right);
        }
    };

    K retainKey(K key, std::unordered_map<int64_t, RowData*>& retainedKeys)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            // Streaming Limit shape: single slot for the one arity-0 key.
            if (limitShape) {
                auto found = retainedKeys.find(0);
                if (found != retainedKeys.end()) {
                    delete key;
                    return found->second;
                }
                retainedKeys.emplace(0, key);
                return key;
            }
            int64_t id = *key->getLong(0);
            auto found = retainedKeys.find(id);
            if (found != retainedKeys.end()) {
                delete key;
                return found->second;
            }
            retainedKeys.emplace(id, key);
            return key;
        } else {
            return key;
        }
    }

    bool nullsLast() const
    {
        return this->sortNullsIsLast.empty() || this->sortNullsIsLast[0];
    }

    // Single long sort key, descending. Equal keys keep insertion order.
    bool strictlyBetter(bool candNull, int64_t cand, bool otherNull, int64_t other) const
    {
        if (candNull && otherNull) {
            return false;
        }
        if (candNull) {
            return !nullsLast();
        }
        if (otherNull) {
            return nullsLast();
        }
        return cand > other;
    }

    void readBatchSort(omnistream::VectorBatch* batch, int row, bool& isNull, int64_t& value) const
    {
        if (this->sortKeyIndices.empty()) {
            // Streaming Limit shape: no sort column, every row is equal.
            isNull = false;
            value = 0;
            return;
        }
        auto* column = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(this->sortKeyIndices[0]));
        if (column->IsNull(row)) {
            isNull = true;
            value = 0;
            return;
        }
        isNull = false;
        value = column->GetValue(row);
    }

    bool rocksBackend() const
    {
        return stateType_ == omnistream::StateType::ROCKSDB;
    }

    RowData* makeSortKey(bool isNull, int64_t value) const
    {
        if (this->sortKeyIndices.empty()) {
            // Streaming Limit shape: the constant arity-0 group key.
            return BinaryRowData::createBinaryRowDataWithMem(0);
        }
        auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
        if (isNull) {
            row->setNullAt(0);
        } else {
            row->setLong(0, value);
        }
        return row;
    }

    void readSortKey(RowData* key, bool& isNull, int64_t& value) const
    {
        if (key == nullptr) {
            throw std::runtime_error("AppendOnlyTopNFunction sort key is missing");
        }
        if (this->sortKeyIndices.empty()) {
            // Streaming Limit shape: the single arity-0 key row is never null.
            isNull = false;
            value = 0;
            return;
        }
        isNull = key->isNullAt(0);
        value = isNull ? 0 : *key->getLong(0);
    }

    bool sameSortKey(bool leftNull, int64_t left, bool rightNull, int64_t right) const
    {
        if (leftNull || rightNull) {
            return leftNull && rightNull;
        }
        return left == right;
    }

    void fillFromGroups(PartitionWork& active)
    {
        int count = 0;
        for (const LoadedGroup& group : active.loadedGroups) {
            count += group.rowCount;
        }
        active.hadState = count > 0;
        active.loadedCount = count;
        active.rows.reserve(static_cast<size_t>(count));
        int rank = 1;
        for (int groupIndex = 0; groupIndex < static_cast<int>(active.loadedGroups.size()); ++groupIndex) {
            LoadedGroup& group = active.loadedGroups[static_cast<size_t>(groupIndex)];
            for (int indexInGroup = 0; indexInGroup < group.rowCount; ++indexInGroup) {
                WorkItem item;
                item.sortNull = group.sortNull;
                item.sortValue = group.sortValue;
                item.groupIndex = groupIndex;
                item.indexInGroup = indexInGroup;
                item.stateRow = (*group.rows)[static_cast<size_t>(indexInGroup)];
                item.oldRank = rank++;
                active.rows.push_back(item);
            }
        }
    }

    void sortLoadedGroups(PartitionWork& active)
    {
        std::stable_sort(
            active.loadedGroups.begin(),
            active.loadedGroups.end(),
            [&](const LoadedGroup& left, const LoadedGroup& right) {
                return strictlyBetter(left.sortNull, left.sortValue, right.sortNull, right.sortValue);
            });
    }

    void loadPartition(PartitionWork& active)
    {
        emhash7::HashMap<RowData*, std::vector<RowData*>*>* encoded = dataState->entries();
        if (encoded == nullptr || encoded->empty()) {
            active.hadState = false;
            active.loadedCount = 0;
            return;
        }
        active.loadedGroups.reserve(encoded->size());
        for (const auto& entry : *encoded) {
            LoadedGroup group;
            group.sortKey = entry.first;
            group.rows = entry.second;
            if (group.sortKey == nullptr || group.rows == nullptr) {
                throw std::runtime_error("AppendOnlyTopNFunction data-state-with-append is malformed");
            }
            readSortKey(group.sortKey, group.sortNull, group.sortValue);
            group.rowCount = static_cast<int>(group.rows->size());
            active.loadedGroups.push_back(group);
        }
        sortLoadedGroups(active);
        fillFromGroups(active);
    }

    LoadedGroup* findLoaded(PartitionWork& active, bool sortNull, int64_t sortValue)
    {
        for (LoadedGroup& group : active.loadedGroups) {
            if (sameSortKey(group.sortNull, group.sortValue, sortNull, sortValue)) {
                return &group;
            }
        }
        return nullptr;
    }

    bool membershipChanged(const PartitionWork& active) const
    {
        if (!active.evicted.empty()) {
            return true;
        }
        for (const WorkItem& item : active.rows) {
            if (item.fromBatch) {
                return true;
            }
        }
        return false;
    }

    // RocksDB entries() returns copies. HashMap entries() returns the live map.
    void releaseLoadedCopies(PartitionWork& active, std::vector<RowData*>& deleteAfterOutput)
    {
        if (!rocksBackend()) {
            return;
        }
        for (LoadedGroup& group : active.loadedGroups) {
            if (group.rows != nullptr) {
                for (RowData* row : *group.rows) {
                    if (row != nullptr) {
                        deleteAfterOutput.push_back(row);
                    }
                }
                delete group.rows;
                group.rows = nullptr;
            }
            // The entries cache owns these sort keys and deletes them when the cache is cleared.
            group.sortKey = nullptr;
        }
    }

    void writeFlinkState(omnistream::VectorBatch* batch, PartitionWork& active)
    {
        struct FinalGroup {
            bool sortNull = false;
            int64_t sortValue = 0;
            RowData* sortKey = nullptr;
            bool keyCreated = false;
            bool changed = false;
            LoadedGroup* previous = nullptr;
            std::vector<RowData*>* list = nullptr;
            std::vector<RowData*> extracted;
        };

        std::vector<FinalGroup> groups;
        for (const WorkItem& item : active.rows) {
            if (groups.empty() ||
                !sameSortKey(groups.back().sortNull, groups.back().sortValue, item.sortNull, item.sortValue)) {
                FinalGroup group;
                group.sortNull = item.sortNull;
                group.sortValue = item.sortValue;
                group.previous = findLoaded(active, item.sortNull, item.sortValue);
                group.sortKey = group.previous == nullptr ? nullptr : group.previous->sortKey;
                group.list = new std::vector<RowData*>();
                groups.push_back(std::move(group));
            }
            FinalGroup& group = groups.back();
            if (item.fromBatch) {
                RowData* materialized = batch->extractRowData(item.batchRow);
                group.list->push_back(materialized);
                group.extracted.push_back(materialized);
                group.changed = true;
            } else {
                group.list->push_back(item.stateRow);
            }
        }

        for (FinalGroup& group : groups) {
            if (group.previous != nullptr) {
                group.previous->kept = true;
                if (group.previous->rows == nullptr || group.list->size() != group.previous->rows->size()) {
                    group.changed = true;
                }
            }
            if (!group.changed) {
                delete group.list;
                group.list = nullptr;
                continue;
            }
            if (group.sortKey == nullptr) {
                group.sortKey = makeSortKey(group.sortNull, group.sortValue);
                group.keyCreated = true;
            }
            dataState->put(group.sortKey, group.list);
            if (rocksBackend()) {
                for (RowData* row : group.extracted) {
                    delete row;
                }
                delete group.list;
                group.list = nullptr;
                if (group.keyCreated) {
                    delete group.sortKey;
                    group.sortKey = nullptr;
                }
            } else if (
                group.previous != nullptr && group.previous->rows != nullptr && group.previous->rows != group.list) {
                delete group.previous->rows;
                group.previous->rows = nullptr;
            }
        }

        for (LoadedGroup& previous : active.loadedGroups) {
            if (previous.kept) {
                continue;
            }
            dataState->remove(previous.sortKey);
            if (!rocksBackend()) {
                delete previous.rows;
                previous.rows = nullptr;
                delete previous.sortKey;
                previous.sortKey = nullptr;
            }
        }
    }

    void considerRow(omnistream::VectorBatch* batch, int row, PartitionWork& active)
    {
        WorkItem item;
        item.fromBatch = true;
        item.batchRow = row;
        item.timestamp = batch->getTimestamp(row);
        readBatchSort(batch, row, item.sortNull, item.sortValue);

        const int limit = static_cast<int>(this->rankEnd);
        if (static_cast<int>(active.rows.size()) >= limit) {
            const WorkItem& worst = active.rows.back();
            if (!strictlyBetter(item.sortNull, item.sortValue, worst.sortNull, worst.sortValue)) {
                return;
            }
        }

        auto pos = active.rows.begin();
        while (
            pos != active.rows.end() && !strictlyBetter(item.sortNull, item.sortValue, pos->sortNull, pos->sortValue)) {
            ++pos;
        }
        active.rows.insert(pos, item);
        if (static_cast<int>(active.rows.size()) > limit) {
            WorkItem dropped = active.rows.back();
            active.rows.pop_back();
            if (!dropped.fromBatch) {
                active.evicted.push_back(dropped);
            }
        }
    }

    void flushPartition(
        omnistream::VectorBatch* batch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        const std::vector<K>& touched,
        std::unordered_map<K, PartitionWork, PartitionKeyHash, PartitionKeyEq>& partitionWork,
        std::vector<OutRow>& outputs,
        std::vector<RowData*>& deleteAfterOutput)
    {
        for (const K& partitionKey : touched) {
            flushOnePartition(batch, ctx, partitionWork.find(partitionKey)->second, outputs, deleteAfterOutput);
        }
    }

    void flushOnePartition(
        omnistream::VectorBatch* batch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        PartitionWork& active,
        std::vector<OutRow>& outputs,
        std::vector<RowData*>& deleteAfterOutput)
    {
        ctx.setCurrentKey(active.key);
        if (!membershipChanged(active)) {
            releaseLoadedCopies(active, deleteAfterOutput);
            active.rows.clear();
            active.evicted.clear();
            active.loadedGroups.clear();
            active.hadState = false;
            active.loadedCount = 0;
            active.open = false;
            return;
        }

        for (const WorkItem& dropped : active.evicted) {
            if (dropped.oldRank >= this->rankStart && dropped.oldRank <= this->rankEnd) {
                appendRetract(outputs, dropped);
            }
        }

        for (size_t i = 0; i < active.rows.size(); ++i) {
            int rank = static_cast<int>(i) + 1;
            const WorkItem& item = active.rows[i];
            bool nowVisible = rank >= this->rankStart && rank <= this->rankEnd;
            bool wasVisible = item.oldRank >= this->rankStart && item.oldRank <= this->rankEnd;
            if (item.fromBatch) {
                if (nowVisible) {
                    appendEmit(outputs, batch, item, rank, RowKind::INSERT);
                }
                continue;
            }
            if (wasVisible && nowVisible && item.oldRank != rank) {
                appendRetract(outputs, item);
                RowKind kind = this->generateUpdateBefore ? RowKind::UPDATE_AFTER : RowKind::INSERT;
                appendEmit(outputs, batch, item, rank, kind);
            } else if (wasVisible && !nowVisible) {
                appendRetract(outputs, item);
            } else if (!wasVisible && nowVisible) {
                appendEmit(outputs, batch, item, rank, RowKind::INSERT);
            }
        }

        writeFlinkState(batch, active);
        if (rocksBackend()) {
            releaseLoadedCopies(active, deleteAfterOutput);
        } else {
            for (const WorkItem& dropped : active.evicted) {
                if (dropped.stateRow != nullptr) {
                    deleteAfterOutput.push_back(dropped.stateRow);
                }
            }
        }

        active.rows.clear();
        active.evicted.clear();
        active.loadedGroups.clear();
        active.hadState = false;
        active.loadedCount = 0;
        active.open = false;
    }

    void appendRetract(std::vector<OutRow>& outputs, const WorkItem& item)
    {
        OutRow row;
        row.stateRow = item.stateRow;
        row.rank = item.oldRank;
        row.timestamp = item.timestamp;
        row.kind = this->generateUpdateBefore ? RowKind::UPDATE_BEFORE : RowKind::DELETE;
        outputs.push_back(row);
    }

    void appendEmit(
        std::vector<OutRow>& outputs, omnistream::VectorBatch* batch, const WorkItem& item, int rank, RowKind kind)
    {
        OutRow row;
        if (item.fromBatch) {
            row.batch = batch;
            row.batchRow = item.batchRow;
            row.timestamp = item.timestamp;
        } else {
            row.stateRow = item.stateRow;
            row.timestamp = item.timestamp;
        }
        row.rank = rank;
        row.kind = kind;
        outputs.push_back(row);
    }

    omnistream::VectorBatch* buildOutput(const std::vector<OutRow>& outputs)
    {
        int numRows = static_cast<int>(outputs.size());
        int numColumns = static_cast<int>(this->inputRowType->size());
        auto* outputBatch = new omnistream::VectorBatch(numRows);
        for (int col = 0; col < numColumns; ++col) {
            outputBatch->Append(copyColumn(col, outputs));
        }
        if (this->outputRankNumber) {
            auto* rankVec = new omniruntime::vec::Vector<int64_t>(numRows);
            for (int i = 0; i < numRows; ++i) {
                rankVec->SetValue(i, outputs[i].rank);
            }
            outputBatch->Append(rankVec);
        }
        std::vector<int64_t> timestamps(numRows);
        std::vector<RowKind> kinds(numRows);
        for (int i = 0; i < numRows; ++i) {
            timestamps[i] = outputs[i].timestamp;
            kinds[i] = outputs[i].kind;
        }
        outputBatch->setTimestamps(0, timestamps.data(), numRows);
        outputBatch->setRowKinds(0, kinds.data(), numRows);
        return outputBatch;
    }

    omniruntime::vec::BaseVector* copyColumn(int col, const std::vector<OutRow>& outputs)
    {
        int numRows = static_cast<int>(outputs.size());
        switch (this->inputRowType->at(col)) {
            case DataTypeId::OMNI_LONG:
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: return copyLongColumn(col, outputs, numRows);
            case DataTypeId::OMNI_INT: return copyIntColumn(col, outputs, numRows);
            case DataTypeId::OMNI_BOOLEAN: return copyBoolColumn(col, outputs, numRows);
            case DataTypeId::OMNI_VARCHAR: return copyVarcharColumn(col, outputs, numRows);
            default: throw std::runtime_error("Unsupported column type in AppendOnlyTopNFunction");
        }
    }

    omniruntime::vec::Vector<int64_t>* copyLongColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        auto* vector = new omniruntime::vec::Vector<int64_t>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, *outputs[i].stateRow->getLong(col));
            }
        }
        return vector;
    }

    omniruntime::vec::Vector<int32_t>* copyIntColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        auto* vector = new omniruntime::vec::Vector<int32_t>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<int32_t>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, *outputs[i].stateRow->getInt(col));
            }
        }
        return vector;
    }

    omniruntime::vec::Vector<bool>* copyBoolColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        auto* vector = new omniruntime::vec::Vector<bool>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<bool>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, *outputs[i].stateRow->getBool(col));
            }
        }
        return vector;
    }

    omniruntime::vec::BaseVector* copyVarcharColumn(int col, const std::vector<OutRow>& outputs, int numRows)
    {
        using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
        auto* vector = new VarcharVector(numRows);
        for (int i = 0; i < numRows; ++i) {
            std::string value;
            bool isNull = false;
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<VarcharVector*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    isNull = true;
                } else {
                    value = std::string(src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                isNull = true;
            } else {
                value = std::string(outputs[i].stateRow->getStringView(col));
            }
            if (isNull) {
                vector->SetNull(i);
            } else {
                omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(vector, i, (void*)&value);
            }
        }
        return vector;
    }
};

#endif // OMNISTREAM_APPENDONLYTOPNFUNCTION_H
