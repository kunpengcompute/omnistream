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
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "AbstractTopNFunction.h"
#include "AppendOnlyTopNHelper.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/HeapMapState.h"
#include "runtime/state/rocksdb/RocksdbMapState.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/types/logical/RowType.h"
#include "core/typeutils/ListSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

// Persist complete rows in the existing MapState<sortKey, row list>. Keep only
// owned, ordered sort-key/count metadata across batches. Old rows are fetched
// on demand once per group per batch. flushPartition calls updateOnePartition
// and commits this input batch before publishing its metadata or emitting it.
template <typename K>
class AppendOnlyTopNFunction : public AbstractTopNFunction<K> {
public:
    // 根据 rank 配置初始化 AppendOnly TopN 算子。
    explicit AppendOnlyTopNFunction(
        const nlohmann::json& rankConfig, bool metadataCache = true, bool batchWrites = true)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
          cacheRequested_(metadataCache),
          batchWrites_(batchWrites)
    {
        if (this->sortKeyTypeIds.size() != 1 || this->sortKeyTypeIds[0] != OMNI_LONG || this->sortOrder[0]) {
            NOT_IMPL_EXCEPTION;
        }
    }

    // 释放本算子持有的 metadata 缓存。
    ~AppendOnlyTopNFunction() override
    {
        releaseMetadata();
    }

    // 关闭算子并释放本地缓存及父类资源。
    void close() override
    {
        releaseMetadata();
        AbstractTopNFunction<K>::close();
    }

    // 初始化序列化器，并与状态后端交互获取 MapState。
    void open(const Configuration& context) override
    {
        releaseMetadata();
        cacheEnabled_ = cacheRequested_ && this->isConstantRankEnd && this->rankEnd > 0 &&
                        this->partitionKeyTypeIds.size() == 1 && this->partitionKeyTypeIds[0] == OMNI_LONG;
        // 构造 MapState key（sort key）的 RowType 与序列化器。
        // 当前仅支持单个排序字段。
        std::vector<std::string> sortTypeNames{(*this->inputTypeNames)[this->sortKeyIndices[0]]};
        omnistream::RowType sortKeyType(false, sortTypeNames);
        auto* sortKeySer = new RowDataSerializer(&sortKeyType);

        // 构造 MapState value（相同 sort key 下的一组输入行）的序列化器。
        omnistream::RowType inputType(false, *this->inputTypeNames);
        auto* rowListSerializer = new ListSerializer(new RowDataSerializer(&inputType));

        auto* desc = new MapStateDescriptor<RowData*, std::vector<RowData*>*>(
            "data-state-with-append", sortKeySer, rowListSerializer);
        desc->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_LIST_BK);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        dataState = runtimeContext->template getMapState<RowData*, std::vector<RowData*>*>(desc);
        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "AppendOnlyTopNFunction");

        knownEmptyStart_ = runtimeContext->startsWithEmptyKeyedState(); // for quick path
        if ((rocksBackend() && dynamic_cast<RocksState*>(dataState) == nullptr) ||
            (!rocksBackend() && dynamic_cast<HeapState*>(dataState) == nullptr)) {
            throw std::runtime_error("AppendOnlyTopNFunction unsupported state implementation");
        }
    }

    // 按分区处理一个输入 batch，并输出 TopN 变化。
    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override
    {
        auto inputBatchGuard = std::unique_ptr<omnistream::VectorBatch>(inputBatch);
        this->initRankEnd(nullptr);
        auto* batch = inputBatchGuard.get();
        // 当前 batch 中待输出的变更行。
        std::vector<AppendOnlyTopNOutputRow> outputs;
        // 当前 batch 结束或异常时需要释放的临时资源。
        BatchCleanup cleanup;
        // 按分区聚合当前 batch 的 TopN 计算中间结果。
        std::unordered_map<K, PartitionWork, PartitionKeyHash, PartitionKeyEq> partitionWork;
        // 保持分区在当前 batch 中首次出现的顺序，供后续逐分区 flush。
        std::vector<K> touched;
        try {
            for (int i = 0; i < batch->GetRowCount(); ++i) {
                K key = Helper::retainKey(partitionKeySelector.getKey(batch, i), cleanup.retainedKeys);
                auto [it, created] = partitionWork.try_emplace(key);
                if (created) {
                    ctx.setCurrentKey(key);
                    it->second.key = key;
                    loadPartition(it->second);
                    touched.push_back(key);
                }
                considerRow(batch, i, it->second);
            }
            rocksdb::WriteBatch writeBatch;
            auto* pending = rocksBackend() && batchWrites_ ? &writeBatch : nullptr;
            for (const K& key : touched) {
                updateOnePartition(batch, ctx, partitionWork.find(key)->second, outputs, cleanup.rows, pending);
            }
            if (pending != nullptr && pending->Count() != 0) {
                static_cast<RocksState*>(dataState)->commitBatch(*pending);
            }
            for (const K& key : touched) {
                Helper::publishMetadata(metadata_, partitionWork.find(key)->second);
            }
            dataState->clearEntriesCache();
            if (!outputs.empty()) {
                this->collectOutputBatch(
                    out, OutputHelper::build(outputs, *this->inputRowType, this->outputRankNumber));
            }
        } catch (...) {
            // Neither a failed write nor a partially processed input batch is
            // a valid source of metadata for another processBatch invocation.
            releaseMetadata();
            knownEmptyStart_ = false;
            dataState->clearEntriesCache();
            throw;
        }
    }

    // AppendOnly TopN 不使用 ValueState。
    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    using Helper = AppendOnlyTopNHelper<K>;
    using RowList = typename Helper::RowList;
    using RocksState = typename Helper::RocksState;
    using HeapState = typename Helper::HeapState;
    using GroupMeta = typename Helper::GroupMeta;
    using PartitionMeta = typename Helper::PartitionMeta;
    using WorkItem = typename Helper::WorkItem;
    using LoadedGroup = typename Helper::LoadedGroup;
    using PartitionWork = typename Helper::PartitionWork;
    using BatchCleanup = typename Helper::BatchCleanup;
    using PartitionKeyHash = typename Helper::PartitionKeyHash;
    using PartitionKeyEq = typename Helper::PartitionKeyEq;
    MapState<RowData*, RowList*>* dataState = nullptr;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
    KeySelector<K> partitionKeySelector;
    std::unordered_map<int64_t, PartitionMeta> metadata_;
    bool cacheRequested_;
    bool batchWrites_;
    bool cacheEnabled_ = false;
    bool knownEmptyStart_ = false;

    // 释放跨 batch 的 metadata 缓存容量。
    void releaseMetadata()
    {
        // clear() alone retains bucket storage. Swap releases that as well.
        decltype(metadata_){}.swap(metadata_);
    }
    // 判断当前状态后端是否为 RocksDB。
    bool rocksBackend() const
    {
        return stateType_ == omnistream::StateType::ROCKSDB;
    }

    // 与状态后端交互加载一个分区的排序组及缓存摘要。
    void loadPartition(PartitionWork& active)
    {
        active.cacheable = Helper::cacheKey(cacheEnabled_, active.key, active.cacheId);
        if (active.cacheable) {
            auto found = metadata_.find(active.cacheId);
            if (found != metadata_.end()) {
                active.loadedGroups.reserve(found->second.groups.size());
                for (const auto& meta : found->second.groups) {
                    LoadedGroup group;
                    group.sortValue = meta.sortValue;
                    group.sortNull = meta.sortNull;
                    group.rowCount = meta.rowCount;
                    active.loadedGroups.push_back(std::move(group));
                }
                Helper::fillFromGroups(active, this->rankEnd);
                return;
            }
            if (knownEmptyStart_) {
                return;
            }
        }
        auto* encoded = dataState->entries();
        if (encoded == nullptr) {
            return;
        }
        active.loadedGroups.reserve(encoded->size());
        for (const auto& entry : *encoded) {
            if (entry.first == nullptr || entry.second == nullptr) {
                throw std::runtime_error("AppendOnlyTopNFunction malformed state group");
            }
            LoadedGroup group;
            group.sortKey = entry.first;
            group.rows = entry.second;
            group.rowCount = static_cast<int>(group.rows->size());
            Helper::readSortKey(group.sortKey, group.sortNull, group.sortValue);
            if (rocksBackend()) {
                // entriesCache's emhash owns its pointer keys; the values
                // are independent row-list copies owned by this batch.
                group.ownedRows.reset(group.rows);
            }
            active.loadedGroups.push_back(std::move(group));
        }
        std::stable_sort(
            active.loadedGroups.begin(), active.loadedGroups.end(), [&](const LoadedGroup& a, const LoadedGroup& b) {
                return Helper::strictlyBetter(
                    Helper::nullsLast(this->sortNullsIsLast), a.sortNull, a.sortValue, b.sortNull, b.sortValue);
            });
        Helper::fillFromGroups(active, this->rankEnd);
    }

    // 将当前输入行按排序规则纳入该分区的 TopN 候选集。
    void considerRow(omnistream::VectorBatch* batch, int row, PartitionWork& active)
    {
        WorkItem item;
        item.fromBatch = true;
        item.batchRow = row;
        item.timestamp = batch->getTimestamp(row);
        Helper::readBatchSort(batch, this->sortKeyIndices[0], row, item.sortNull, item.sortValue);
        const int limit = static_cast<int>(this->rankEnd);
        if (static_cast<int>(active.rows.size()) >= limit) {
            const auto& worst = active.rows.back();
            if (!Helper::strictlyBetter(
                    Helper::nullsLast(this->sortNullsIsLast),
                    item.sortNull,
                    item.sortValue,
                    worst.sortNull,
                    worst.sortValue)) {
                return;
            }
        }
        auto pos = active.rows.begin();
        while (pos != active.rows.end() && !Helper::strictlyBetter(
                                               Helper::nullsLast(this->sortNullsIsLast),
                                               item.sortNull,
                                               item.sortValue,
                                               pos->sortNull,
                                               pos->sortValue)) {
            ++pos;
        }
        active.rows.insert(pos, item);
        if (static_cast<int>(active.rows.size()) > limit) {
            auto dropped = active.rows.back();
            active.rows.pop_back();
            if (!dropped.fromBatch) {
                active.evicted.push_back(dropped);
            }
        }
    }

    // 与状态后端交互写入更新后的排序组，并处理 HeapState 行所有权。
    void writeFlinkState(
        omnistream::VectorBatch* batch,
        PartitionWork& active,
        rocksdb::WriteBatch* writeBatch,
        std::vector<RowData*>& deleteAfterOutput)
    {
        size_t first = 0;
        while (first < active.rows.size()) {
            const auto& front = active.rows[first];
            size_t end = first + 1;
            int oldKept = front.fromBatch ? 0 : 1;
            int newKept = front.fromBatch ? 1 : 0;
            while (end < active.rows.size() &&
                   Helper::sameSortKey(
                       front.sortNull, front.sortValue, active.rows[end].sortNull, active.rows[end].sortValue)) {
                oldKept += active.rows[end].fromBatch ? 0 : 1;
                newKept += active.rows[end].fromBatch ? 1 : 0;
                ++end;
            }
            auto* previous = Helper::findLoaded(active, front.sortNull, front.sortValue);
            if (previous != nullptr) {
                previous->kept = true;
            }
            const bool changed = newKept > 0 || (previous != nullptr && oldKept != previous->rowCount);
            if (!changed) {
                first = end;
                continue;
            }
            if (previous != nullptr) {
                previous->changed = true;
                if (oldKept > 0 || !rocksBackend()) {
                    Helper::ensureLoaded(dataState, rocksBackend(), *previous);
                }
            }
            auto list = std::make_unique<RowList>();
            list->reserve(end - first);
            std::vector<std::unique_ptr<RowData>> extracted;
            extracted.reserve(newKept);
            for (size_t i = first; i < end; ++i) {
                auto& item = active.rows[i];
                if (item.fromBatch) {
                    extracted.emplace_back(batch->extractRowData(item.batchRow));
                    list->push_back(extracted.back().get());
                } else {
                    list->push_back(previous->rows->at(item.indexInGroup));
                }
            }
            std::unique_ptr<RowData> key;
            RowData* sortKey = previous == nullptr ? nullptr : previous->sortKey;
            if (sortKey == nullptr) {
                key.reset(Helper::makeSortKey(front.sortNull, front.sortValue));
                sortKey = key.get();
            }
            if (writeBatch != nullptr) {
                static_cast<RocksState*>(dataState)->appendPut(*writeBatch, sortKey, list.get());
            } else {
                dataState->put(sortKey, list.get());
            }
            if (!rocksBackend()) {
                for (auto& row : extracted) {
                    row.release();
                }
                list.release();
                if (previous == nullptr) {
                    key.release();
                } else {
                    delete previous->rows;
                    previous->rows = nullptr;
                }
            }
            first = end;
        }
        for (auto& previous : active.loadedGroups) {
            if (!previous.kept) {
                previous.changed = true;
                if (!rocksBackend()) {
                    Helper::ensureLoaded(dataState, rocksBackend(), previous);
                }
                std::unique_ptr<RowData> key;
                RowData* sortKey = previous.sortKey;
                if (sortKey == nullptr) {
                    key.reset(Helper::makeSortKey(previous.sortNull, previous.sortValue));
                    sortKey = key.get();
                }
                if (writeBatch != nullptr) {
                    static_cast<RocksState*>(dataState)->appendRemove(*writeBatch, sortKey);
                } else {
                    dataState->remove(sortKey);
                }
                if (!rocksBackend()) {
                    delete previous.rows;
                    previous.rows = nullptr;
                    delete previous.sortKey;
                    previous.sortKey = nullptr;
                }
            }
        }
        if (!rocksBackend()) {
            // Surviving old rows belong to the new backend lists. Only evicted
            // old rows are freed, after the output has copied their contents.
            for (auto& dropped : active.evicted) {
                if (dropped.stateRow == nullptr) {
                    // A rank outside the output range can still be evicted.
                    // Load before replacing/removing its old list instead.
                    throw std::runtime_error("AppendOnlyTopNFunction unresolved evicted heap row");
                }
                deleteAfterOutput.push_back(dropped.stateRow);
            }
        }
    }

    // 计算一个分区的增删改输出，并将其新 TopN 写入状态后端。
    void updateOnePartition(
        omnistream::VectorBatch* batch,
        typename KeyedProcessFunction<K, RowData*, RowData*>::Context& ctx,
        PartitionWork& active,
        std::vector<AppendOnlyTopNOutputRow>& outputs,
        std::vector<RowData*>& deleteAfterOutput,
        rocksdb::WriteBatch* writeBatch)
    {
        ctx.setCurrentKey(active.key);
        Helper::snapshotMetadata(active);
        if (!Helper::membershipChanged(active)) {
            return;
        }
        for (auto& dropped : active.evicted) {
            if (!rocksBackend() || (dropped.oldRank >= this->rankStart && dropped.oldRank <= this->rankEnd)) {
                Helper::resolveRow(dataState, rocksBackend(), active, dropped);
            }
            if (dropped.oldRank >= this->rankStart && dropped.oldRank <= this->rankEnd) {
                OutputHelper::appendRetract(outputs, dropped, this->generateUpdateBefore);
            }
        }
        for (size_t i = 0; i < active.rows.size(); ++i) {
            int rank = static_cast<int>(i) + 1;
            auto& item = active.rows[i];
            bool nowVisible = rank >= this->rankStart && rank <= this->rankEnd;
            bool wasVisible = item.oldRank >= this->rankStart && item.oldRank <= this->rankEnd;
            if (item.fromBatch) {
                if (nowVisible) {
                    OutputHelper::appendEmit(outputs, batch, item, rank, RowKind::INSERT);
                }
            } else if (wasVisible && nowVisible && item.oldRank != rank) {
                Helper::resolveRow(dataState, rocksBackend(), active, item);
                OutputHelper::appendRetract(outputs, item, this->generateUpdateBefore);
                RowKind kind = this->generateUpdateBefore ? RowKind::UPDATE_AFTER : RowKind::INSERT;
                OutputHelper::appendEmit(outputs, batch, item, rank, kind);
            } else if (wasVisible && !nowVisible) {
                Helper::resolveRow(dataState, rocksBackend(), active, item);
                OutputHelper::appendRetract(outputs, item, this->generateUpdateBefore);
            } else if (!wasVisible && nowVisible) {
                Helper::resolveRow(dataState, rocksBackend(), active, item);
                OutputHelper::appendEmit(outputs, batch, item, rank, RowKind::INSERT);
            }
        }
        writeFlinkState(batch, active, writeBatch, deleteAfterOutput);
    }
};

#endif // OMNISTREAM_APPENDONLYTOPNFUNCTION_H
