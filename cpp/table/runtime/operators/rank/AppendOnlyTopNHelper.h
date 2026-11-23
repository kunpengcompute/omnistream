/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */

#ifndef OMNISTREAM_APPENDONLYTOPNHELPER_H
#define OMNISTREAM_APPENDONLYTOPNHELPER_H

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "core/api/common/state/MapState.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/HeapMapState.h"
#include "runtime/state/rocksdb/RocksdbMapState.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

template <typename K>
struct AppendOnlyTopNHelper {
    using RowList = std::vector<RowData*>;
    using RocksState = RocksdbMapState<K, VoidNamespace, RowData*, RowList*>;
    using HeapState = HeapMapState<K, VoidNamespace, RowData*, RowList*>;

    struct GroupMeta {
        int64_t sortValue = 0;
        int64_t rowCount = 0;
        bool sortNull = false;
    };

    struct PartitionMeta {
        std::vector<GroupMeta> groups;
    };

    struct WorkItem {
        bool sortNull = false;
        int64_t sortValue = 0;
        bool fromBatch = false;
        int32_t batchRow = -1;
        int64_t groupIndex = -1;
        int64_t indexInGroup = -1;
        RowData* stateRow = nullptr;
        int64_t oldRank = 0;
        int64_t timestamp = 0;
    };

    struct RowListDeleter {
        // 释放 RocksDB 解码出的行列表及其行对象。
        void operator()(RowList* rows) const
        {
            if (rows != nullptr) {
                for (auto* row : *rows) {
                    delete row;
                }
                delete rows;
            }
        }
    };

    struct LoadedGroup {
        bool sortNull = false;
        int64_t sortValue = 0;
        RowData* sortKey = nullptr;
        RowList* rows = nullptr;
        int64_t rowCount = 0;
        bool kept = false;
        bool changed = false;
        bool fetchedForOutput = false;
        std::unique_ptr<RowData> ownedKey;
        std::unique_ptr<RowList, RowListDeleter> ownedRows;
    };

    struct PartitionWork {
        K key{};
        bool cacheable = false;
        int64_t cacheId = 0;
        std::vector<WorkItem> rows;
        std::vector<WorkItem> evicted;
        std::vector<LoadedGroup> loadedGroups;
        std::vector<GroupMeta> pending;
    };

    struct PartitionKeyHash {
        size_t operator()(const K& key) const
        {
            if constexpr (std::is_same<K, RowData*>::value) {
                return key == nullptr ? 0 : static_cast<size_t>(key->hashCodeFast());
            } else {
                return std::hash<K>{}(key);
            }
        }
    };

    struct PartitionKeyEq {
        bool operator()(const K& left, const K& right) const
        {
            return samePartition(left, right);
        }
    };

    struct BatchCleanup {
        std::unordered_map<K, K, PartitionKeyHash, PartitionKeyEq> retainedKeys;
        std::vector<RowData*> rows;
        ~BatchCleanup()
        {
            for (auto* row : rows) {
                delete row;
            }
            if constexpr (std::is_same<K, RowData*>::value) {
                for (const auto& entry : retainedKeys) {
                    delete entry.second;
                }
            }
        }
    };

    // 将可缓存的分区 key 转换为 metadata 的 int64 标识。
    static bool cacheKey(bool cacheEnabled, const K& key, int64_t& id)
    {
        if (!cacheEnabled) {
            return false;
        }
        if constexpr (std::is_same<K, RowData*>::value) {
            if (key == nullptr || key->isNullAt(0)) {
                return false;
            }
            id = *key->getLong(0);
            return true;
        } else if constexpr (std::is_integral<K>::value) {
            id = static_cast<int64_t>(key);
            return true;
        }
        return false;
    }

    // 保留 batch 内唯一的 RowData 分区 key 并释放重复副本。
    static K retainKey(K key, std::unordered_map<K, K, PartitionKeyHash, PartitionKeyEq>& retainedKeys)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            if (key == nullptr) {
                throw std::invalid_argument("TopN partition key row is missing");
            }
            auto found = retainedKeys.find(key);
            if (found != retainedKeys.end()) {
                delete key;
                return found->second;
            }
            retainedKeys.emplace(key, key);
        }
        return key;
    }

    // 判断候选排序值是否严格优于另一个排序值。
    static bool strictlyBetter(bool nullsLast, bool candNull, int64_t cand, bool otherNull, int64_t other)
    {
        if (candNull && otherNull) {
            return false;
        }
        if (candNull) {
            return !nullsLast;
        }
        if (otherNull) {
            return nullsLast;
        }
        return cand > other;
    }

    // 返回当前单排序字段的 null 排序方向。
    static bool nullsLast(const std::vector<bool>& sortNullsIsLast)
    {
        return sortNullsIsLast.empty() || sortNullsIsLast[0];
    }

    // 从输入 batch 读取单列 long 类型排序值。
    static void readBatchSort(omnistream::VectorBatch* batch, int sortKeyIndex, int row, bool& isNull, int64_t& value)
    {
        auto* column = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(sortKeyIndex));
        isNull = column->IsNull(row);
        value = isNull ? 0 : column->GetValue(row);
    }

    // 根据排序值创建用于状态查询或写入的二进制 key。
    static RowData* makeSortKey(bool isNull, int64_t value)
    {
        auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
        if (isNull) {
            row->setNullAt(0);
        } else {
            row->setLong(0, value);
        }
        return row;
    }

    // 从状态排序 key 中解析 null 标记和 long 值。
    static void readSortKey(RowData* key, bool& isNull, int64_t& value)
    {
        if (key == nullptr) {
            throw std::runtime_error("AppendOnlyTopNFunction sort key is missing");
        }
        isNull = key->isNullAt(0);
        value = isNull ? 0 : *key->getLong(0);
    }

    // 判断两个单列排序 key 是否相同。
    static bool sameSortKey(bool leftNull, int64_t left, bool rightNull, int64_t right)
    {
        return (leftNull || rightNull) ? leftNull == rightNull : left == right;
    }

    // 根据已加载的排序组展开 TopN 候选行引用。
    static void fillFromGroups(PartitionWork& active, int64_t rankEnd)
    {
        size_t count = 0;
        for (const auto& group : active.loadedGroups) {
            if (group.rowCount < 0 || static_cast<uint64_t>(group.rowCount) > active.rows.max_size() - count) {
                throw std::length_error("TopN state exceeds supported row count");
            }
            count += static_cast<size_t>(group.rowCount);
        }
        const size_t limit = static_cast<size_t>(rankEnd);
        // Reserve room for new rows without multiplying an unbounded rankEnd.
        const size_t reserveCount = count >= limit || count > limit - count ? limit : count * 2;
        active.rows.reserve(reserveCount);
        int64_t rank = 1;
        for (int64_t g = 0; g < static_cast<int64_t>(active.loadedGroups.size()); ++g) {
            const auto& group = active.loadedGroups[g];
            for (int64_t i = 0; i < group.rowCount; ++i) {
                active.rows.push_back({group.sortNull, group.sortValue, false, -1, g, i, nullptr, rank++, 0});
            }
        }
    }

    // 判断当前分区的 TopN 成员是否发生变化。
    static bool membershipChanged(const PartitionWork& active)
    {
        if (!active.evicted.empty()) {
            return true;
        }
        for (const auto& item : active.rows) {
            if (item.fromBatch) {
                return true;
            }
        }
        return false;
    }

    // 从当前 TopN 候选行生成待发布的排序组摘要。
    static void snapshotMetadata(PartitionWork& active)
    {
        active.pending.reserve(active.rows.size());
        for (const auto& item : active.rows) {
            if (active.pending.empty() ||
                !sameSortKey(
                    active.pending.back().sortNull, active.pending.back().sortValue, item.sortNull, item.sortValue)) {
                active.pending.push_back({item.sortValue, 0, item.sortNull});
            }
            ++active.pending.back().rowCount;
        }
    }

    // 将已成功写入状态的分区摘要发布到本地 metadata 缓存。
    static void publishMetadata(std::unordered_map<int64_t, PartitionMeta>& metadata, PartitionWork& active)
    {
        if (active.cacheable) {
            metadata[active.cacheId].groups.assign(active.pending.begin(), active.pending.end());
        }
    }

    // 在已加载排序组中查找指定排序 key。
    static LoadedGroup* findLoaded(PartitionWork& active, bool sortNull, int64_t sortValue)
    {
        for (auto& group : active.loadedGroups) {
            if (sameSortKey(group.sortNull, group.sortValue, sortNull, sortValue)) {
                return &group;
            }
        }
        return nullptr;
    }

    // 与状态后端交互按需读取一个排序组的完整行列表。
    static void ensureLoaded(MapState<RowData*, RowList*>* dataState, bool rocksBackend, LoadedGroup& group)
    {
        if (group.rows != nullptr) {
            return;
        }
        auto queryKey = std::unique_ptr<RowData>(makeSortKey(group.sortNull, group.sortValue));
        auto value = dataState->get(queryKey.get());
        if (!value.has_value() || *value == nullptr) {
            throw std::runtime_error("AppendOnlyTopNFunction metadata references missing state group");
        }
        group.rows = *value;
        if (rocksBackend) {
            group.ownedKey = std::move(queryKey);
            group.sortKey = group.ownedKey.get();
            group.ownedRows.reset(group.rows);
        } else {
            auto key = static_cast<HeapState*>(dataState)->getStoredKey(queryKey.get());
            if (!key.has_value()) {
                throw std::runtime_error("AppendOnlyTopNFunction heap group key is missing");
            }
            group.sortKey = *key;
        }
        if (group.rows->size() != static_cast<size_t>(group.rowCount)) {
            throw std::runtime_error("AppendOnlyTopNFunction metadata row count disagrees with state");
        }
    }

    // 与状态后端交互按需解析旧候选行的完整 RowData。
    static void resolveRow(
        MapState<RowData*, RowList*>* dataState, bool rocksBackend, PartitionWork& active, WorkItem& item)
    {
        if (item.fromBatch || item.stateRow != nullptr) {
            return;
        }
        auto& group = active.loadedGroups.at(item.groupIndex);
        ensureLoaded(dataState, rocksBackend, group);
        item.stateRow = group.rows->at(item.indexInGroup);
    }

private:
    // 判断两个分区 key 是否指向同一个逻辑分区。
    static bool samePartition(const K& left, const K& right)
    {
        if constexpr (std::is_same<K, RowData*>::value) {
            return left == right || (left != nullptr && right != nullptr && *left == *right);
        }
        return left == right;
    }
};

struct AppendOnlyTopNOutputRow {
    omnistream::VectorBatch* batch = nullptr;
    int32_t batchRow = -1;
    RowData* stateRow = nullptr;
    int64_t rank = 0;
    int64_t timestamp = 0;
    RowKind kind = RowKind::INSERT;
};

class OutputHelper {
public:
    // 将待输出行复制为下游可消费的 VectorBatch。
    static omnistream::VectorBatch* build(
        const std::vector<AppendOnlyTopNOutputRow>& outputs,
        const std::vector<omniruntime::type::DataTypeId>& inputTypes,
        bool outputRankNumber)
    {
        const int numRows = static_cast<int>(outputs.size());
        auto* outputBatch = new omnistream::VectorBatch(numRows);
        for (int col = 0; col < static_cast<int>(inputTypes.size()); ++col) {
            outputBatch->Append(copyColumn(col, inputTypes.at(col), outputs));
        }
        if (outputRankNumber) {
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

    template <typename WorkItem>
    // 追加一条旧行的撤回输出。
    static void appendRetract(
        std::vector<AppendOnlyTopNOutputRow>& outputs, const WorkItem& item, bool generateUpdateBefore)
    {
        outputs.push_back(
            {nullptr,
             -1,
             item.stateRow,
             item.oldRank,
             item.timestamp,
             generateUpdateBefore ? RowKind::UPDATE_BEFORE : RowKind::DELETE});
    }

    template <typename WorkItem>
    // 追加一条新行或更新后旧行的输出。
    static void appendEmit(
        std::vector<AppendOnlyTopNOutputRow>& outputs,
        omnistream::VectorBatch* batch,
        const WorkItem& item,
        int64_t rank,
        RowKind kind)
    {
        AppendOnlyTopNOutputRow row;
        row.batch = item.fromBatch ? batch : nullptr;
        row.batchRow = item.fromBatch ? item.batchRow : -1;
        row.stateRow = item.fromBatch ? nullptr : item.stateRow;
        row.rank = rank;
        row.timestamp = item.timestamp;
        row.kind = kind;
        outputs.push_back(row);
    }

private:
    // 按逻辑类型复制输出 batch 的一列。
    static omniruntime::vec::BaseVector* copyColumn(
        int col, omniruntime::type::DataTypeId type, const std::vector<AppendOnlyTopNOutputRow>& outputs)
    {
        const int numRows = static_cast<int>(outputs.size());
        switch (type) {
            case omniruntime::type::DataTypeId::OMNI_LONG:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
                return copyFixedColumn<int64_t>(
                    col, outputs, numRows, [](RowData* row, int index) { return *row->getLong(index); });
            case omniruntime::type::DataTypeId::OMNI_INT:
                return copyFixedColumn<int32_t>(
                    col, outputs, numRows, [](RowData* row, int index) { return *row->getInt(index); });
            case omniruntime::type::DataTypeId::OMNI_BOOLEAN:
                return copyFixedColumn<bool>(
                    col, outputs, numRows, [](RowData* row, int index) { return *row->getBool(index); });
            case omniruntime::type::DataTypeId::OMNI_VARCHAR: return copyVarcharColumn(col, outputs, numRows);
            default: throw std::runtime_error("Unsupported column type in AppendOnlyTopNFunction");
        }
    }

    template <typename T, typename StateGetter>
    // 复制定长列，并兼容输入 batch 行和状态行两种来源。
    static omniruntime::vec::Vector<T>* copyFixedColumn(
        int col, const std::vector<AppendOnlyTopNOutputRow>& outputs, int numRows, StateGetter readState)
    {
        auto* vector = new omniruntime::vec::Vector<T>(numRows);
        for (int i = 0; i < numRows; ++i) {
            if (outputs[i].batch != nullptr) {
                auto* src = reinterpret_cast<omniruntime::vec::Vector<T>*>(outputs[i].batch->Get(col));
                if (src->IsNull(outputs[i].batchRow)) {
                    vector->SetNull(i);
                } else {
                    vector->SetValue(i, src->GetValue(outputs[i].batchRow));
                }
            } else if (outputs[i].stateRow->isNullAt(col)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, readState(outputs[i].stateRow, col));
            }
        }
        return vector;
    }

    // 复制 varchar 列，并将状态字符串转为输出向量值。
    static omniruntime::vec::BaseVector* copyVarcharColumn(
        int col, const std::vector<AppendOnlyTopNOutputRow>& outputs, int numRows)
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
                omniruntime::vec::VectorHelper::VectorSetValue<omniruntime::type::DataTypeId::OMNI_VARCHAR>(
                    vector, i, (void*)&value);
            }
        }
        return vector;
    }
};

#endif // OMNISTREAM_APPENDONLYTOPNHELPER_H
