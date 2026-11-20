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

#ifndef OMNISTREAM_UPDATABLETOPNFUNCTION_H
#define OMNISTREAM_UPDATABLETOPNFUNCTION_H

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "AbstractTopNFunction.h"
#include "UpdatableTopNBuffer.h"
#include "SortedKVCache.h"
#include "core/api/common/state/MapState.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/include/common.h"
#include "core/include/outputbuffer.h"
#include "core/memory/DataInputDeserializer.h"
#include "core/memory/DataOutputSerializer.h"
#include "runtime/state/VoidNamespace.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/types/logical/RowType.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/RowDataIntPairSerializer.h"
#include "table/typeutils/RowDataSerializer.h"

// Native port of org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction.
// Selected by the planner when a Rank uses the UpdateFastStrategy (append-only input,
// unique key contains the partition key, sort monotonicity holds). It is the fast
// variant which keeps only the top-N rows per partition: the persisted state is
// Flink's "data-state-with-update" MapState<rowKey RowData, Tuple2<RowData, Integer>>
// (rowKey -> record + inner rank within the same sort key), the value wire format
// being byte-identical to Flink's TupleSerializer<Tuple2<RowData,Integer>>.
//
// Per partition the operator keeps an in-memory TopNBuffer (sort key -> row-key
// bytes in insertion order) plus a rowKeyMap (serialized row-key bytes -> RankRow).
// Partition working sets live in an LRU cache (SortedKVCache<std::string,
// PartitionCacheEntry*>) keyed by the serialized partition-key bytes; on eviction
// the dirty rows are flushed to state exactly like Flink's CacheRemovalListener.
// A batch flushes every touched partition at batch end and frees all batch
// temporaries.
//
// Ownership summary:
//  - RankRow has no destructor (aggregate). RankRow.row is always owned by the row:
//    deleted together with the struct by deleteRankRow(). stateKey is NOT owned:
//    on HEAP it is the live map's actual key pointer (map owns it; the operator
//    deletes it explicitly on remove), on RocksDB it is always nullptr.
//  - HEAP state maps identify RowData* by CONTENT, not by address: std::hash<RowData*>
//    hashes the bytes (hashCodeFast) and std::equal_to<RowData*> compares the bytes
//    (RowData.h / BinaryRowData.h), so a key re-deserialized from bytes always matches
//    the stored key. State tables in this project never key by pointer identity.
//  - emhash7::HashMap<RowData*, RowDataIntPair*> never releases keys on erase and
//    never releases values; its destructor only deletes remaining keys. The operator
//    therefore frees HEAP state keys/values when removing entries itself.
//  - RocksDB restore reads the backend through MapState::iteratorV2() (a fresh
//    RocksDB scan per restore, mirroring Flink's dataState.iterator()). The row-key
//    bytes are recomputed from the record through rowKeySelector (the iterator's
//    deserialized key may be a shared reusable buffer), the pair's row is moved into
//    the RankRow, and the pair struct is freed by the iterator entry. The operator
//    never holds a pointer into RocksdbMapStateTable::entriesCache.
template <typename K>
class UpdatableTopNFunction : public AbstractTopNFunction<K> {
public:
    explicit UpdatableTopNFunction(const nlohmann::json& rankConfig)
        : AbstractTopNFunction<K>(rankConfig),
          partitionKeySelector(this->partitionKeyTypeIds, this->partitionKeyIndices),
          sortKeyCmp_(std::vector<omniruntime::type::DataTypeId>(this->sortKeyTypeIds.begin(),
                                                                  this->sortKeyTypeIds.end()),
                      this->sortOrder,
                      this->sortNullsIsLast)
    {
        if (!rankConfig.contains("primaryKeys")) {
            throw std::runtime_error("Missing primaryKeys for UpdatableTopNFunction");
        }
        rowKeyIndices = rankConfig.at("primaryKeys").get<std::vector<int>>();
        for (int col : rowKeyIndices) {
            rowKeyTypeIds.push_back(this->inputRowType->at(static_cast<size_t>(col)));
        }
        if (rankConfig.contains("cacheSize")) {
            cacheSize_ = rankConfig.at("cacheSize").get<long>();
        } else {
            cacheSize_ = 10000L;
        }
        rowKeySelector = KeySelector<RowData*>(rowKeyTypeIds, rowKeyIndices);
    }

    ~UpdatableTopNFunction() override
    {
        if (kvCache != nullptr) {
            delete kvCache;
            kvCache = nullptr;
        }
        for (RankRow* row : replacedRows_) {
            deleteRankRow(row);
        }
        replacedRows_.clear();
    }

    void open(const Configuration& context) override
    {
        // Flink: MapStateDescriptor<RowData, Tuple2<RowData, Integer>>
        //        ("data-state-with-update", rowKeyType, new TupleTypeInfo<>(inputRowType, Types.INT)).
        // rowKeyType is the row key projection (primary key columns).
        std::vector<std::string> rowKeyTypeNames;
        for (int col : rowKeyIndices) {
            rowKeyTypeNames.push_back((*this->inputTypeNames)[col]);
        }
        omnistream::RowType rowKeyType(false, rowKeyTypeNames);
        omnistream::RowType inputType(false, *this->inputTypeNames);
        auto* rowKeySer = new RowDataSerializer(&rowKeyType);
        auto* valueSer = new RowDataIntPairSerializer(static_cast<int>(this->inputRowType->size()));
        auto* desc = new MapStateDescriptor<RowData*, RowDataIntPair*>(
            "data-state-with-update", rowKeySer, valueSer);
        desc->setKeyValueBackendTypeId(BackendDataType::ROW_BK, BackendDataType::ROW_INT_PAIR_BK);
        auto* runtimeContext = static_cast<StreamingRuntimeContext<K>*>(this->getRuntimeContext());
        dataState = runtimeContext->template getMapState<RowData*, RowDataIntPair*>(desc);
        stateType_ = runtimeContext->getStateType();
        omnistream::checkStateType(stateType_, "UpdatableTopNFunction");

        // Flink's AbstractTopNFunction.open() registers the rankEnd ValueState<Long>
        // only for a variable rank range.
        if (!this->isConstantRankEnd) {
            this->rankEndState.open(runtimeContext);
        }

        // Flink: lruCacheSize = max(1, cacheSize / getDefaultTopNSize()).
        const int lruCacheSize = std::max(1, static_cast<int>(cacheSize_ / this->getDefaultTopNSize()));
        kvCache = new SortedKVCache<std::string, PartitionCacheEntry*>(static_cast<size_t>(lruCacheSize));
        kvCache->setOwnsValues(true);
        INFO_RELEASE(
            "UpdatableTopNFunction: TopN = " + std::to_string(this->getDefaultTopNSize()) +
            ", lruCacheSize = " + std::to_string(lruCacheSize));
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

        // The cache evicts entries only while put() runs inside this batch, so the
        // per-batch listener is always live whenever an eviction can happen.
        kvCache->setRemovalListener([this, &ctx](const std::string& key, PartitionCacheEntry* value) {
            onCacheRemoval(key, value, ctx);
        });

        // Serialized partition bytes -> retained partition key, in first-touch order.
        std::unordered_map<std::string, K> touchedPartitions;
        std::vector<std::string> touchedOrder;
        touchedPartitions.reserve(static_cast<size_t>(rowCnt));

        for (int i = 0; i < rowCnt; ++i) {
            K rawKey = partitionKeySelector.getKey(inputBatchGuard.get(), i);
            const std::string bytes = partitionKeyString(rawKey);
            auto found = touchedPartitions.find(bytes);
            if (found == touchedPartitions.end()) {
                touchedPartitions.emplace(bytes, rawKey);
                touchedOrder.push_back(bytes);
            } else {
                delete rawKey;
            }
            K partitionKey = touchedPartitions[bytes];
            ctx.setCurrentKey(partitionKey);
            PartitionCacheEntry* entry = getOrCreateCacheEntry(partitionKey, bytes, ctx);
            processRow(inputBatchGuard.get(), i, *entry);
        }

        // Emit first, then write state, then free batch temporaries. createOutputBatch
        // reads the collected rows so it must run before cleanupBatchTemporaries().
        if (this->hasOutputRows()) {
            omnistream::VectorBatch* outputBatch = this->createOutputBatch();
            this->collectOutputBatch(out, outputBatch);
        }

        for (const std::string& bytes : touchedOrder) {
            K partitionKey = touchedPartitions[bytes];
            ctx.setCurrentKey(partitionKey);
            PartitionCacheEntry* entry = kvCache->get(bytes);
            if (entry != nullptr) {
                // Evicted entries were already flushed by onCacheRemoval.
                flushBufferToState(*entry);
            }
        }

        cleanupBatchTemporaries();

        for (auto& kv : touchedPartitions) {
            delete kv.second;
        }
    }

    ValueState<K>* getValueState() override
    {
        return nullptr;
    }

private:
    using Context = typename KeyedProcessFunction<K, RowData*, RowData*>::Context;

    // Mirrors Flink's RankRow: row (owned deep copy), innerRank within the same sort
    // key, dirty flag, and the HEAP state key pointer (not owned, nullptr on RocksDB).
    struct RankRow {
        RowData* row = nullptr;
        int innerRank = 0;
        bool dirty = false;
        RowData* stateKey = nullptr;
    };

    // One top-N working set per partition. The buffer owns its stored sort keys;
    // the rowKeyMap owns the RankRow structs (and their rows).
    struct PartitionCacheEntry {
        explicit PartitionCacheEntry(SortKeyCmp cmp) : buffer(cmp)
        {
        }
        ~PartitionCacheEntry()
        {
            for (auto& kv : rowKeyMap) {
                if (kv.second != nullptr) {
                    delete kv.second->row;
                    delete kv.second;
                }
            }
        }
        UpdatableTopNBuffer buffer;
        std::unordered_map<std::string, RankRow*> rowKeyMap;
    };

    MapState<RowData*, RowDataIntPair*>* dataState = nullptr;
    SortedKVCache<std::string, PartitionCacheEntry*>* kvCache = nullptr;
    KeySelector<K> partitionKeySelector;
    KeySelector<RowData*> rowKeySelector;
    SortKeyCmp sortKeyCmp_;
    std::vector<int> rowKeyIndices;
    std::vector<int> rowKeyTypeIds;
    long cacheSize_ = 10000L;
    bool lenient_ = true;
    omnistream::StateType stateType_ = omnistream::StateType::UNKNOWN;
    // The input row currently being processed; emitted as-is by protectRowForEmit.
    RowData* currentInputRow_ = nullptr;
    // Batch temporaries, freed by cleanupBatchTemporaries().
    std::vector<RowData*> inputRowProbes_;
    std::vector<RowData*> emittedCopies_;
    std::vector<RowData*> rowKeyProbes_;
    std::vector<RowDataIntPair*> pairsToFree_;
    // Old RankRows evicted by replaceRankRow: their rows are still referenced by the
    // caller after the replacement (emitRecordsWithRowNumber / collectUpdateBefore use
    // oldRow->row), so the structs are parked here and freed at batch end.
    std::vector<RankRow*> replacedRows_;

    bool rocksBackend() const
    {
        return stateType_ == omnistream::StateType::ROCKSDB;
    }

    // ===== sort key helpers =====
    bool sortKeyEquals(RowData* left, RowData* right) const
    {
        return !sortKeyCmp_(left, right) && !sortKeyCmp_(right, left);
    }

    int compareSortKeys(RowData* left, RowData* right) const
    {
        if (sortKeyCmp_(left, right)) {
            return -1;
        }
        if (sortKeyCmp_(right, left)) {
            return 1;
        }
        return 0;
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
                    THROW_LOGIC_EXCEPTION("UpdatableTopNFunction: unsupported sort key column type");
            }
        }
        return key;
    }

    bool checkSortKeyInBufferRange(RowData* sortKey, UpdatableTopNBuffer& buffer)
    {
        return buffer.checkSortKeyInBufferRange(sortKey, this->getDefaultTopNSize());
    }

    // ===== key serialization =====
    std::string partitionKeyString(const K& key) const
    {
        if constexpr (std::is_same_v<K, RowData*>) {
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
        } else {
            return std::to_string(key);
        }
    }

    std::string serializeKey(RowData* key) const
    {
        DataOutputSerializer output;
        OutputBufferStatus bufferStatus;
        output.setBackendBuffer(&bufferStatus);
        BinaryRowDataSerializer serializer(static_cast<int>(rowKeyTypeIds.size()));
        serializer.serialize(key, output);
        return std::string(
            reinterpret_cast<const char*>(output.getData()), static_cast<size_t>(output.getPosition()));
    }

    // BinaryRowDataSerializer::deserialize returns a shared reUse_ buffer, so the
    // result must be copied before it is stored anywhere.
    RowData* deserializeKey(const std::string& bytes) const
    {
        DataInputDeserializer input(reinterpret_cast<const uint8_t*>(bytes.data()), static_cast<int>(bytes.size()));
        BinaryRowDataSerializer serializer(static_cast<int>(rowKeyTypeIds.size()));
        RowData* row = static_cast<RowData*>(serializer.deserialize(input));
        return row->copy();
    }

    K deserializePartitionKey(const std::string& bytes) const
    {
        DataInputDeserializer input(reinterpret_cast<const uint8_t*>(bytes.data()), static_cast<int>(bytes.size()));
        BinaryRowDataSerializer serializer(static_cast<int>(this->partitionKeyTypeIds.size()));
        RowData* row = static_cast<RowData*>(serializer.deserialize(input));
        return static_cast<K>(row->copy());
    }

    std::string debugKey(const K& key, const std::vector<int>& typeIds) const
    {
        if constexpr (std::is_same_v<K, RowData*>) {
            return this->debugRow(key, typeIds);
        } else {
            return std::to_string(key);
        }
    }

    std::string debugTreeMap(const std::map<int, std::string>& treeMap) const
    {
        std::ostringstream out;
        out << "{";
        bool first = true;
        for (const auto& entry : treeMap) {
            if (!first) {
                out << ", ";
            }
            first = false;
            RowData* rowKey = deserializeKey(entry.second);
            out << entry.first << "=" << this->debugRow(rowKey, rowKeyTypeIds);
            delete rowKey;
        }
        out << "}";
        return out.str();
    }

    // ===== memory helpers =====
    static void deletePair(RowDataIntPair* pair)
    {
        if (pair != nullptr) {
            delete pair->row;
            delete pair;
        }
    }

    static void deleteRankRow(RankRow* rankRow)
    {
        if (rankRow != nullptr) {
            delete rankRow->row;
            delete rankRow;
        }
    }

    // Emitted rows must outlive the batch conversion. The current input row is
    // emitted as-is (it lives in inputRowProbes_ until cleanup); every other row
    // (state rows, replaced rows) is copied into emittedCopies_.
    RowData* protectRowForEmit(RowData* row)
    {
        if (row == currentInputRow_) {
            return row;
        }
        RowData* copy = row->copy();
        emittedCopies_.push_back(copy);
        return copy;
    }

    // ===== partition cache =====
    // Mirrors Flink's initHeapStates: fetch the partition's working set from the LRU
    // cache, restoring it from the state on first touch. The caller must have already
    // set the current key to partitionKey.
    PartitionCacheEntry* getOrCreateCacheEntry(K partitionKey, const std::string& partitionKeyBytes, Context& ctx)
    {
        this->requestCount += 1;
        PartitionCacheEntry* entry = kvCache->get(partitionKeyBytes);
        if (entry == nullptr) {
            entry = new PartitionCacheEntry(sortKeyCmp_);
            kvCache->put(partitionKeyBytes, entry);
            restorePartition(partitionKey, *entry);
        } else {
            this->hitCount += 1;
        }
        return entry;
    }

    // Adds one restored state record to the partition working set. `takeRow` is true
    // when the caller owns `pair`'s row (RocksDB iterator) and false when only a copy
    // may be taken (HEAP live map / BSS snapshot).
    void restoreRecord(PartitionCacheEntry& entry,
                       std::map<RowData*, std::map<int, std::string>, SortKeyCmp>& tempSortedMap,
                       const std::string& rowKeyBytes,
                       RowDataIntPair* pair,
                       RowData* stateKey,
                       bool takeRow)
    {
        if (takeRow) {
            entry.rowKeyMap.emplace(rowKeyBytes, new RankRow{pair->row, pair->innerRank, false, nullptr});
        } else {
            entry.rowKeyMap.emplace(rowKeyBytes, new RankRow{pair->row->copy(), pair->innerRank, false, stateKey});
        }
        RowData* sortKey = projectSortKey(pair->row);
        auto [sit, inserted] = tempSortedMap.try_emplace(sortKey, std::map<int, std::string>());
        if (!inserted) {
            delete sortKey;
        }
        sit->second.emplace(pair->innerRank, rowKeyBytes);
    }

    // Rebuilds the partition's buffer and rowKeyMap from the "data-state-with-update"
    // entries. The caller must have set the current key.
    //
    // Mirrors Flink's initHeapStates: HEAP and ROCKSDB read the backend's CURRENT
    // state through a fresh iterator (Flink: dataState.iterator()) instead of a
    // cached snapshot. This matters after a partition was evicted from kvCache and
    // flushed by onCacheRemoval: a later touch in the same batch must observe the
    // flushed state, and no stale/dangling entry from
    // RocksdbMapStateTable::entriesCache may be reused. BSS keeps using entries(),
    // which already performs a fresh on-disk scan on every call.
    void restorePartition(const K& partitionKey, PartitionCacheEntry& entry)
    {
        // Temporary map keeping each sort key's records ordered by inner rank
        // (Flink's HashMap<RowData, TreeMap<Integer, RowData>>).
        std::map<RowData*, std::map<int, std::string>, SortKeyCmp> tempSortedMap(sortKeyCmp_);
        if (stateType_ == omnistream::StateType::BSS) {
            emhash7::HashMap<RowData*, RowDataIntPair*>* encoded = dataState->entries();
            if (encoded != nullptr) {
                for (const auto& kv : *encoded) {
                    RowData* rowKey = kv.first;
                    RowDataIntPair* pair = kv.second;
                    if (rowKey == nullptr || pair == nullptr || pair->row == nullptr) {
                        throw std::runtime_error("UpdatableTopNFunction data-state-with-update is malformed");
                    }
                    restoreRecord(entry, tempSortedMap, serializeKey(rowKey), pair, rowKey, false);
                }
            }
        } else {
            auto iter = dataState->iteratorV2();
            if (iter != nullptr) {
                while (iter->hasNext()) {
                    auto& mapEntry = iter->next();
                    auto valueOpt = mapEntry.getValue();
                    if (!valueOpt.has_value() || valueOpt.value() == nullptr || valueOpt.value()->row == nullptr) {
                        throw std::runtime_error("UpdatableTopNFunction data-state-with-update is malformed");
                    }
                    RowDataIntPair* pair = valueOpt.value();
                    if (rocksBackend()) {
                        // Do NOT call mapEntry.getKey() on RocksDB: RocksDBMapEntryV2 takes
                        // ownership of the deserialized key and deletes it, while the row-key
                        // serializer hands back a shared reusable buffer. The state key is by
                        // construction the row-key projection of the stored record, so derive
                        // it from the value row instead (Flink's rowKeySelector invariant).
                        RowData* projectedKey = rowKeySelector.getKey(pair->row);
                        const std::string rowKeyBytes = serializeKey(projectedKey);
                        delete projectedKey;
                        // Take the deserialized row into the RankRow. The pair struct stays
                        // owned by the iterator entry (RocksDBMapEntryV2) and is freed when
                        // the entry is released; deleting it here would double free it.
                        restoreRecord(entry, tempSortedMap, rowKeyBytes, pair, nullptr, true);
                    } else {
                        auto keyOpt = mapEntry.getKey();
                        if (!keyOpt.has_value() || keyOpt.value() == nullptr) {
                            throw std::runtime_error("UpdatableTopNFunction data-state-with-update is malformed");
                        }
                        // The live heap map owns both the key and the value; mirror them.
                        restoreRecord(entry, tempSortedMap, serializeKey(keyOpt.value()), pair, keyOpt.value(), false);
                    }
                }
            }
        }
        for (auto& skEntry : tempSortedMap) {
            RowData* sortKey = skEntry.first;
            for (auto& rankEntry : skEntry.second) {
                const int size = entry.buffer.put(sortKey, rankEntry.second);
                if (rankEntry.first != size) {
                    WARN_RELEASE(
                        "Failed to build sorted map from state, this may result in wrong result. "
                        "The sort key is " + this->debugRow(sortKey, this->sortKeyTypeIds) +
                        ", partition key is " + debugKey(partitionKey, this->partitionKeyTypeIds) +
                        ", treeMap is " + debugTreeMap(skEntry.second) +
                        ". The expected inner rank is " + std::to_string(rankEntry.first) +
                        ", but current size is " + std::to_string(size));
                }
            }
        }
        for (auto& skEntry : tempSortedMap) {
            delete skEntry.first;
        }
    }

    // Finds the value in the live heap map by the actual stored key pointer. The
    // caller must have set the current key. Lookup is content-based (the inner
    // emhash7 map uses std::hash/equal_to<RowData*>), so a freshly deserialized
    // key would also match; the stored pointer is used to stay ownership-exact.
    RowDataIntPair* heapFindPair(RowData* stateKey)
    {
        emhash7::HashMap<RowData*, RowDataIntPair*>* encoded = dataState->entries();
        if (encoded == nullptr) {
            return nullptr;
        }
        auto it = encoded->find(stateKey);
        if (it == encoded->end()) {
            return nullptr;
        }
        return it->second;
    }

    RankRow* findRankRow(PartitionCacheEntry& entry, const std::string& rowKeyBytes)
    {
        auto it = entry.rowKeyMap.find(rowKeyBytes);
        if (it == entry.rowKeyMap.end()) {
            throw std::runtime_error("UpdatableTopNFunction: row key not found in rowKeyMap");
        }
        return it->second;
    }

    // Mirrors Flink's rowKeyMap.put(rowKey, newRankRow): swaps the rowKeyMap entry for
    // an updated record already in the top-N. On HEAP the state key pointer must be
    // transferred to the new RankRow. The backend maps identify RowData* by CONTENT
    // (std::hash/equal_to<RowData*> hash and compare the bytes), so a freshly
    // deserialized key would still match the stored entry; the transfer is about
    // ownership instead: emhash7 stores the exact pointer passed on insert, so keeping
    // one owned stateKey in sync with the map's stored key avoids allocating a second
    // key on the next flush and leaking the map's stored key. The caller still reads
    // oldRow (oldRow->row) after this returns (emitRecordsWithRowNumber /
    // collectUpdateBefore), so the old struct is parked in replacedRows_ and freed by
    // cleanupBatchTemporaries() at batch end.
    void replaceRankRow(PartitionCacheEntry& entry,
                        typename std::unordered_map<std::string, RankRow*>::iterator found,
                        RankRow* oldRow,
                        const std::string& rowKeyBytes,
                        RankRow* newRow)
    {
        newRow->stateKey = oldRow->stateKey;
        oldRow->stateKey = nullptr;
        replacedRows_.push_back(oldRow);
        entry.rowKeyMap.erase(found);
        entry.rowKeyMap.emplace(rowKeyBytes, newRow);
    }

    // ===== state write-back =====
    // Flink's flushBufferToState: writes every dirty row and clears the dirty flag.
    // The caller must have set the current key to the partition key.
    void flushBufferToState(PartitionCacheEntry& entry)
    {
        for (auto& kv : entry.rowKeyMap) {
            RankRow* rankRow = kv.second;
            if (rankRow->dirty) {
                flushDirtyRow(rankRow, kv.first);
                rankRow->dirty = false;
            }
        }
    }

    void flushDirtyRow(RankRow* rankRow, const std::string& rowKeyBytes)
    {
        if (rocksBackend()) {
            // RocksDB serializes both immediately, so the probe and the new pair are
            // only needed to stay alive until the call returns (deferred to batch end).
            RowData* probe = deserializeKey(rowKeyBytes);
            auto* pair = new RowDataIntPair{rankRow->row->copy(), rankRow->innerRank};
            dataState->put(probe, pair);
            rowKeyProbes_.push_back(probe);
            pairsToFree_.push_back(pair);
        } else {
            if (rankRow->stateKey == nullptr) {
                rankRow->stateKey = deserializeKey(rowKeyBytes);
            }
            RowDataIntPair* oldPair = heapFindPair(rankRow->stateKey);
            if (oldPair != nullptr) {
                deletePair(oldPair);
            }
            auto* pair = new RowDataIntPair{rankRow->row->copy(), rankRow->innerRank};
            dataState->put(rankRow->stateKey, pair);
        }
    }

    // Flink's dataState.remove(rowKey) for the out-of-TopN cleanup paths. On HEAP the
    // stored key pointer is required: the inner emhash7 map matches by content but keeps
    // the exact key pointer passed at insert, so remove must use the same pointer and the
    // operator frees it here (delete rankRow->stateKey after erase) to avoid a leak. A
    // row that was never written to state has stateKey == nullptr and nothing to remove.
    void removeFromState(RankRow* rankRow, const std::string& rowKeyBytes)
    {
        if (rocksBackend()) {
            RowData* probe = deserializeKey(rowKeyBytes);
            dataState->remove(probe);
            rowKeyProbes_.push_back(probe);
        } else if (rankRow->stateKey != nullptr) {
            RowDataIntPair* oldPair = heapFindPair(rankRow->stateKey);
            if (oldPair == nullptr) {
                throw std::runtime_error(
                    "UpdatableTopNFunction removeFromState: state key not found in heap map");
            }
            dataState->remove(rankRow->stateKey);
            deletePair(oldPair);
            delete rankRow->stateKey;
            rankRow->stateKey = nullptr;
        }
    }

    // Mirrors Flink's CacheRemovalListener: an entry evicted by size overflow flushes
    // its dirty rows under the evicted partition's key. The probe key is parked in
    // rowKeyProbes_ so it is not freed while the key context still points at it.
    // The probe is a freshly deserialized partition key, but partition routing is safe:
    // KeyGroupRangeAssignment hashes RowData by content and CopyOnWriteStateMap matches
    // RowData* by content too (std::hash/equal_to<RowData*>, see RowData.h), so the probe
    // resolves to the SAME partition that owns this entry -- there is no pointer-identity
    // hazard here.
    void onCacheRemoval(const std::string& partitionKeyBytes, PartitionCacheEntry* entry, Context& ctx)
    {
        if (entry == nullptr) {
            return;
        }
        K probe = deserializePartitionKey(partitionKeyBytes);
        K previousKey = ctx.getCurrentKey();
        ctx.setCurrentKey(probe);
        flushBufferToState(*entry);
        ctx.setCurrentKey(previousKey);
        rowKeyProbes_.push_back(static_cast<RowData*>(probe));
    }

    // Flink's updateInnerRank: renumbers the records under oldSortKey after one was
    // moved to a new sort key, marking changed ranks dirty.
    void updateInnerRank(PartitionCacheEntry& entry, RowData* oldSortKey)
    {
        std::vector<std::string>* list = entry.buffer.get(oldSortKey);
        if (list == nullptr) {
            return;
        }
        int innerRank = 1;
        for (const std::string& rowKeyBytes : *list) {
            RankRow* row = findRankRow(entry, rowKeyBytes);
            if (row->innerRank != innerRank) {
                row->innerRank = innerRank;
                row->dirty = true;
            }
            innerRank += 1;
        }
    }

    // Flink's rowNumber: computes (rank, innerRank) of rowKey within the buffer.
    std::pair<int, int> rowNumber(PartitionCacheEntry& entry, RowData* sortKey, const std::string& rowKeyBytes)
    {
        int curRank = 1;
        for (auto& bufferEntry : entry.buffer) {
            RowData* curKey = bufferEntry.first;
            std::vector<std::string>* rowKeys = bufferEntry.second;
            if (sortKeyEquals(curKey, sortKey)) {
                int innerRank = 1;
                for (const std::string& rowKey : *rowKeys) {
                    if (rowKey == rowKeyBytes) {
                        return {curRank, innerRank};
                    }
                    innerRank += 1;
                    curRank += 1;
                }
            } else {
                curRank += static_cast<int>(rowKeys->size());
            }
        }
        RowData* rowKey = deserializeKey(rowKeyBytes);
        std::string errorMsg =
            "Failed to find the sortKey: " + this->debugRow(sortKey, this->sortKeyTypeIds) +
            ", rowkey: " + this->debugRow(rowKey, rowKeyTypeIds) +
            " in the buffer. This should never happen";
        delete rowKey;
        ERROR_RELEASE(errorMsg);
        throw std::runtime_error(errorMsg);
    }

    // ===== emit: with row number =====
    // Flink's emitRecordsWithRowNumber (both the new-record and update overloads;
    // oldSortKey/oldRow/oldRank are nulled for a new record). All emitted state rows
    // are protected copies; the caller keeps oldRow and oldSortKey alive for the call.
    void emitRecordsWithRowNumber(PartitionCacheEntry& entry,
                                  RowData* sortKey,
                                  RowData* inputRow,
                                  int64_t timestamp,
                                  RowData* oldSortKey = nullptr,
                                  RankRow* oldRow = nullptr,
                                  int oldRank = -1)
    {
        int currentRank = 0;
        RowData* currentRow = nullptr;
        bool findsSortKey = false;
        bool oldRowRetracted = false;
        auto it = entry.buffer.begin();
        for (; it != entry.buffer.end() && this->isInRankEnd(currentRank); ++it) {
            RowData* curSortKey = it->first;
            std::vector<std::string>* rowKeys = it->second;
            if (!findsSortKey && sortKeyEquals(curSortKey, sortKey)) {
                currentRank += static_cast<int>(rowKeys->size());
                currentRow = inputRow;
                findsSortKey = true;
            } else if (findsSortKey) {
                if (oldSortKey == nullptr) {
                    for (const std::string& rowKeyBytes : *rowKeys) {
                        if (!this->isInRankEnd(currentRank)) {
                            break;
                        }
                        RankRow* prevRow = findRankRow(entry, rowKeyBytes);
                        this->collectUpdateBefore(protectRowForEmit(prevRow->row), currentRank, timestamp);
                        if (currentRow == inputRow && oldRow != nullptr && !oldRowRetracted) {
                            this->collectUpdateBefore(protectRowForEmit(oldRow->row), oldRank, timestamp);
                            oldRowRetracted = true;
                        }
                        this->collectUpdateAfter(protectRowForEmit(currentRow), currentRank, timestamp);
                        currentRow = prevRow->row;
                        currentRank += 1;
                    }
                } else {
                    const int compare = compareSortKeys(curSortKey, oldSortKey);
                    if (compare <= 0) {
                        for (const std::string& rowKeyBytes : *rowKeys) {
                            if (currentRank >= oldRank) {
                                break;
                            }
                            RankRow* prevRow = findRankRow(entry, rowKeyBytes);
                            this->collectUpdateBefore(protectRowForEmit(prevRow->row), currentRank, timestamp);
                            if (currentRow == inputRow && oldRow != nullptr && !oldRowRetracted) {
                                this->collectUpdateBefore(protectRowForEmit(oldRow->row), oldRank, timestamp);
                                oldRowRetracted = true;
                            }
                            this->collectUpdateAfter(protectRowForEmit(currentRow), currentRank, timestamp);
                            currentRow = prevRow->row;
                            currentRank += 1;
                        }
                    } else {
                        break;
                    }
                }
            } else {
                currentRank += static_cast<int>(rowKeys->size());
            }
        }
        if (this->isInRankEnd(currentRank)) {
            if (oldRow == nullptr) {
                this->collectInsert(protectRowForEmit(currentRow), currentRank, timestamp);
            } else {
                if (currentRank != oldRank) {
                    std::string errorMsg =
                        "UpdatableTopNFunction invariant violated: currentRank " +
                        std::to_string(currentRank) + " != oldRank " +
                        std::to_string(oldRank) +
                        ", sortKey=" + this->debugRow(sortKey, this->sortKeyTypeIds) +
                        ", oldSortKey=" + this->debugRow(oldSortKey, this->sortKeyTypeIds) +
                        ", inputRow=" + this->debugRow(inputRow, *this->inputRowType);
                    ERROR_RELEASE(errorMsg);
                    throw std::invalid_argument(errorMsg);
                }
                if (!oldRowRetracted) {
                    this->collectUpdateBefore(protectRowForEmit(oldRow->row), oldRank, timestamp);
                }
                this->collectUpdateAfter(protectRowForEmit(currentRow), currentRank, timestamp);
            }
            return;
        }

        // Remove the records out of the Top-N: drop their state and rowKeyMap entries,
        // then drop the whole sort-key groups from the buffer.
        std::vector<RowData*> toDeleteSortKeys;
        for (; it != entry.buffer.end(); ++it) {
            std::vector<std::string>* rowKeys = it->second;
            for (const std::string& rowKeyBytes : *rowKeys) {
                auto rit = entry.rowKeyMap.find(rowKeyBytes);
                if (rit == entry.rowKeyMap.end()) {
                    throw std::runtime_error("UpdatableTopNFunction: row key missing in rowKeyMap");
                }
                RankRow* rankRow = rit->second;
                removeFromState(rankRow, rowKeyBytes);
                entry.rowKeyMap.erase(rit);
                deleteRankRow(rankRow);
            }
            toDeleteSortKeys.push_back(it->first);
        }
        for (RowData* toDeleteKey : toDeleteSortKeys) {
            entry.buffer.removeAll(toDeleteKey);
        }
    }

    // Flink's emitRecordsWithRowNumberIgnoreStateError: the lenient path taken when
    // the new sort key is lower than the old one (monotonicity broken, e.g. TTL-staled
    // state). Emits UB of the old row, shifts the affected ranks, then UA of the new row.
    void emitRecordsWithRowNumberIgnoreStateError(PartitionCacheEntry& entry,
                                                  RowData* newRow,
                                                  int newRank,
                                                  RankRow* oldRow,
                                                  int oldRank,
                                                  int64_t timestamp)
    {
        int currentRank = 0;
        RowData* currentRow = nullptr;
        this->collectUpdateBefore(protectRowForEmit(oldRow->row), oldRank, timestamp);
        bool reached = false;
        for (auto& bufferEntry : entry.buffer) {
            std::vector<std::string>* rowKeys = bufferEntry.second;
            for (const std::string& rowKeyBytes : *rowKeys) {
                currentRank += 1;
                RankRow* rr = findRankRow(entry, rowKeyBytes);
                currentRow = rr->row;
                if (currentRank == newRank) {
                    reached = true;
                    break;
                }
                if (oldRank <= currentRank) {
                    this->collectUpdateBefore(protectRowForEmit(currentRow), currentRank + 1, timestamp);
                    this->collectUpdateAfter(protectRowForEmit(currentRow), currentRank, timestamp);
                }
            }
            if (reached) {
                break;
            }
        }
        this->collectUpdateAfter(protectRowForEmit(newRow), newRank, timestamp);
    }

    // ===== per-row algorithms =====
    // Flink's processElementWithRowNumber (selected when outputRankNumber or hasOffset()).
    void processElementWithRowNumber(PartitionCacheEntry& entry, RowData* inputRow, int64_t timestamp)
    {
        RowData* sortKey = projectSortKey(inputRow);
        RowData* rowKey = rowKeySelector.getKey(inputRow);
        const std::string rowKeyBytes = serializeKey(rowKey);
        delete rowKey;
        auto found = entry.rowKeyMap.find(rowKeyBytes);
        if (found != entry.rowKeyMap.end()) {
            // An updated record which is already in the top-N.
            RankRow* oldRow = found->second;
            RowData* oldSortKey = projectSortKey(oldRow->row);
            const int compare = compareSortKeys(sortKey, oldSortKey);
            if (compare == 0) {
                // Sort key unchanged: the rank stays, only the row content changes.
                const auto [rank, innerRank] = rowNumber(entry, sortKey, rowKeyBytes);
                this->collectUpdateBefore(protectRowForEmit(oldRow->row), rank, timestamp);
                this->collectUpdateAfter(protectRowForEmit(inputRow), rank, timestamp);
                replaceRankRow(entry, found, oldRow, rowKeyBytes, new RankRow{inputRow->copy(), innerRank, true, nullptr});
            } else {
                const auto [oldRank, oldInnerRank] = rowNumber(entry, oldSortKey, rowKeyBytes);
                // Remove the row from the old sort key, add it under the new one.
                entry.buffer.remove(oldSortKey, rowKeyBytes);
                const int size = entry.buffer.put(sortKey, rowKeyBytes);
                replaceRankRow(entry, found, oldRow, rowKeyBytes, new RankRow{inputRow->copy(), size, true, nullptr});
                // Update inner ranks of the records left under the old sort key.
                updateInnerRank(entry, oldSortKey);
                if (compare < 0) {
                    // New sort key sorts better: shift the affected ranks.
                    emitRecordsWithRowNumber(entry, sortKey, inputRow, timestamp, oldSortKey, oldRow, oldRank);
                } else {
                    std::string errorMsg =
                        "The input retract record:{" + this->debugRow(inputRow, *this->inputRowType) +
                        "}'s sort key: " + this->debugRow(sortKey, this->sortKeyTypeIds) +
                        " is lower than old sort key: " +
                        this->debugRow(oldSortKey, this->sortKeyTypeIds) +
                        ", this break the monotonicity on sort key field which is guaranteed by the "
                        "sql semantic. It's highly possible upstream stateful operator has shorter state "
                        "ttl than the stream records is that cause the staled record cleared by state ttl.";
                    if (lenient_) {
                        WARN_RELEASE(errorMsg);
                        const auto [newRank, newInnerRank] =
                            rowNumber(entry, sortKey, rowKeyBytes);
                        emitRecordsWithRowNumberIgnoreStateError(
                            entry, inputRow, newRank, oldRow, oldRank, timestamp);
                    } else {
                        ERROR_RELEASE(errorMsg);
                        throw std::runtime_error(errorMsg);
                    }
                }
            }
            delete oldSortKey;
        } else if (checkSortKeyInBufferRange(sortKey, entry.buffer)) {
            // A new record inside the top-N.
            const int size = entry.buffer.put(sortKey, rowKeyBytes);
            entry.rowKeyMap.emplace(
                rowKeyBytes, new RankRow{inputRow->copy(), size, true, nullptr});
            emitRecordsWithRowNumber(entry, sortKey, inputRow, timestamp);
        }
        delete sortKey;
    }

    // Flink's processElementWithoutRowNumber (no rank column, no offset).
    void processElementWithoutRowNumber(PartitionCacheEntry& entry, RowData* inputRow, int64_t timestamp)
    {
        RowData* sortKey = projectSortKey(inputRow);
        RowData* rowKey = rowKeySelector.getKey(inputRow);
        const std::string rowKeyBytes = serializeKey(rowKey);
        delete rowKey;
        auto found = entry.rowKeyMap.find(rowKeyBytes);
        if (found != entry.rowKeyMap.end()) {
            // An updated record which is already in the top-N.
            RankRow* oldRow = found->second;
            RowData* oldSortKey = projectSortKey(oldRow->row);
            if (!sortKeyEquals(oldSortKey, sortKey)) {
                entry.buffer.remove(oldSortKey, rowKeyBytes);
                const int size = entry.buffer.put(sortKey, rowKeyBytes);
                replaceRankRow(entry, found, oldRow, rowKeyBytes, new RankRow{inputRow->copy(), size, true, nullptr});
                updateInnerRank(entry, oldSortKey);
            } else {
                // Sort key unchanged: keep the inner rank, update the row content.
                replaceRankRow(entry, found, oldRow, rowKeyBytes,
                               new RankRow{inputRow->copy(), oldRow->innerRank, true, nullptr});
            }
            delete oldSortKey;
            this->collectUpdateBefore(protectRowForEmit(oldRow->row), timestamp);
            this->collectUpdateAfter(protectRowForEmit(inputRow), timestamp);
        } else if (checkSortKeyInBufferRange(sortKey, entry.buffer)) {
            // A new record inside the top-N.
            const int size = entry.buffer.put(sortKey, rowKeyBytes);
            entry.rowKeyMap.emplace(
                rowKeyBytes, new RankRow{inputRow->copy(), size, true, nullptr});
            // Remove the retired element when the buffer overflows.
            if (entry.buffer.getCurrentTopNum() > this->rankEnd) {
                const std::string lastRowKey = entry.buffer.removeLast();
                if (!lastRowKey.empty()) {
                    auto rit = entry.rowKeyMap.find(lastRowKey);
                    if (rit == entry.rowKeyMap.end()) {
                        throw std::runtime_error("UpdatableTopNFunction: retired row key missing in rowKeyMap");
                    }
                    RankRow* lastRow = rit->second;
                    entry.rowKeyMap.erase(rit);
                    removeFromState(lastRow, lastRowKey);
                    this->collectDelete(protectRowForEmit(lastRow->row), timestamp);
                    deleteRankRow(lastRow);
                }
            }
            this->collectInsert(protectRowForEmit(inputRow), timestamp);
        }
        delete sortKey;
    }

    // Per-batch-row dispatch: extract the row (owned temporary), select the algorithm,
    // and mirror Flink's per-record initRankEnd under the partition's current key
    // (set by processBatch before the row is dispatched).
    void processRow(omnistream::VectorBatch* batch, int rowIndex, PartitionCacheEntry& entry)
    {
        RowData* inputRow = batch->extractRowData(rowIndex);
        inputRowProbes_.push_back(inputRow);
        currentInputRow_ = inputRow;
        const int64_t timestamp = batch->getTimestamp(rowIndex);
        this->initRankEnd(inputRow);
        if (this->outputRankNumber || this->hasOffset()) {
            processElementWithRowNumber(entry, inputRow, timestamp);
        } else {
            processElementWithoutRowNumber(entry, inputRow, timestamp);
        }
        currentInputRow_ = nullptr;
    }

    // Frees every pointer accumulated while processing the batch.
    void cleanupBatchTemporaries()
    {
        for (RowData* row : inputRowProbes_) {
            delete row;
        }
        inputRowProbes_.clear();
        for (RowData* row : emittedCopies_) {
            delete row;
        }
        emittedCopies_.clear();
        for (RowData* key : rowKeyProbes_) {
            delete key;
        }
        rowKeyProbes_.clear();
        for (RowDataIntPair* pair : pairsToFree_) {
            deletePair(pair);
        }
        pairsToFree_.clear();
        for (RankRow* row : replacedRows_) {
            deleteRankRow(row);
        }
        replacedRows_.clear();
        currentInputRow_ = nullptr;
    }
};

#endif // OMNISTREAM_UPDATABLETOPNFUNCTION_H
