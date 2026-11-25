/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

#pragma once

#include "AbstractTopNFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "rank_range.h"
#include "Top1Comparator.h"
#include "types/logical/RowType.h"
#include "typeutils/InternalTypeInfo.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "typeutils/RowDataSerializer.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "SortedKVCache.h"
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <memory>
#include <cstring>

// FastTop1Function processes batches of rows and maintains the Top-1 per partition.
// The partition key type is templated (KeyType).
template <typename KeyType>
class FastTop1Function : public AbstractTopNFunction<KeyType> {
public:
    explicit FastTop1Function(const nlohmann::json& rankConfig) : AbstractTopNFunction<KeyType>(rankConfig)
    {
    }
    ~FastTop1Function()
    {
        delete comparator;
    }
    // Processes a batch of rows from inputBatch and writes results into outputBatch.
    void processBatch(
        omnistream::VectorBatch* inputBatch,
        typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx,
        TimestampedCollector& out) override;

    ValueState<KeyType>* getValueState() override
    {
        return nullptr;
    };
    // Initialization method; call before processing batches.
    void open(const Configuration& context) override;
    void freeKeyInCache(KeyType mutableKey);

private:
    ValueState<RowData*>* stateStore = nullptr;
    Top1Comparator<KeyType>* comparator = nullptr;
    SortedKVCache<KeyType, RowData*> kvCache;
    omnistream::StateType backendType_ = omnistream::StateType::HEAP;
    int compareRows(BinaryRowData* inputRow, BinaryRowData* previousRow);
    int compareRowsV2(omnistream::VectorBatch* vectorBatch, int rowId, BinaryRowData* previousRow);
};

template <typename KeyType>
void FastTop1Function<KeyType>::open(const Configuration& context)
{
    auto rankRowTypeInfo = InternalTypeInfo::ofRowType(new omnistream::RowType(true, *this->inputTypeNames));
    std::string name = "Top1-Rank-State";
    ValueStateDescriptor<RowData*>* recordStateDesc = new ValueStateDescriptor<RowData*>(name, rankRowTypeInfo);
    recordStateDesc->SetStateSerializer(rankRowTypeInfo->getTypeSerializer());
    this->stateStore = static_cast<StreamingRuntimeContext<KeyType>*>(this->getRuntimeContext())
                           ->template getState<RowData*>(recordStateDesc);
    // Decide backendType by probing the concrete state implementation.
    if (dynamic_cast<RocksdbValueState<KeyType, VoidNamespace, RowData*>*>(this->stateStore)) {
        INFO_RELEASE("FastTop1Function backend is rocksdb");
        this->backendType_ = omnistream::StateType::ROCKSDB;
        // RocksDB stores a byte-copy in the DB, not the ptr — cache is sole owner
        // of its V pointers, so it's safe to free them on LRU eviction.
        kvCache.setOwnsValues(true);
    } else {
        INFO_RELEASE("FastTop1Function backend is mem");
        this->backendType_ = omnistream::StateType::HEAP;
    }
    comparator = new Top1Comparator<KeyType>(
        this->partitionKeyTypeIds, this->partitionKeyIndices, this->sortKeyIndices, this->sortOrder);
}

template <typename KeyType>
void FastTop1Function<KeyType>::processBatch(
    omnistream::VectorBatch* inputBatch,
    typename KeyedProcessFunction<KeyType, RowData*, RowData*>::Context& ctx,
    TimestampedCollector& out)
{
    int rowCount = inputBatch->GetRowCount();
    if (rowCount == 0) {
        return;
    }
    // Find top-1 row IDs by partition using the helper function.
    // std::unordered_map<KeyType, int> top1RowIds = comparator->findTop1RowIdsByPartition(inputBatch);
    std::unordered_map<KeyType, int> top1RowIds = comparator->findTop1RowIdsByPartitionV2(inputBatch);
    std::set<RowData*> rowToDel;
    // Process each partition key's top row.
    for (const auto& [partitionKey, rowId] : top1RowIds) {
        KeyType mutableKey = partitionKey;
        ctx.setCurrentKey(mutableKey);
        // Retrieve the top row for the partition key.
        RowData* previousRow = kvCache.get(mutableKey);
        bool fromCache = previousRow != nullptr;
        if (previousRow == nullptr) {
            previousRow = stateStore->value();
        }

        if (previousRow == nullptr) {
            RowData* inputRow = inputBatch->extractRowData(rowId);

            // No previous state, insert the new row into the state store.
            stateStore->update(inputRow, false);
            kvCache.put(mutableKey, inputRow);
            int64_t timestamp = inputBatch->getTimestamp(rowId);
            this->collectInsert(inputRow, 1, timestamp); // Emit the new row.
        } else {
            if (compareRowsV2(inputBatch, rowId, static_cast<BinaryRowData*>(previousRow)) > 0) {
                int64_t timestamp = inputBatch->getTimestamp(rowId);
                RowData* inputRow = inputBatch->extractRowData(rowId);
                this->collectUpdateBefore(previousRow, 1, timestamp); // Emit an update.
                this->collectUpdateAfter(inputRow, 1, timestamp);
                stateStore->update(inputRow, false);
                kvCache.put(mutableKey, inputRow);
                if (!fromCache) {
                    rowToDel.insert(previousRow);
                }
            } else {
                // Case B2: retain existing state value.
                // If previousRow came from stateStore->value() on rocksdb, it is
                // a fresh copy we own. Rather than discard, warm the cache with it
                // so a future lookup for this partition can skip a rocksdb read.
                // Cache takes ownership of both mutableKey and previousRow; LRU
                // eviction (with ownsValues=true) will free them later.
                // For heap backend, previousRow is still owned by the state —
                // cannot hand it to the cache, just free the local key copy.
                if (!fromCache && this->backendType_ == omnistream::StateType::ROCKSDB) {
                    kvCache.put(mutableKey, previousRow);
                } else {
                    freeKeyInCache(mutableKey);
                }
            }
        }
    }
    // omniruntime::vec::VectorHelper::FreeVecBatch(inputBatch);
    delete inputBatch;
    omnistream::VectorBatch* outputBatch = this->createOutputBatch();
    this->collectOutputBatch(out, outputBatch);
    // Drain previousRow pointers we became sole owners of (see per-case comments).
    for (RowData* r : rowToDel) {
        delete r;
    }
    // Explicitly clear the `top1RowIds` map to release memory.
    kvCache.clearOldValues();
}

template <typename KeyType>
void FastTop1Function<KeyType>::freeKeyInCache(KeyType key)
{
    if constexpr (std::is_same<KeyType, RowData*>::value) {
        delete key;
    }
}

template <typename KeyType>
int FastTop1Function<KeyType>::compareRows(BinaryRowData* inputRow, BinaryRowData* previousRow)
{
    if (!inputRow) {
        LOG("input row is null");
        throw std::runtime_error("input row is null");
    }
    for (size_t i = 0; i < this->sortKeyIndices.size(); ++i) {
        int colId = this->sortKeyIndices[i];
        bool ascending = this->sortOrder[i];

        // Compare values based on the column type
        int comparisonResult = 0;

        switch (this->inputRowType->at(colId)) {
            case DataTypeId::OMNI_INT: {
                int32_t inputVal = *inputRow->getInt(colId);
                int32_t previousVal = *previousRow->getInt(colId);
                comparisonResult = (inputVal < previousVal) ? -1 : (inputVal > previousVal) ? 1 : 0;
                break;
            }
            case DataTypeId::OMNI_LONG: {
                int64_t inputVal = *inputRow->getLong(colId);
                int64_t previousVal = *previousRow->getLong(colId);
                comparisonResult = (inputVal < previousVal) ? -1 : (inputVal > previousVal) ? 1 : 0;
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: {
                if (!inputRow) {
                    throw std::runtime_error("input row is null, check the data");
                }
                TimestampData inputVal = inputRow->getTimestamp(colId);
                TimestampData previousVal = previousRow->getTimestamp(colId);
                if (inputVal.getMillisecond() == previousVal.getMillisecond()) {
                    comparisonResult = (inputVal.getNanoOfMillisecond() < previousVal.getNanoOfMillisecond())   ? -1
                                       : (inputVal.getNanoOfMillisecond() > previousVal.getNanoOfMillisecond()) ? 1
                                                                                                                : 0;
                } else {
                    comparisonResult = (inputVal.getMillisecond() < previousVal.getMillisecond()) ? -1 : 1;
                }
                break;
            }
                // Add other data types as needed.
            default:
                throw std::runtime_error("Unsupported DataTypeId for row comparison." + this->inputRowType->at(colId));
        }

        // Adjust the comparison result based on the sort order.
        if (comparisonResult != 0) {
            return ascending ? comparisonResult : -comparisonResult;
        }
    }

    // If all key columns are equal, return 0.
    return 0;
}

template <typename KeyType>
int FastTop1Function<KeyType>::compareRowsV2(omnistream::VectorBatch* originalVb, int rowId, BinaryRowData* previousRow)
{
    for (size_t i = 0; i < this->sortKeyIndices.size(); ++i) {
        int colId = this->sortKeyIndices[i];
        bool ascending = this->sortOrder[i];

        // Compare values based on the column type
        int comparisonResult = 0;

        switch (this->inputRowType->at(colId)) {
            case DataTypeId::OMNI_INT: {
                int32_t inputVal = reinterpret_cast<vec::Vector<int32_t>*>(originalVb->Get(colId))->GetValue(rowId);
                int32_t previousVal = *previousRow->getInt(colId);
                comparisonResult = (inputVal < previousVal) ? -1 : (inputVal > previousVal) ? 1 : 0;
                break;
            }
            case DataTypeId::OMNI_LONG: {
                int64_t inputVal = reinterpret_cast<vec::Vector<int64_t>*>(originalVb->Get(colId))->GetValue(rowId);
                int64_t previousVal = *previousRow->getLong(colId);
                comparisonResult = (inputVal < previousVal) ? -1 : (inputVal > previousVal) ? 1 : 0;
                break;
            }
            case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DataTypeId::OMNI_TIMESTAMP: {
                int64_t currentMillseconds =
                    reinterpret_cast<vec::Vector<int64_t>*>(originalVb->Get(colId))->GetValue(rowId);
                TimestampData inputVal = TimestampData::fromEpochMillis(currentMillseconds);
                TimestampData previousVal = previousRow->getTimestamp(colId);
                if (inputVal.getMillisecond() == previousVal.getMillisecond()) {
                    comparisonResult = (inputVal.getNanoOfMillisecond() < previousVal.getNanoOfMillisecond())   ? -1
                                       : (inputVal.getNanoOfMillisecond() > previousVal.getNanoOfMillisecond()) ? 1
                                                                                                                : 0;
                } else {
                    comparisonResult = (inputVal.getMillisecond() < previousVal.getMillisecond()) ? -1 : 1;
                }
                break;
            }
                // Add other data types as needed.
            default:
                throw std::runtime_error("Unsupported DataTypeId for row comparison." + this->inputRowType->at(colId));
        }

        // Adjust the comparison result based on the sort order.
        if (comparisonResult != 0) {
            return ascending ? -comparisonResult : comparisonResult;
        }
    }

    // If all key columns are equal, return 0.
    return 0;
}
