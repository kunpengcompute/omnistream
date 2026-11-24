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

#ifndef FLINK_TNEL_TOP1COMPARATOR_H
#define FLINK_TNEL_TOP1COMPARATOR_H

#include <iostream>
#include <vector>
#include <queue>
#include <unordered_map>
#include <cassert>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/keyselector/KeySelector.h"

using namespace omnistream;

// Forward declaration for CompositeKeyRef
class CompositeKeyRef;

// ---------------------------------------------------------------------
// CompositeKeyRef: Represents a reference to a composite key in a vector batch.
// ---------------------------------------------------------------------
class CompositeKeyRef {
public:
    CompositeKeyRef(
        const std::vector<int>& columnIds,
        const std::vector<bool>& ascending,
        int rowId,
        omnistream::VectorBatch* vectorBatch)
        : columnIds_(columnIds),
          ascending_(ascending),
          rowId_(rowId),
          vectorBatch_(vectorBatch)
    {
    }

    // Comparison operator for heap sorting.
    __attribute__((noinline)) bool operator<(const CompositeKeyRef& other) const
    {
        ASSERT(columnIds_.size() == ascending_.size());
        ASSERT(columnIds_.size() == other.columnIds_.size());
        ASSERT(!columnIds_.empty());
        ASSERT(!ascending_.empty());
        ASSERT(vectorBatch_ != nullptr);
        ASSERT(other.vectorBatch_ != nullptr);

        for (size_t i = 0; i < columnIds_.size(); ++i) {
            int colId = columnIds_[i];

            DataTypeId dataTypeId = vectorBatch_->Get(colId)->GetTypeId();
            int64_t a = 0;
            int64_t b = 0;

            // Cast the vector and access the value for the current row.
            switch (dataTypeId) {
                case DataTypeId::OMNI_INT: {
                    auto* intVectorA = static_cast<omniruntime::vec::Vector<int32_t>*>(vectorBatch_->Get(colId));
                    auto* intVectorB = static_cast<omniruntime::vec::Vector<int32_t>*>(other.vectorBatch_->Get(colId));
                    a = static_cast<int64_t>(intVectorA->GetValue(rowId_));
                    b = static_cast<int64_t>(intVectorB->GetValue(other.rowId_));
                    break;
                }
                case DataTypeId::OMNI_LONG:
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP: {
                    auto* longVectorA = static_cast<omniruntime::vec::Vector<int64_t>*>(vectorBatch_->Get(colId));
                    auto* longVectorB = static_cast<omniruntime::vec::Vector<int64_t>*>(other.vectorBatch_->Get(colId));
                    a = longVectorA->GetValue(rowId_);
                    b = longVectorB->GetValue(other.rowId_);
                    break;
                }
                case DataTypeId::OMNI_DOUBLE: {
                    auto* doubleVectorA = static_cast<omniruntime::vec::Vector<double>*>(vectorBatch_->Get(colId));
                    auto* doubleVectorB =
                        static_cast<omniruntime::vec::Vector<double>*>(other.vectorBatch_->Get(colId));
                    a = static_cast<int64_t>(doubleVectorA->GetValue(rowId_));
                    b = static_cast<int64_t>(doubleVectorB->GetValue(other.rowId_));
                    break;
                }
                default: throw std::runtime_error("Unsupported DataTypeId for column comparison.");
            }

            if (a != b) {
                return ascending_[i] ? a < b : a > b;
            }
        }
        return false; // All keys are equal.
    }

    int getRowId() const
    {
        return rowId_;
    };

private:
    std::vector<int> columnIds_;
    std::vector<bool> ascending_;
    int rowId_;
    omnistream::VectorBatch* vectorBatch_;
};

// ---------------------------------------------------------------------
// Top1Comparator: Encapsulates logic to find the top-1 row ID for each partition.
// ---------------------------------------------------------------------
template <typename K>
class Top1Comparator {
public:
    Top1Comparator(
        const std::vector<int>& partitionKeyColTypes,
        const std::vector<int>& partitionKeyColId,
        const std::vector<int>& sortColumnIds,
        const std::vector<bool>& ascending)
        : partitionKeyColType_(partitionKeyColTypes),
          partitionKeyColId_(partitionKeyColId),
          sortColumnIds_(sortColumnIds),
          ascending_(ascending),
          keySelector_(partitionKeyColType_, partitionKeyColId_)
    {
    }

    struct CompositeKeyComparator {
        bool operator()(const CompositeKeyRef& a, const CompositeKeyRef& b) const
        {
            return a < b;
        }
    };

    // Method to find the top-1 row IDs for each partition.
    std::unordered_map<K, int> findTop1RowIdsByPartition(omnistream::VectorBatch* vectorBatch)
    {
        LOG("findTop1RowIdsByPartition is called.");
        ASSERT(vectorBatch != nullptr);
        // Extract the top-1 row ID for each partition key.
        std::unordered_map<K, int> top1RowIds;

        if (vectorBatch->GetVectorCount() == 0) {
            return top1RowIds;
        }

        // Map to store heaps for each partition key.
        std::
            unordered_map<K, std::priority_queue<CompositeKeyRef, std::vector<CompositeKeyRef>, CompositeKeyComparator>>
                partitionHeaps;

        size_t rowCount = vectorBatch->GetRowCount();

        // Populate the heaps.
        for (size_t rowId = 0; rowId < rowCount; ++rowId) {
            // Use keySelector to get key
            K key = keySelector_.getKey(vectorBatch, static_cast<int>(rowId));
            CompositeKeyRef keyRef(sortColumnIds_, ascending_, static_cast<int>(rowId), vectorBatch);
            auto it = partitionHeaps.find(key);
            if (it != partitionHeaps.end()) {
                it->second.push(keyRef);
                if constexpr (std::is_same<K, RowData*>::value) {
                    delete key;
                }
            } else {
                partitionHeaps[key].push(keyRef);
            }
        }

        for (auto& [k, heap] : partitionHeaps) {
            top1RowIds[k] = heap.top().getRowId();
        }

        return top1RowIds;
    }

    std::unordered_map<K, int> findTop1RowIdsByPartitionV2(omnistream::VectorBatch* vectorBatch)
    {
        LOG("findTop1RowIdsByPartition is called.");
        ASSERT(vectorBatch != nullptr);
        // Extract the top-1 row ID for each partition key.
        std::unordered_map<K, int> top1RowIds;

        if (vectorBatch->GetVectorCount() == 0) {
            return top1RowIds;
        }

        // Map to store heaps for each partition key.
        std::unordered_map<K, CompositeKeyRef> partitionHeaps;

        size_t rowCount = vectorBatch->GetRowCount();

        // Populate the heaps.
        for (size_t rowId = 0; rowId < rowCount; ++rowId) {
            // Use keySelector to get key
            K key = keySelector_.getKey(vectorBatch, static_cast<int>(rowId));
            CompositeKeyRef keyRef(sortColumnIds_, ascending_, static_cast<int>(rowId), vectorBatch);
            auto it = partitionHeaps.find(key);
            if (it != partitionHeaps.end()) {
                if (keyRef < it->second) {
                    it->second = keyRef;
                }
                if constexpr (std::is_same<K, RowData*>::value) {
                    delete key;
                }
            } else {
                partitionHeaps.emplace(key, keyRef);
            }
        }

        for (auto& [k, heap] : partitionHeaps) {
            top1RowIds[k] = heap.getRowId();
        }

        return top1RowIds;
    }

private:
    std::vector<int> partitionKeyColType_;
    std::vector<int> partitionKeyColId_; // Column index for the partition key.
    std::vector<int> sortColumnIds_;     // Columns to use for sorting.
    std::vector<bool> ascending_;        // Sorting order for each column.
    KeySelector<K> keySelector_;
};

#endif // FLINK_TNEL_TOP1COMPARATOR_H
