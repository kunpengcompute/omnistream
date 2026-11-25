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

#ifndef FLINK_TNEL_SHARED_DISTINCT_COUNT_CONTAINER_FUNCTION_H
#define FLINK_TNEL_SHARED_DISTINCT_COUNT_CONTAINER_FUNCTION_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <unordered_map>
#include <vector>
#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
#include "../runtime/state/VoidNamespace.h"

class SharedDistinctCountContainerFunction : public AggsHandleFunction {
public:
    explicit SharedDistinctCountContainerFunction(std::string stateName = "distinct_acc_shared");

    void addDistinctEntry(
        int aggFuncIndex, const std::string& aggType, int aggIdx, int filterIndex, const std::string& inputType);
    void finalizeEntries();
    void bindEntryAccValueIndex(int aggFuncIndex, int accIndex, int valueIndex);

    void setWindowSize(int windowSize) override
    {
    }
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;
    void open(StateDataViewStore* store);
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void retract(RowData* retractInput) override
    {
    }
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override
    {
    }
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    void cleanup() override;
    void close() override;
    void setCurrentGroupKey(RowData* key) override;
    void updateInnerState() override;

    void bindAccValueIndex(int accStartIndex, int valueStartIndex) override;
    int accumulatorSlots() const override
    {
        return static_cast<int>(entries_.size());
    }
    bool hasAggOutput() const override
    {
        return !entries_.empty();
    }

private:
    struct DistinctEntry {
        int aggFuncIndex = -1;
        std::string aggType;
        int aggIdx = -1;
        int filterIndex = -1;
        std::string inputType;
        omniruntime::type::DataTypeId typeId = omniruntime::type::DataTypeId::OMNI_LONG;
        std::size_t groupIndex = 0;
        std::uint8_t bitOffset = 0;
        int accIndex = -1;
        int valueIndex = -1;
        long aggCount = -1L;
        bool valueIsNull = true;
    };

    using PendingDistinctUpdates = std::unordered_map<RowData*, std::vector<std::tuple<long, long>>>;
    struct DistinctGroup {
        int aggIdx = -1;
        omniruntime::type::DataTypeId typeId = omniruntime::type::DataTypeId::OMNI_LONG;
        std::string stateName;
        std::vector<DistinctEntry*> groupEntries;
        KeyedStateMapViewWithKeysNullable<VoidNamespace, long, long>* distinctMapView = nullptr;
        PendingDistinctUpdates pendingDistinctUpdates;
    };

    bool shouldAccumulateForEntry(DistinctEntry* entry, RowData* inputRow) const;
    std::uint64_t collectCandidateMask(RowData* inputRow, const DistinctGroup& group) const;
    void applyMaskDelta(std::size_t groupIndex, std::uint64_t deltaMask);
    long getRowFieldValue(RowData* row, int aggIdx, omniruntime::type::DataTypeId typeId, bool& isNull) const;
    void getOrCreateGroup(DistinctEntry* entry);

    std::string stateName_;
    std::vector<DistinctEntry*> entries_;
    std::vector<DistinctGroup> groups_;
    std::unordered_map<int, int> entryIndexToAggFuncIndex;
    std::unordered_map<int, int> distinctGroupMap;
    bool finalized_ = false;
    StateDataViewStore* store_ = nullptr;
    RowData* currentGroupKey_ = nullptr;
    long getRowFieldValueFromVB(
        omnistream::VectorBatch* input,
        int columnIdx,
        int rowIdx,
        omniruntime::type::DataTypeId typeId,
        bool& isNull) const;
};

#endif // FLINK_TNEL_SHARED_DISTINCT_COUNT_CONTAINER_FUNCTION_H
