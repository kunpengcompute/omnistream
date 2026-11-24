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

#include <cstdint>
#include <vector>
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/data/util/VectorBatchUtil.h"

class RowData;
class StateDataViewStore;

template <typename N>
class NamespaceAggsBasicFunction {
public:
    NamespaceAggsBasicFunction(
        std::vector<int32_t> argIndexes,
        std::vector<int32_t> inputTypeIds,
        std::vector<int32_t> accIndexes,
        std::vector<int32_t> accTypeIds,
        int32_t aggValueIndex,
        int32_t aggValueTypeId);
    virtual ~NamespaceAggsBasicFunction() = default;

    virtual void accumulate(RowData* input) = 0;
    virtual void retract(RowData* input) = 0;
    virtual void accumulate(
        const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices) = 0;
    virtual void retract(
        const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices) = 0;
    virtual void merge(N ns, RowData* otherAcc) = 0;
    virtual void setAccumulators(N ns, RowData* acc) = 0;
    virtual RowData* getAccumulators() = 0;
    virtual void updateAggValue(RowData* aggValue) = 0;

protected:
    int32_t singleArgIndex() const;
    int32_t singleAccIndex() const;
    int64_t readInputByIndex(RowData* input, int32_t index) const;
    int64_t readInputByIndex(omnistream::VectorBatch* input, int32_t argIndex, int rowId) const;

    std::vector<int32_t> argIndexes_;
    std::vector<int32_t> inputTypeIds_;
    std::vector<int32_t> accIndexes_;
    std::vector<int32_t> accTypeIds_;
    int32_t aggValueIndex_;
    int32_t aggValueTypeId_;
    StateDataViewStore* store_{};
    RowData* currentAcc_{};
    bool isNull_ = true;
};
