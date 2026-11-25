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

#include "NamespaceAggsSumFunction.h"

#include <utility>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

template <typename N>
NamespaceAggsSumFunction<N>::NamespaceAggsSumFunction(
    std::vector<int32_t> argIndexes,
    std::vector<int32_t> inputTypeIds,
    std::vector<int32_t> accIndexes,
    std::vector<int32_t> accTypeIds,
    int32_t aggValueIndex,
    int32_t aggValueTypeId)
    : NamespaceAggsBasicFunction<N>(
          std::move(argIndexes),
          std::move(inputTypeIds),
          std::move(accIndexes),
          std::move(accTypeIds),
          aggValueIndex,
          aggValueTypeId)
{
}

template <typename N>
void NamespaceAggsSumFunction<N>::accumulate(RowData* input)
{
    const int32_t argIndex = this->singleArgIndex();
    if (!input->isNullAt(argIndex)) {
        sum_ += this->readInputByIndex(input, argIndex);
        this->isNull_ = false;
    }
}

template <typename N>
void NamespaceAggsSumFunction<N>::accumulate(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    const int32_t argIndex = this->singleArgIndex();
    for (auto combinedId : indices) {
        int batchId = VectorBatchUtil::getBatchId(combinedId);
        int rowId = VectorBatchUtil::getRowId(combinedId);
        auto& targetBatch = inputBatches[batchId];
        if (!targetBatch->Get(argIndex)->IsNull(rowId)) {
            sum_ += this->readInputByIndex(targetBatch, argIndex, rowId);
            this->isNull_ = false;
        }
    }
}

template <typename N>
void NamespaceAggsSumFunction<N>::retract(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    const int32_t argIndex = this->singleArgIndex();
    for (auto combinedId : indices) {
        int batchId = VectorBatchUtil::getBatchId(combinedId);
        int rowId = VectorBatchUtil::getRowId(combinedId);
        auto& targetBatch = inputBatches[batchId];
        if (!targetBatch->Get(argIndex)->IsNull(rowId)) {
            sum_ -= this->readInputByIndex(targetBatch, argIndex, rowId);
        }
    }
}

template <typename N>
void NamespaceAggsSumFunction<N>::retract(RowData* input)
{
    const int32_t argIndex = this->singleArgIndex();
    if (!input->isNullAt(argIndex)) {
        sum_ -= this->readInputByIndex(input, argIndex);
    }
}

template <typename N>
void NamespaceAggsSumFunction<N>::merge(N ns, RowData* otherAcc)
{
    const int32_t accIndex = this->singleAccIndex();
    if (!otherAcc->isNullAt(accIndex)) {
        sum_ += *otherAcc->getLong(accIndex);
        this->isNull_ = false;
    }
}

template <typename N>
void NamespaceAggsSumFunction<N>::setAccumulators(N ns, RowData* acc)
{
    this->currentAcc_ = acc;
    const int32_t accIndex = this->singleAccIndex();
    this->isNull_ = acc->isNullAt(accIndex);
    sum_ = this->isNull_ ? 0L : *acc->getLong(accIndex);
}

template <typename N>
RowData* NamespaceAggsSumFunction<N>::getAccumulators()
{
    const int32_t accIndex = this->singleAccIndex();
    if (this->isNull_) {
        static_cast<BinaryRowData*>(this->currentAcc_)->setNullAt(accIndex);
    } else {
        this->currentAcc_->setLong(accIndex, sum_);
    }
    return this->currentAcc_;
}

template <typename N>
void NamespaceAggsSumFunction<N>::updateAggValue(RowData* aggValue)
{
    if (this->isNull_) {
        static_cast<BinaryRowData*>(aggValue)->setNullAt(this->aggValueIndex_);
    } else {
        aggValue->setLong(this->aggValueIndex_, sum_);
    }
}

template class NamespaceAggsSumFunction<int64_t>;
template class NamespaceAggsSumFunction<TimeWindow>;
