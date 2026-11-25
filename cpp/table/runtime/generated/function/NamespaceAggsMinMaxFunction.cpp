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

#include "NamespaceAggsMinMaxFunction.h"

#include <stdexcept>
#include <utility>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/runtime/generated/NamespaceAggsBasicFunctionFactory.h"

template <typename N>
NamespaceAggsMinMaxFunction<N>::NamespaceAggsMinMaxFunction(
    std::vector<int32_t> argIndexes,
    std::vector<int32_t> inputTypeIds,
    std::vector<int32_t> accIndexes,
    std::vector<int32_t> accTypeIds,
    int32_t aggValueIndex,
    int32_t aggValueTypeId,
    NamespaceAggsBasicFunctionType type)
    : NamespaceAggsBasicFunction<N>(
          std::move(argIndexes),
          std::move(inputTypeIds),
          std::move(accIndexes),
          std::move(accTypeIds),
          aggValueIndex,
          aggValueTypeId),
      type_(type)
{
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::accumulate(RowData* input)
{
    const int32_t argIndex = this->singleArgIndex();
    if (!input->isNullAt(argIndex)) {
        update(this->readInputByIndex(input, argIndex));
    }
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::accumulate(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    const int32_t argIndex = this->singleArgIndex();
    for (auto combinedId : indices) {
        int batchId = VectorBatchUtil::getBatchId(combinedId);
        int rowId = VectorBatchUtil::getRowId(combinedId);
        auto& targetBatch = inputBatches[batchId];
        if (!targetBatch->Get(argIndex)->IsNull(rowId)) {
            update(this->readInputByIndex(targetBatch, argIndex, rowId));
        }
    }
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::retract(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    THROW_RUNTIME_ERROR("Retract is not supported for MIN/MAX.");
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::retract(RowData* input)
{
    THROW_RUNTIME_ERROR("Retract is not supported for MIN/MAX.");
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::merge(N ns, RowData* otherAcc)
{
    const int32_t accIndex = this->singleAccIndex();
    if (!otherAcc->isNullAt(accIndex)) {
        update(*otherAcc->getLong(accIndex));
    }
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::setAccumulators(N ns, RowData* acc)
{
    this->currentAcc_ = acc;
    const int32_t accIndex = this->singleAccIndex();
    this->isNull_ = acc->isNullAt(accIndex);
    if (!this->isNull_) {
        value_ = *acc->getLong(accIndex);
    }
}

template <typename N>
RowData* NamespaceAggsMinMaxFunction<N>::getAccumulators()
{
    int32_t accIndex = this->singleAccIndex();
    if (this->isNull_) {
        static_cast<BinaryRowData*>(this->currentAcc_)->setNullAt(accIndex);
    } else {
        this->currentAcc_->setLong(accIndex, value_);
    }
    return this->currentAcc_;
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::updateAggValue(RowData* aggValue)
{
    if (this->isNull_) {
        static_cast<BinaryRowData*>(aggValue)->setNullAt(this->aggValueIndex_);
    } else {
        aggValue->setLong(this->aggValueIndex_, value_);
    }
}

template <typename N>
void NamespaceAggsMinMaxFunction<N>::update(int64_t candidate)
{
    if (this->isNull_ || (type_ == NamespaceAggsBasicFunctionType::MIN ? candidate < value_ : candidate > value_)) {
        value_ = candidate;
        this->isNull_ = false;
    }
}

template class NamespaceAggsMinMaxFunction<int64_t>;
template class NamespaceAggsMinMaxFunction<TimeWindow>;
