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

#include "NamespaceAggsCountFunction.h"

#include <utility>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"

template <typename N>
NamespaceAggsCountFunction<N>::NamespaceAggsCountFunction(
    std::vector<int32_t> argIndexes,
    std::vector<int32_t> inputTypeIds,
    std::vector<int32_t> accIndexes,
    std::vector<int32_t> accTypeIds,
    int32_t aggValueIndex)
    : NamespaceAggsBasicFunction<N>(
          std::move(argIndexes),
          std::move(inputTypeIds),
          std::move(accIndexes),
          std::move(accTypeIds),
          aggValueIndex,
          omniruntime::type::DataTypeId::OMNI_LONG)
{
    this->isNull_ = false;
}

template <typename N>
void NamespaceAggsCountFunction<N>::accumulate(RowData* input)
{
    if (this->argIndexes_.empty() || !input->isNullAt(this->singleArgIndex())) {
        ++count_;
    }
}

template <typename N>
void NamespaceAggsCountFunction<N>::accumulate(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    for (auto combinedId : indices) {
        if (this->argIndexes_.empty()) {
            ++count_;
            continue;
        }

        int batchId = VectorBatchUtil::getBatchId(combinedId);
        int rowId = VectorBatchUtil::getRowId(combinedId);
        auto& targetBatch = inputBatches[batchId];
        if (!targetBatch->Get(this->singleArgIndex())->IsNull(rowId)) {
            ++count_;
        }
    }
}

template <typename N>
void NamespaceAggsCountFunction<N>::retract(RowData* input)
{
    if (this->argIndexes_.empty() || !input->isNullAt(this->singleArgIndex())) {
        --count_;
    }
}

template <typename N>
void NamespaceAggsCountFunction<N>::retract(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    for (auto combinedId : indices) {
        if (this->argIndexes_.empty()) {
            --count_;
            continue;
        }

        int batchId = VectorBatchUtil::getBatchId(combinedId);
        int rowId = VectorBatchUtil::getRowId(combinedId);
        auto& targetBatch = inputBatches[batchId];
        if (!targetBatch->Get(this->singleArgIndex())->IsNull(rowId)) {
            --count_;
        }
    }
}

template <typename N>
void NamespaceAggsCountFunction<N>::merge(N ns, RowData* otherAcc)
{
    const int32_t accIndex = this->singleAccIndex();
    if (!otherAcc->isNullAt(accIndex)) {
        count_ += *otherAcc->getLong(accIndex);
    }
}

template <typename N>
void NamespaceAggsCountFunction<N>::setAccumulators(N ns, RowData* acc)
{
    this->currentAcc_ = acc;
    const int32_t accIndex = this->singleAccIndex();
    count_ = acc->isNullAt(accIndex) ? 0L : *acc->getLong(accIndex);
}

template <typename N>
RowData* NamespaceAggsCountFunction<N>::getAccumulators()
{
    this->currentAcc_->setLong(this->singleAccIndex(), count_);
    return this->currentAcc_;
}

template <typename N>
void NamespaceAggsCountFunction<N>::updateAggValue(RowData* aggValue)
{
    if (this->aggValueIndex_ < 0) {
        return;
    }
    aggValue->setLong(this->aggValueIndex_, count_);
}

template class NamespaceAggsCountFunction<int64_t>;
template class NamespaceAggsCountFunction<TimeWindow>;
