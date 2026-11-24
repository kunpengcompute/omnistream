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

#include "WindowAggsHandleFunction.h"

#include <utility>

#include "common.h"
#include "table/data/binary/BinaryRowData.h"

WindowAggsHandleFunction::WindowAggsHandleFunction(
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> functions,
    std::vector<int32_t> aggValueTypeIds,
    std::vector<int32_t> outputValueTypeIds,
    SliceAssigner* sliceAssigner,
    int32_t accumulatorArity)
    : NamespaceAggsHandleFunction<int64_t>(accumulatorArity),
      aggValueTypeIds_(std::move(aggValueTypeIds)),
      outputValueTypeIds_(std::move(outputValueTypeIds)),
      sliceAssigner_(sliceAssigner),
      functions_(std::move(functions))
{
}

void WindowAggsHandleFunction::open(StateDataViewStore* store)
{
}

void WindowAggsHandleFunction::accumulate(RowData* accInput)
{
    for (auto& func : functions_) {
        func->accumulate(accInput);
    }
}

// local window Agg optimization
void WindowAggsHandleFunction::accumulate(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    for (auto& func : functions_) {
        func->accumulate(inputBatches, indices);
    }
}

void WindowAggsHandleFunction::retract(
    const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices)
{
    for (auto& func : functions_) {
        func->retract(inputBatches, indices);
    }
}

void WindowAggsHandleFunction::retract(RowData* retractInput)
{
    for (auto& func : functions_) {
        func->retract(retractInput);
    }
}

void WindowAggsHandleFunction::merge(int64_t ns, RowData* otherAcc)
{
    for (auto& func : functions_) {
        func->merge(ns, otherAcc);
    }
}

void WindowAggsHandleFunction::setAccumulators(int64_t ns, RowData* acc)
{
    currentAcc_ = acc;
    for (auto& func : functions_) {
        func->setAccumulators(ns, acc);
    }
}

RowData* WindowAggsHandleFunction::getAccumulators()
{
    for (const auto& function : functions_) {
        function->getAccumulators();
    }
    return currentAcc_;
}

RowData* WindowAggsHandleFunction::createAccumulators()
{
    auto* newAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity_);
    for (int32_t i = 0; i < accumulatorArity_; ++i) {
        newAcc->setNullAt(i);
    }
    return newAcc;
}

RowData* WindowAggsHandleFunction::getValue(int64_t ns)
{
    auto* res = BinaryRowData::createBinaryRowDataWithMem(this->outputValueTypeIds_.size());
    for (auto& func : functions_) {
        func->updateAggValue(res);
    }
    res->setLong(this->outputValueTypeIds_.size() - 2, sliceAssigner_->getWindowStart(ns));
    res->setLong(this->outputValueTypeIds_.size() - 1, ns);
    return res;
}

void WindowAggsHandleFunction::cleanup(int64_t ns)
{
}

void WindowAggsHandleFunction::close()
{
}
