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

#include "NamespaceAggsBasicFunction.h"

#include <stdexcept>
#include <utility>

#include "table/runtime/operators/window/TimeWindow.h"
#include "table/data/RowData.h"

template <typename N>
NamespaceAggsBasicFunction<N>::NamespaceAggsBasicFunction(
    std::vector<int32_t> argIndexes,
    std::vector<int32_t> inputTypeIds,
    std::vector<int32_t> accIndexes,
    std::vector<int32_t> accTypeIds,
    int32_t aggValueIndex,
    int32_t aggValueTypeId)
    : argIndexes_(std::move(argIndexes)),
      inputTypeIds_(std::move(inputTypeIds)),
      accIndexes_(std::move(accIndexes)),
      accTypeIds_(std::move(accTypeIds)),
      aggValueIndex_(aggValueIndex),
      aggValueTypeId_(aggValueTypeId)
{
}

template <typename N>
int32_t NamespaceAggsBasicFunction<N>::singleArgIndex() const
{
    if (argIndexes_.size() != 1) {
        THROW_LOGIC_EXCEPTION("The aggregate function requires exactly one input argument.");
    }
    return argIndexes_[0];
}

template <typename N>
int32_t NamespaceAggsBasicFunction<N>::singleAccIndex() const
{
    if (accIndexes_.size() != 1) {
        THROW_LOGIC_EXCEPTION("The aggregate function requires exactly one accumulate argument.");
    }
    return accIndexes_[0];
}

template <typename N>
int64_t NamespaceAggsBasicFunction<N>::readInputByIndex(RowData* input, int32_t index) const
{
    switch (inputTypeIds_.at(index)) {
        case omniruntime::type::DataTypeId::OMNI_INT: return *input->getInt(index);
        case omniruntime::type::DataTypeId::OMNI_LONG:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: return *input->getLong(index);
        default: THROW_RUNTIME_ERROR("The data type is not supported: " << inputTypeIds_.at(index));
    }
}

template <typename N>
int64_t NamespaceAggsBasicFunction<N>::readInputByIndex(
    omnistream::VectorBatch* input, int32_t argIndex, int rowId) const
{
    switch (inputTypeIds_.at(argIndex)) {
        case omniruntime::type::DataTypeId::OMNI_INT: {
            auto val = input->GetValueAt<int32_t>(argIndex, rowId);
            return static_cast<int64_t>(val);
        }
        case omniruntime::type::DataTypeId::OMNI_LONG:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP:
        case omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            return input->GetValueAt<int64_t>(argIndex, rowId);
        }
        default: THROW_RUNTIME_ERROR("The data type is not supported: " << inputTypeIds_.at(argIndex));
    }
}

template class NamespaceAggsBasicFunction<int64_t>;
template class NamespaceAggsBasicFunction<TimeWindow>;
