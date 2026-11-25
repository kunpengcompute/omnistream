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

#include "GroupWindowAggsHandleFunction.h"

#include <type_traits>
#include <utility>

#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/utils/TimeWindowUtil.h"
#include "typeutils/BinaryRowDataSerializer.h"

template <typename N>
GroupWindowAggsHandleFunction<N>::GroupWindowAggsHandleFunction(
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<N>>> functions,
    std::vector<int32_t> aggValueTypeIds,
    std::vector<int32_t> windowPropertyTypeIds,
    std::vector<int32_t> outputValueTypeIds,
    int32_t accumulatorArity,
    std::string shiftTimeZone)
    : NamespaceAggsHandleFunction<N>(accumulatorArity),
      functions_(std::move(functions)),
      aggValueTypeIds_(std::move(aggValueTypeIds)),
      windowPropertyTypeIds_(std::move(windowPropertyTypeIds)),
      outputValueTypeIds_(std::move(outputValueTypeIds)),
      shiftTimeZone_(std::move(shiftTimeZone))
{
    if (outputValueTypeIds_.size() != windowPropertyTypeIds_.size() + aggValueTypeIds_.size()) {
        THROW_LOGIC_EXCEPTION(
            "Output value type ids size must be equal to window property type ids size plus agg "
            "value type ids size!");
    }
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::open(StateDataViewStore* store)
{
    NOT_IMPL_EXCEPTION;
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::setAccumulators(N namespaceVal, RowData* accumulators)
{
    currentAcc_ = accumulators;
    for (const auto& function : functions_) {
        function->setAccumulators(namespaceVal, accumulators);
    }
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::accumulate(RowData* inputRow)
{
    for (const auto& function : functions_) {
        function->accumulate(inputRow);
    }
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::retract(RowData* inputRow)
{
    for (const auto& function : functions_) {
        function->retract(inputRow);
    }
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::merge(N namespaceVal, RowData* otherAcc)
{
    for (const auto& function : functions_) {
        function->merge(namespaceVal, otherAcc);
    }
}

template <typename N>
RowData* GroupWindowAggsHandleFunction<N>::createAccumulators()
{
    auto* accumulators = BinaryRowData::createBinaryRowDataWithMem(this->accumulatorArity_);
    for (int32_t i = 0; i < this->accumulatorArity_; ++i) {
        accumulators->setNullAt(i);
    }
    return accumulators;
}

template <typename N>
RowData* GroupWindowAggsHandleFunction<N>::getAccumulators()
{
    for (const auto& function : functions_) {
        function->getAccumulators();
    }
    return currentAcc_;
}

template <typename N>
RowData* GroupWindowAggsHandleFunction<N>::getValue(N namespaceVal)
{
    auto* value = BinaryRowData::createBinaryRowDataWithMem(outputValueTypeIds_.size());

    for (const auto& function : functions_) {
        function->updateAggValue(value);
    }

    if constexpr (std::is_same_v<N, TimeWindow>) {
        const int64_t windowStart = static_cast<TimeWindow>(namespaceVal).getStart();
        const int64_t windowEnd = static_cast<TimeWindow>(namespaceVal).getEnd();
        // TODO: is there always 4 window property type ids?
        value->setLong(
            outputValueTypeIds_.size() - 4, convertWindowBoundaryTimestamp(windowStart, windowPropertyTypeIds_[0]));
        value->setLong(
            outputValueTypeIds_.size() - 3, convertWindowBoundaryTimestamp(windowEnd, windowPropertyTypeIds_[1]));
        value->setLong(
            outputValueTypeIds_.size() - 2, convertWindowBoundaryTimestamp(windowEnd - 1, windowPropertyTypeIds_[2]));
        value->setLong(outputValueTypeIds_.size() - 1, -1);
    } else {
        NOT_IMPL_EXCEPTION;
    }

    return value;
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::cleanup(N namespaceVal)
{
}

template <typename N>
void GroupWindowAggsHandleFunction<N>::close()
{
}

template <typename N>
int64_t GroupWindowAggsHandleFunction<N>::convertWindowBoundaryTimestamp(
    int64_t utcTimestampMills, int32_t propertyTypeId) const
{
    // Flink emits window start/end as TIMESTAMP wall-clock values.
    // Only TIMESTAMP_LTZ properties are stored as epoch millis.
    if (propertyTypeId == omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        return TimeWindowUtil::toEpochMills(utcTimestampMills, shiftTimeZone_);
    }
    return utcTimestampMills;
}

template class GroupWindowAggsHandleFunction<int64_t>;
template class GroupWindowAggsHandleFunction<TimeWindow>;
