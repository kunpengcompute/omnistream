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

#include "CountFunction.h"

bool CountFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    if (r1->isNullAt(valueIndex) || r2->isNullAt(valueIndex)) {
        return false;
    }
    bool isEqual = false;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            isEqual = *r1->getInt(valueIndex) == *r2->getInt(valueIndex);
            break;
        }

        case DataTypeId::OMNI_LONG: {
            isEqual = *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
    return isEqual;
}

void CountFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void CountFunction::accumulate(RowData* accInput)
{
    if (isCountStar) {
        if (!valueIsNull) {
            aggCount++;
        }
        return;
    }
    bool shouldDoAccumulate = true;
    if (hasFilter) {
        bool isFilterNull = accInput->isNullAt(filterIndex);
        shouldDoAccumulate = !isFilterNull && *(accInput->getBool(filterIndex));
    }
    if (shouldDoAccumulate) {
        if (aggIdx == -1 && valueIndex == -1) {
            aggCount++;
            valueIsNull = false;
        } else {
            bool isFieldNull = accInput->isNullAt(aggIdx);
            if (!isFieldNull) {
                if (!valueIsNull) {
                    // input data not null and aggCount not null, aggCount++
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
            }
        }
        LOG("Accumulate. Count:  " << aggCount << " countIsNull: " << valueIsNull);
    }
}

void CountFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    const bool hasFilterCol = hasFilter;
    const auto filterData =
        hasFilterCol ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex)) : nullptr;

    // aggIdx is -1 for count(*) even when isCountStar was never set -- the row branch below
    // treats "isCountStar || aggIdx == -1" as count-star, so guard the fetch the same way.
    // Get(-1) indexes before the vector array; it only faults when that memory happens to be
    // unmapped, which is why q17 ran (with a bad read) under some allocator configurations and
    // segfaulted under others.
    omniruntime::vec::BaseVector* columnData = nullptr;
    if (!isCountStar && aggIdx >= 0) {
        columnData = input->Get(aggIdx);
    }

    for (int rowIndex : indices) {
        bool shouldDoAccumulate = true;

        // Apply filter if needed
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
        }

        if (!shouldDoAccumulate) continue;
        if (isCountStar || aggIdx == -1) {
            if (aggCount == -1) {
                aggCount = 0;
            }
            aggCount++;
            valueIsNull = false;
        } else {
            bool isFieldNull = columnData->IsNull(rowIndex);
            if (!isFieldNull) {
                if (!valueIsNull) {
                    aggCount++;
                } else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
            }
        }
    }

    LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
}

void CountFunction::retract(RowData* retractInput)
{
    if (isCountStar) {
        aggCount = !valueIsNull ? aggCount - 1 : aggCount;
        return;
    }
    bool isFieldNull = retractInput->isNullAt(aggIdx);
    if (!isFieldNull) {
        aggCount = !valueIsNull ? aggCount - 1 : aggCount;
    }
}

void CountFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    // Same guard as accumulate: aggIdx == -1 means count-star, and Get(-1) is out of bounds.
    // Also initialise columnData -- it was left uninitialised when the fetch was skipped.
    omniruntime::vec::BaseVector* columnData = nullptr;
    if (!isCountStar && aggIdx >= 0) {
        columnData = input->Get(aggIdx);
    }
    for (int rowIndex : indices) {
        if (isCountStar || aggIdx == -1) {
            aggCount = aggCount != -1 ? aggCount - 1 : aggCount;
            valueIsNull = false;
        } else {
            bool isFieldNull = columnData->IsNull(rowIndex);
            if (!isFieldNull) {
                aggCount = !valueIsNull ? aggCount - 1 : -1L;
                valueIsNull = false;
            }
        }
    }
}

void CountFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("This function does not require merge method, but the merge method is called.");
}

void CountFunction::setAccumulators(RowData* acc)
{
    valueIsNull = acc->isNullAt(accIndex); // true
    aggCount = valueIsNull ? -1L : *acc->getLong(accIndex);
    LOG("Set Acc. Count:  " << aggCount << " countIsNull: " << valueIndex);
}

void CountFunction::resetAccumulators()
{
    aggCount = (static_cast<long>(0L));
    valueIsNull = false;
    LOG("Reset Acc. Count:  " << aggCount << " countIsNull: " << valueIsNull);
}

void CountFunction::getAccumulators(BinaryRowData* accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggCount);
    }
    LOG("Get acc: " << aggCount);
}

void CountFunction::createAccumulators(BinaryRowData* accumulators)
{
    if (false) { // This condition is always false, but it's in the original code.
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, 0L);
    }
    LOG("Create Count acc");
}

void CountFunction::getValue(BinaryRowData* newAggValue)
{
    LOG("newAggValue->getArity : " << newAggValue->getArity());
    LOG("valueIndex : " << valueIndex);
    if (valueIndex != -1) {
        if (valueIsNull) {
            LOG("setNullAt ");
            newAggValue->setNullAt(valueIndex);
        } else {
            LOG("setLong aggCount :" << aggCount);
            newAggValue->setLong(valueIndex, aggCount);
        }
        LOG("Get value: " << aggCount);
    }
}

void CountFunction::setCountStart(bool isCountStartFunc)
{
    this->isCountStar = isCountStartFunc;
}
