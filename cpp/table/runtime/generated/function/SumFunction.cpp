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

#include "SumFunction.h"

SumFunction::SumFunction(int aggIdx, std::string inputType, int accIndex, int valueIndex, int filterIndex)
    : aggIdx(aggIdx),
      inputType(inputType),
      accIndex(accIndex),
      valueIndex(valueIndex),
      filterIndex(filterIndex)
{
    hasFilter = filterIndex != -1;
    store = nullptr;
    accIndexCount0 = -1;
}

void SumFunction::accumulate(RowData* accInput)
{
    bool shouldDoAccumulate = true;
    if (hasFilter) {
        bool isFilterNull = accInput->isNullAt(filterIndex);
        shouldDoAccumulate = !isFilterNull && *(accInput->getBool(filterIndex));
    }
    if (shouldDoAccumulate) {
        auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        bool isFieldNull = accInput->isNullAt(aggIdx);
        long fieldValue;
        switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue = isFieldNull ? -1L : *accInput->getInt(aggIdx);
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue = isFieldNull ? -1L : *accInput->getLong(aggIdx);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }

        if (!isFieldNull) {
            if (sumIsNull) {
                sum = fieldValue;
                sumIsNull = false;
            } else {
                sum += fieldValue;
            }
            if (consumeRetraction) {
                count0IsNull = false;
                count0 += 1;
            }
        }
    }
}

void SumFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);
    const bool hasFilterCol = hasFilter;
    const auto filterData =
        hasFilterCol ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex)) : nullptr;

    for (int rowIndex : indices) {
        bool isFilter = true;
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            isFilter = !isFilterNull && filterData->GetValue(rowIndex);
        }

        if (!isFilter) continue;

        bool isFieldNull = columnData->IsNull(rowIndex);
        long fieldValue;
        auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue =
                    isFieldNull ? -1L : dynamic_cast<omniruntime::vec::Vector<int>*>(columnData)->GetValue(rowIndex);
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue =
                    isFieldNull ? -1L : dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }

        if (!isFieldNull) {
            if (sumIsNull) {
                sum = fieldValue;
                sumIsNull = false;
            } else {
                sum += fieldValue;
            }
        }
    }
}

void SumFunction::setAccumulators(RowData* _acc)
{
    sumIsNull = _acc->isNullAt(accIndex);
    sum = sumIsNull ? 0L : *_acc->getLong(accIndex);
    if (consumeRetraction) {
        count0 = count0IsNull ? 0L : *_acc->getLong(accIndexCount0);
    }
}

void SumFunction::resetAccumulators()
{
    sum = 0;
    sumIsNull = true;
    if (consumeRetraction) {
        count0 = 0;
        count0IsNull = true;
    }
}

void SumFunction::createAccumulators(BinaryRowData* accumulators)
{
    accumulators->setNullAt(accIndex);
    if (consumeRetraction) {
        accumulators->setNullAt(accIndexCount0);
    }
}

void SumFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void SumFunction::retract(RowData* retractInput)
{
    auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
    bool isFieldNull = retractInput->isNullAt(aggIdx);
    long fieldValue;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            fieldValue = isFieldNull ? -1L : *retractInput->getInt(aggIdx);
            break;
        }
        case DataTypeId::OMNI_LONG: {
            fieldValue = isFieldNull ? -1L : *retractInput->getLong(aggIdx);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }

    if (!isFieldNull) {
        sum = sumIsNull ? sum : sum - fieldValue;
        if (consumeRetraction) {
            count0 -= 1;
        }
    }
}

void SumFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);

    auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);

    for (int rowIndex : indices) {
        bool isFieldNull = columnData->IsNull(rowIndex);
        long fieldValue;

        switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue =
                    isFieldNull ? -1L : dynamic_cast<omniruntime::vec::Vector<int>*>(columnData)->GetValue(rowIndex);
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue =
                    isFieldNull ? -1L : dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }

        if (!isFieldNull) {
            sum = sumIsNull ? sum : sum - fieldValue;
        }
    }
}

void SumFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("This function does not require the merge method, but the merge method is called.");
}

void SumFunction::getAccumulators(BinaryRowData* acc)
{
    if (sumIsNull) {
        acc->setNullAt(accIndex);
    } else {
        acc->setLong(accIndex, sum);
    }

    if (consumeRetraction) {
        if (count0IsNull) {
            acc->setNullAt(accIndexCount0);
        } else {
            acc->setLong(accIndexCount0, count0);
        }
    }
}

void SumFunction::getValue(BinaryRowData* aggValue)
{
    if (sumIsNull) {
        aggValue->setNullAt(valueIndex);
    } else {
        aggValue->setLong(valueIndex, sum);
    }
}

bool SumFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    if (r1->isNullAt(valueIndex) || r2->isNullAt(valueIndex)) {
        return false;
    }
    bool isEqual = false;
    auto typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
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

void SumFunction::setRetraction(int accIndexCount0Index)
{
    this->accIndexCount0 = accIndexCount0Index;
    if (accIndexCount0Index >= 0) {
        consumeRetraction = true;
    }
}
