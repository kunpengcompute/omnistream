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

#include "AverageFunction.h"

AverageFunction::AverageFunction(
    int aggIdx, std::string inputType, int accIndexSum, int accIndexCount0, int valueIndex, int filterIndex)
    : aggIdx(aggIdx),
      accIndexSum(accIndexSum),
      accIndexCount0(accIndexCount0),
      valueIndex(valueIndex),
      filterIndex(filterIndex)
{
    typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
    hasFilter = filterIndex != -1;
    store = nullptr;
}

void AverageFunction::accumulate(RowData* accInput)
{
    bool isFilter = true;
    if (hasFilter) {
        bool isFilterNull = accInput->isNullAt(filterIndex);
        isFilter = !isFilterNull && *(accInput->getBool(filterIndex));
    }
    if (isFilter) {
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

        // Update agg0_sum
        if (!isFieldNull) {
            if (sumIsNull) {
                sum = fieldValue;
                sumIsNull = false;
            } else {
                sum += fieldValue;
            }

            if (count0IsNull) {
                count0 = 1;
                count0IsNull = false;
            } else {
                count0 += 1;
            }
        }
    }
}

void AverageFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
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

        // Update agg0_sum
        if (!isFieldNull) {
            if (sumIsNull) {
                sum = fieldValue;
                sumIsNull = false;
            } else {
                sum += fieldValue;
            }

            if (count0IsNull) {
                count0 = 1;
                count0IsNull = false;
            } else {
                count0 += 1;
            }
        }
    }
}

void AverageFunction::setAccumulators(RowData* _acc)
{
    sumIsNull = _acc->isNullAt(accIndexSum);
    sum = sumIsNull ? 0L : *_acc->getLong(accIndexSum);

    count0IsNull = _acc->isNullAt(accIndexCount0);
    count0 = count0IsNull ? 0L : *_acc->getLong(accIndexCount0);
}

void AverageFunction::resetAccumulators()
{
    sum = 0;
    sumIsNull = false;

    count0 = 0;
    count0IsNull = false;
}

void AverageFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

void AverageFunction::createAccumulators(BinaryRowData* accumulators)
{
    accumulators->setLong(accIndexSum, 0L);
    accumulators->setLong(accIndexCount0, 0L);
}

void AverageFunction::retract(RowData* retractInput)
{
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

    // Update agg0_sum
    if (!isFieldNull) {
        sum = sumIsNull ? sum : sum - fieldValue;
        count0 = count0IsNull ? count0 : count0 - 1;
    }
}

void AverageFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);

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

        // Update agg0_sum and count0
        if (!isFieldNull) {
            sum = sumIsNull ? sum : sum - fieldValue;
            count0 = count0IsNull ? count0 : count0 - 1;
        }
    }
}

void AverageFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("This function does not require the merge method, but the merge method is called.");
}

void AverageFunction::getAccumulators(BinaryRowData* acc)
{
    if (sumIsNull) {
        acc->setNullAt(accIndexSum);
    } else {
        acc->setLong(accIndexSum, sum);
    }

    if (count0IsNull) {
        acc->setNullAt(accIndexCount0);
    } else {
        acc->setLong(accIndexCount0, count0);
    }
}

void AverageFunction::getValue(BinaryRowData* aggValue)
{
    if (count0IsNull || count0 == 0 || sumIsNull) {
        aggValue->setNullAt(valueIndex);
    } else {
        long average = sum / count0;
        aggValue->setLong(valueIndex, average);
    }
}

bool AverageFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
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
