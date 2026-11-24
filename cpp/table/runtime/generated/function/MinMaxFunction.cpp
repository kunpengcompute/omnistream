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

#include "MinMaxFunction.h"
#include <stdexcept>
#include <iostream>
#include <common.h>
#include "table/data/binary/BinaryRowData.h"

void MinMaxFunction::accumulate(RowData* accInput)
{
    bool isFilter = true;
    if (hasFilter) {
        bool isFilterNull = accInput->isNullAt(filterIndex);
        isFilter = !isFilterNull && *(accInput->getBool(filterIndex));
    }
    if (isFilter) {
        switch (typeId) {
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_LONG: {
                accumulateLong(accInput);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                accumulateString(accInput);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }
    }
}

void MinMaxFunction::accumulateLong(RowData* accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    long fieldValue = isFieldNull ? limit : *accInput->getLong(aggIdx);

    if (isFieldNull) {
        aggValue = valueIsNull ? limit : aggValue;
    } else {
        bool toUpdate = !valueIsNull && (aggOperator == MAX_FUNC ? fieldValue > aggValue : fieldValue < aggValue);
        if (valueIsNull || toUpdate) {
            aggValue = fieldValue;
            valueIsNull = false;
        } else {
            valueIsNull = valueIsNull;
        }
    }
}

void MinMaxFunction::accumulateString(RowData* accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    std::string fieldValue = isFieldNull ? stringAggValueLimit : std::string(accInput->getStringView(aggIdx));

    if (isFieldNull) {
        stringAggValue = valueIsNull ? stringAggValueLimit : stringAggValue;
    } else {
        bool toUpdate =
            !valueIsNull && (aggOperator == MAX_FUNC ? compareStrRowDataSimple(fieldValue, stringAggValue) > 0
                                                     : compareStrRowDataSimple(fieldValue, stringAggValue) < 0);
        if (valueIsNull || toUpdate) {
            stringAggValue = fieldValue;
            valueIsNull = false;
        } else {
            valueIsNull = valueIsNull;
        }
    }
}

void MinMaxFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);
    const bool hasFilterCol = hasFilter;
    const auto filterData =
        hasFilterCol ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex)) : nullptr;
    for (int rowIndex : indices) {
        bool shouldDoAccumulate = true;
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
        }
        if (!shouldDoAccumulate) continue;
        switch (typeId) {
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_LONG: {
                accumulateLong(columnData, rowIndex);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                accumulateString(columnData, rowIndex);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }
    }
}

void MinMaxFunction::accumulateLong(omniruntime::vec::BaseVector* columnData, int rowIndex)
{
    bool toUpdate = false;
    bool isFieldNull = columnData->IsNull(rowIndex);
    long fieldValue =
        isFieldNull ? limit : dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);
    if (isFieldNull) {
        //?? why:  we need to set aggValue when fieldValue is null, and why we set it to limit?
        aggValue = valueIsNull ? limit : aggValue;
    } else {
        toUpdate = !valueIsNull && (aggOperator == MAX_FUNC ? fieldValue > aggValue : fieldValue < aggValue);
        if (valueIsNull || toUpdate) {
            aggValue = fieldValue;
            valueIsNull = false;
        }
    }
}

void MinMaxFunction::accumulateString(omniruntime::vec::BaseVector* columnData, int rowIndex)
{
    bool toUpdate = false;
    bool isFieldNull = columnData->IsNull(rowIndex);
    std::string_view result;
    if (!isFieldNull) {
        if (columnData->GetEncoding() == omniruntime::vec::OMNI_FLAT) {
            auto casted =
                reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
                    columnData);

            result = casted->GetValue(rowIndex);
        } else { // DICTIONARY
            auto casted = reinterpret_cast<omniruntime::vec::Vector<
                omniruntime::vec::DictionaryContainer<std::string_view, omniruntime::vec::LargeStringContainer>>*>(
                columnData);

            result = casted->GetValue(rowIndex);
        }
    }
    std::string resultStringData = "";
    if (isFieldNull) {
        stringAggValue = valueIsNull ? stringAggValueLimit : stringAggValue;
    } else {
        resultStringData = std::string(result);
        toUpdate =
            !valueIsNull && (aggOperator == MAX_FUNC ? compareStrRowDataSimple(resultStringData, stringAggValue) >= 0
                                                     : compareStrRowDataSimple(resultStringData, stringAggValue) <= 0);
        if ((valueIsNull || toUpdate)) {
            stringAggValue = resultStringData;
            valueIsNull = false;
        }
    }
}

int MinMaxFunction::compareStrRowDataSimple(const std::string& leftStr, const std::string& rightStr)
{
    if (leftStr.empty() || rightStr.empty()) {
        return leftStr.empty() ? (rightStr.empty() ? 0 : -1) : 1;
    }

    return leftStr.compare(rightStr);
}

void MinMaxFunction::retract(RowData* retractInput)
{
}

void MinMaxFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
}

void MinMaxFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("This function does not require merge method, but merge was called.");
}

void MinMaxFunction::setAccumulators(RowData* _acc)
{
    valueIsNull = _acc->isNullAt(accIndex);
    switch (typeId) {
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_LONG: {
            aggValue = valueIsNull ? limit : *_acc->getLong(accIndex);
            break;
        }
        case DataTypeId::OMNI_VARCHAR: {
            stringAggValue = valueIsNull ? stringAggValueLimit : std::string(_acc->getStringView(accIndex));
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
}

void MinMaxFunction::resetAccumulators()
{
    aggValue = aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max();
    stringAggValue = "";
    valueIsNull = true;
}

void MinMaxFunction::createAccumulators(BinaryRowData* accumulators)
{
    switch (typeId) {
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_LONG: {
            accumulators->setNullAt(accIndex);
            break;
        }
        case DataTypeId::OMNI_VARCHAR: {
            accumulators->setNullAt(accIndex);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
}
void MinMaxFunction::getAccumulators(BinaryRowData* accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        switch (typeId) {
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_LONG: {
                accumulators->setLong(accIndex, aggValue);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                std::string_view sv = stringAggValue;
                accumulators->setStringView(accIndex, sv);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }
    }
}

void MinMaxFunction::getValue(BinaryRowData* newAggValue)
{
    if (valueIsNull) {
        newAggValue->setNullAt(valueIndex);
    } else {
        switch (typeId) {
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_LONG: {
                newAggValue->setLong(valueIndex, aggValue);
                break;
            }
            case DataTypeId::OMNI_VARCHAR: {
                std::string_view sv = stringAggValue;
                newAggValue->setStringView(valueIndex, sv);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }
    }
}

void MinMaxFunction::open(StateDataViewStore* store)
{
    this->store = store;
}

bool MinMaxFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    if (r1->isNullAt(valueIndex) || r2->isNullAt(valueIndex)) {
        return false;
    }
    bool isEqual = false;
    switch (typeId) {
        case DataTypeId::OMNI_TIMESTAMP:
        case DataTypeId::OMNI_LONG: {
            isEqual = *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
            break;
        }
        case DataTypeId::OMNI_VARCHAR: {
            // operator == for std::string_view compares the string it refer to
            isEqual = r1->getStringView(valueIndex) == r2->getStringView(valueIndex);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
    return isEqual;
}
