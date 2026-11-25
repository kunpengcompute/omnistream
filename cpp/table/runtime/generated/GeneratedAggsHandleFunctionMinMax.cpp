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

#include "GeneratedAggsHandleFunctionMinMax.h"
#include <stdexcept>
#include <iostream>
#include <common.h>
#include "table/data/binary/BinaryRowData.h"

void GeneratedAggsHandleFunctionMinMax::accumulate(RowData* accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    long fieldValue = isFieldNull ? limit : *accInput->getLong(aggIdx);

    if (isFieldNull) {
        aggValue = valueIsNull ? limit : aggValue;
    } else {
        bool toUpdate = !valueIsNull && (aggOperator == MAX ? fieldValue > aggValue : fieldValue < aggValue);
        if (valueIsNull || toUpdate) {
            aggValue = fieldValue;
            valueIsNull = false;
        } else {
            valueIsNull = valueIsNull;
        }
    }
}

void GeneratedAggsHandleFunctionMinMax::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx); // Get the column data for the aggregation

    for (int rowIndex : indices) {
        bool isFieldNull = columnData->IsNull(rowIndex);
        long fieldValue =
            isFieldNull ? limit : dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);

        if (isFieldNull) {
            // If the field is null, we keep the existing aggregation value (or initialize if it's the first null)
            aggValue = valueIsNull ? limit : aggValue;
        } else {
            // If the field is not null, update the aggregation value based on the operator (MAX or MIN)
            bool toUpdate = !valueIsNull && (aggOperator == MAX ? fieldValue > aggValue : fieldValue < aggValue);
            if (valueIsNull || toUpdate) {
                aggValue = fieldValue; // Update the aggregation value
                valueIsNull = false;   // Mark that the value is no longer null
            }
        }
    }
}

void GeneratedAggsHandleFunctionMinMax::retract(RowData* retractInput)
{
    throw std::runtime_error("This function does not require retract method, but retract was called.");
}

void GeneratedAggsHandleFunctionMinMax::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    throw std::runtime_error("This function does not require retract method, but retract was called.");
}

void GeneratedAggsHandleFunctionMinMax::createAccumulators(BinaryRowData* accumulators)
{
    throw std::runtime_error(
        "This function does not require createAccumulators method, but createAccumulators was called.");
}

void GeneratedAggsHandleFunctionMinMax::merge(RowData* otherAcc)
{
    throw std::runtime_error("This function does not require merge method, but merge was called.");
}

void GeneratedAggsHandleFunctionMinMax::setAccumulators(RowData* _acc)
{
    valueIsNull = _acc->isNullAt(accIndex);
    aggValue = valueIsNull ? limit : *_acc->getLong(accIndex);
}

void GeneratedAggsHandleFunctionMinMax::resetAccumulators()
{
    aggValue = aggOperator == MAX ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max();
    valueIsNull = true;
}

void GeneratedAggsHandleFunctionMinMax::getAccumulators(BinaryRowData* accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggValue);
    }
}

void GeneratedAggsHandleFunctionMinMax::getValue(BinaryRowData* newAggValue)
{
    if (valueIsNull) {
        newAggValue->setNullAt(valueIndex);
    } else {
        newAggValue->setLong(valueIndex, aggValue);
    }
}

void GeneratedAggsHandleFunctionMinMax::open(StateDataViewStore* store)
{
    this->store = store;
}

bool GeneratedAggsHandleFunctionMinMax::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    return !r1->isNullAt(valueIndex) && !r2->isNullAt(valueIndex) &&
           *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
}
