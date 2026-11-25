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

#include "LastStringValueFunction.h"

bool LastStringValueFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    return false;
}

void LastStringValueFunction::accumulate(RowData* accInput)
{
}

void LastStringValueFunction::retract(RowData* retractInput)
{
}

void LastStringValueFunction::merge(RowData* otherAcc)
{
}

void LastStringValueFunction::setAccumulators(RowData* acc)
{
    valueIsNull = acc->isNullAt(accIndex);
    switch (typeId) {
        case DataTypeId::OMNI_VARCHAR: {
            stringAggValue = valueIsNull ? emptyStringAggValue : std::string(acc->getStringView(accIndex));
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
}

void LastStringValueFunction::resetAccumulators()
{
    stringAggValue = "";
    valueIsNull = true;
}

void LastStringValueFunction::getAccumulators(BinaryRowData* accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        switch (typeId) {
            case DataTypeId::OMNI_VARCHAR: {
                std::string_view sv = stringAggValue;
                accumulators->setStringView(accIndex, sv);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }
    }
}

void LastStringValueFunction::createAccumulators(BinaryRowData* accumulators)
{
    switch (typeId) {
        case DataTypeId::OMNI_VARCHAR: {
            accumulators->setNullAt(accIndex);
            break;
        }
        default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
    }
}

void LastStringValueFunction::getValue(BinaryRowData* aggValue)
{
    if (valueIsNull) {
        aggValue->setNullAt(valueIndex);
    } else {
        switch (typeId) {
            case DataTypeId::OMNI_VARCHAR: {
                std::string_view sv = stringAggValue;
                aggValue->setStringView(valueIndex, sv);
                break;
            }
            default: LOG("Data type is not supported."); throw std::runtime_error("Data type is not supported.");
        }
    }
}

void LastStringValueFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
}

void LastStringValueFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
}
