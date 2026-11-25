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

#include "GeneratedAggsHandleFunctionAverage.h"

GeneratedAggsHandleFunctionAverage::GeneratedAggsHandleFunctionAverage(
    int aggIdx, int accIndexSum, int accIndexCount0, int valueIndex, int accIndexCount1)
    : aggIdx(aggIdx),
      accIndexSum(accIndexSum),
      accIndexCount0(accIndexCount0),
      accIndexCount1(accIndexCount1),
      valueIndex(valueIndex)
{
    store = nullptr;
}

void GeneratedAggsHandleFunctionAverage::createAccumulators(BinaryRowData* accumulators)
{
    throw std::runtime_error(
        "This function does not require createAccumulators method, but createAccumulators was called.");
}

void GeneratedAggsHandleFunctionAverage::accumulate(RowData* accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    long fieldValue = isFieldNull ? -1L : *accInput->getLong(aggIdx);

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

    // Update agg1_count1
    if (accIndexCount1 != -1) {
        if (count1IsNull) {
            count1 = 1;
            count1IsNull = false;
        } else {
            count1 += 1;
        }
    }
}

void GeneratedAggsHandleFunctionAverage::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);

    for (int rowIndex : indices) {
        bool isFieldNull = columnData->IsNull(rowIndex);
        long fieldValue = -1L;

        if (!isFieldNull) {
            fieldValue = dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);
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

        // Update agg1_count1
        if (accIndexCount1 != -1) {
            if (count1IsNull) {
                count1 = 1;
                count1IsNull = false;
            } else {
                count1 += 1;
            }
        }
    }
}

void GeneratedAggsHandleFunctionAverage::setAccumulators(RowData* _acc)
{
    sumIsNull = _acc->isNullAt(accIndexSum);
    sum = sumIsNull ? 0L : *_acc->getLong(accIndexSum);

    count0IsNull = _acc->isNullAt(accIndexCount0);
    count0 = count0IsNull ? 0L : *_acc->getLong(accIndexCount0);
    if (accIndexCount1 != -1) {
        count1IsNull = _acc->isNullAt(accIndexCount1);
        count1 = count1IsNull ? 0L : *_acc->getLong(accIndexCount1);
    }
}

void GeneratedAggsHandleFunctionAverage::resetAccumulators()
{
    sum = 0;
    sumIsNull = false;

    count0 = 0;
    count0IsNull = false;

    count1 = 0;
    count1IsNull = false;
}

void GeneratedAggsHandleFunctionAverage::open(StateDataViewStore* store)
{
    this->store = store;
}

void GeneratedAggsHandleFunctionAverage::retract(RowData* retractInput)
{
    bool isFieldNull = retractInput->isNullAt(aggIdx);
    long fieldValue = isFieldNull ? -1L : *retractInput->getLong(aggIdx);

    // Update agg0_sum
    if (!isFieldNull) {
        sum = sumIsNull ? sum : sum - fieldValue;
        count0 = count0IsNull ? count0 : count0 - 1;
    }
    // Update agg1_count1
    if (accIndexCount1 != -1) {
        count1 = count1IsNull ? count1 : count1 - 1L;
    }
}

void GeneratedAggsHandleFunctionAverage::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto columnData = input->Get(aggIdx);
    for (int rowIndex : indices) {
        bool isFieldNull = columnData->IsNull(rowIndex);
        long fieldValue;

        fieldValue = isFieldNull ? -1L : dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);

        // Update agg0_sum
        if (!isFieldNull) {
            sum = sumIsNull ? sum : sum - fieldValue;
            count0 = count0IsNull ? count0 : count0 - 1;
        }

        // Update agg1_count1
        if (accIndexCount1 != -1) {
            count1 = count1IsNull ? count1 : count1 - 1L;
        }
    }
}

void GeneratedAggsHandleFunctionAverage::merge(RowData* otherAcc)
{
    throw std::runtime_error("This function does not require the merge method, but the merge method is called.");
}

void GeneratedAggsHandleFunctionAverage::getAccumulators(BinaryRowData* acc)
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
    if (accIndexCount1 != -1) {
        if (count1IsNull) {
            acc->setNullAt(accIndexCount1);
        } else {
            acc->setLong(accIndexCount1, count1);
        }
    }
}

void GeneratedAggsHandleFunctionAverage::getValue(BinaryRowData* aggValue)
{
    if (count0IsNull || count0 == 0) {
        aggValue->setNullAt(valueIndex);
    } else {
        long average = sum / count0;
        aggValue->setLong(valueIndex, average);
    }
}

bool GeneratedAggsHandleFunctionAverage::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    return !r1->isNullAt(valueIndex) && !r2->isNullAt(valueIndex) &&
           *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
}
