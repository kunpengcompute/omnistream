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

#ifndef GENERATED_AGGS_HANDLER_FUNCTION_AVERAGE_H
#define GENERATED_AGGS_HANDLER_FUNCTION_AVERAGE_H

#include <stdexcept>
#include "AggsHandleFunction.h"
#include "table/data/RowData.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"

class GeneratedAggsHandleFunctionAverage : public AggsHandleFunction {
public:
    GeneratedAggsHandleFunctionAverage(
        int aggIdx, int accIndexSum, int accIndexCount0, int valueIndex, int accIndexCount1 = -1);
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* _acc) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void resetAccumulators() override;
    void open(StateDataViewStore* store);
    void setWindowSize(int windowSize) override
    {
    }
    void cleanup() override
    {
    }
    void close() override
    {
    }
    void getAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;

private:
    long sum = 0;
    bool sumIsNull = false;
    long count0 = 0;
    bool count0IsNull = false;
    long count1 = 0;
    bool count1IsNull = false;
    int aggIdx;
    int accIndexSum;

    int accIndexCount0;
    int accIndexCount1;
    int valueIndex;
    StateDataViewStore* store;
};

#endif // GENERATED_AGGS_HANDLER_FUNCTION_AVERAGE_H
