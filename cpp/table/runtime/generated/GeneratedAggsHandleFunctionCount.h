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

#ifndef GENERATED_AGGS_HANDLER_FUNCTION_COUNT_H
#define GENERATED_AGGS_HANDLER_FUNCTION_COUNT_H

#include "AggsHandleFunction.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "functions/RuntimeContext.h"

class GeneratedAggsHandleFunctionCount : public AggsHandleFunction {
public:
    GeneratedAggsHandleFunctionCount(int aggIdx, int accIndex, int valueIndex)
        : valueIsNull(true),
          aggIdx(aggIdx),
          accIndex(accIndex),
          valueIndex(valueIndex)
    {
    }

    void setWindowSize(int windowSize) override {};
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;
    void open(StateDataViewStore* store);
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    void cleanup() override {};
    void close() override {};

private:
    long aggCount;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    StateDataViewStore* store;
};

#endif // GENERATED_AGGS_HANDLER_FUNCTION_COUNT_H
