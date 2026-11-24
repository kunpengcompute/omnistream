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

#ifndef GENERATED_AGGS_HANDLER_FUNCTION_MAX_H
#define GENERATED_AGGS_HANDLER_FUNCTION_MAX_H

#include <stdexcept>
#include "AggsHandleFunction.h"
#include "table/data/RowData.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"

enum AggMaxOrMin {
    MAX,
    MIN
};

class GeneratedAggsHandleFunctionMinMax : public AggsHandleFunction {
public:
    GeneratedAggsHandleFunctionMinMax(int aggIdx, int accIndex, int valueIndex, AggMaxOrMin aggOperator)
        : aggValue(aggOperator == MAX ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
          limit(aggOperator == MAX ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
          valueIsNull(true),
          aggIdx(aggIdx),
          accIndex(accIndex),
          valueIndex(valueIndex),
          aggOperator(aggOperator)
    {
    }

    void open(StateDataViewStore* store);
    void setWindowSize(int windowSize) override {};
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    void cleanup() override {};
    void close() override {};
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;

private:
    long aggValue;
    long limit;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    AggMaxOrMin aggOperator;
    StateDataViewStore* store = nullptr;
};

#endif // GENERATED_AGGS_HANDLER_FUNCTION_MAX_H
