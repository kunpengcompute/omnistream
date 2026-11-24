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

#ifndef FLINK_TNEL_SUMFUNCTION_H
#define FLINK_TNEL_SUMFUNCTION_H

#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
using namespace omniruntime::type;
class SumFunction : public AggsHandleFunction {
public:
    SumFunction(int aggIdx, std::string inputType, int accIndex = -1, int valueIndex = -1, int filterIndex = -1);
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* _acc) override;
    void resetAccumulators() override;
    void createAccumulators(BinaryRowData* accumulators) override;
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
    void bindAccValueIndex(int accStartIndex, int valueStartIndex) override
    {
        accIndex = accStartIndex;
        valueIndex = valueStartIndex;
        if (consumeRetraction) {
            accIndexCount0 = accStartIndex + 1;
        } else {
            accIndexCount0 = -1;
        }
    }
    int accumulatorSlots() const override
    {
        return consumeRetraction ? 2 : 1;
    }
    bool hasAggOutput() const override
    {
        return valueIndex >= 0;
    }
    void setRetraction(int accIndexCount0Index);

private:
    long sum = 0;
    long count0 = 0;
    bool sumIsNull = false;
    bool count0IsNull = false;
    bool consumeRetraction = false;
    int aggIdx;
    std::string inputType;
    int accIndex;
    int accIndexCount0;
    int valueIndex;
    int filterIndex;
    bool hasFilter;
    StateDataViewStore* store;
};

#endif // FLINK_TNEL_SUMFUNCTION_H
