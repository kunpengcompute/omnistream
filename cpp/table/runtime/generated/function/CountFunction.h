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

#ifndef FLINK_TNEL_COUNTFUNCTION_H
#define FLINK_TNEL_COUNTFUNCTION_H

#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
using namespace omniruntime::type;
class CountFunction : public AggsHandleFunction {
public:
    CountFunction(int aggIdx, std::string inputType, int accIndex = -1, int valueIndex = -1, int filterIndex = -1)
        : valueIsNull(true),
          aggIdx(aggIdx),
          accIndex(accIndex),
          valueIndex(valueIndex),
          filterIndex(filterIndex)
    {
        typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        hasFilter = filterIndex != -1;
        isCountStar = false;
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
    void bindAccValueIndex(int accStartIndex, int valueStartIndex) override
    {
        accIndex = accStartIndex;
        valueIndex = valueStartIndex;
    }
    int accumulatorSlots() const override
    {
        return 1;
    }
    bool hasAggOutput() const override
    {
        return !isCountStar && valueIndex >= 0;
    }
    void setCountStart(bool isCountStartFunc);

private:
    long aggCount;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    int filterIndex;
    bool hasFilter;
    bool isCountStar;
    omniruntime::type::DataTypeId typeId;
    StateDataViewStore* store;
};

#endif // FLINK_TNEL_COUNTFUNCTION_H
