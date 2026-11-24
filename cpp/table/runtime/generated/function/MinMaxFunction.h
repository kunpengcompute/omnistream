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

#ifndef FLINK_TNEL_MINMAXFUNCTION_H
#define FLINK_TNEL_MINMAXFUNCTION_H
#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"

using namespace omniruntime::type;
enum FuncType {
    MAX_FUNC,
    MIN_FUNC
};
class MinMaxFunction : public AggsHandleFunction {
public:
    MinMaxFunction(
        int aggIdx,
        std::string inputType,
        int accIndex = -1,
        int valueIndex = -1,
        FuncType aggOperator = MAX_FUNC,
        int filterIndex = -1)
        : aggOperator(aggOperator),
          aggValue(aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
          valueIsNull(true),
          aggIdx(aggIdx),
          accIndex(accIndex),
          valueIndex(valueIndex),
          limit(aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
          filterIndex(filterIndex)
    {
        typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        hasFilter = filterIndex != -1;
    }

    void open(StateDataViewStore* store);
    void setWindowSize(int windowSize) override {};
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void accumulateLong(RowData* accInput);
    void accumulateLong(omniruntime::vec::BaseVector* columnData, int rowIndex);
    void accumulateString(RowData* accInput);
    void accumulateString(omniruntime::vec::BaseVector* columnData, int rowIndex);
    int compareStrRowDataSimple(const std::string& left, const std::string& right);
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* acc) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    void cleanup() override {};
    void close() override {};
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;
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
        return valueIndex >= 0;
    }

private:
    FuncType aggOperator;
    long aggValue;

    std::string stringAggValue;
    std::string stringAggValueLimit;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    long limit;
    int filterIndex;
    bool hasFilter;
    omniruntime::type::DataTypeId typeId;

    StateDataViewStore* store = nullptr;
};

#endif // FLINK_TNEL_MINMAXFUNCTION_H
