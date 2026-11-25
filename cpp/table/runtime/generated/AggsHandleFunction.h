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

#ifndef FLINK_TNEL_AGGS_HANDLE_FUNCTION_H
#define FLINK_TNEL_AGGS_HANDLE_FUNCTION_H

#include "AggsHandleFunctionBase.h"
#include "../../data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"

class AggsHandleFunction {
public:
    virtual ~AggsHandleFunction() = default;

    virtual void getValue(BinaryRowData* aggValue) = 0;

    virtual void setWindowSize(int windowSize) = 0;

    virtual bool equaliser(BinaryRowData* r1, BinaryRowData* r2) = 0;
    virtual void accumulate(RowData* input) = 0;
    virtual void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) = 0;
    virtual void retract(RowData* input) = 0;
    virtual void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) = 0;
    virtual void merge(RowData* accumulators) = 0;
    virtual void createAccumulators(BinaryRowData* accumulators) = 0;
    virtual void setAccumulators(RowData* accumulators) = 0;
    virtual void resetAccumulators() = 0;
    virtual void getAccumulators(BinaryRowData* accumulators) = 0;
    virtual void cleanup() = 0;
    virtual void close() = 0;
    virtual void bindAccValueIndex(int accStartIndex, int valueStartIndex)
    {
    }
    virtual int accumulatorSlots() const
    {
        return 1;
    }
    virtual bool hasAggOutput() const
    {
        return true;
    }
    virtual void setCurrentGroupKey(RowData* key) {};
    virtual void setBackend(int inputBackend)
    {
        this->backend = inputBackend;
    }
    virtual void updateInnerState() {};

protected:
    int backend = 0; // 0: memory, 1: bss, 2: rocksdb
};

#endif // FLINK_TNEL_AGGS_HANDLE_FUNCTION_H
