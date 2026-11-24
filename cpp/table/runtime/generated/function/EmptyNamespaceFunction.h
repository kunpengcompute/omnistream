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

#ifndef OMNISTREAM_EMPTYFUNCTION_H
#define OMNISTREAM_EMPTYFUNCTION_H

#include <optional>
#include <stdexcept>
#include <cstdint>
#include "table/runtime/generated/AggsHandleFunction.h"
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"

class EmptyNamespaceFunction : public AggsHandleFunction {
public:
    void getValue(BinaryRowData* aggValue) override
    {
    }
    void setWindowSize(int windowSize) override
    {
    }
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override
    {
        return false;
    }
    void accumulate(RowData* input) override
    {
    }
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override
    {
    }

    void retract(RowData* input) override
    {
        throw std::runtime_error("This function does not require retract method, but the retract method is called.");
    }
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override
    {
        throw std::runtime_error("This function does not require retract method, but the retract method is called.");
    }
    void merge(RowData* accumulators) override
    {
    }
    void createAccumulators(BinaryRowData* accumulators) override
    {
    }
    void setAccumulators(RowData* accumulators) override
    {
    }
    void resetAccumulators() override
    {
    }
    void getAccumulators(BinaryRowData* accumulators) override
    {
    }
    void cleanup() override
    {
    }
    void close() override
    {
    }
};

#endif
