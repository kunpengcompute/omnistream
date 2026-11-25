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

#ifndef OMNISTREAM_MTFILTERFUNC_H
#define OMNISTREAM_MTFILTERFUNC_H

#include <vector>
#include <functions/FilterFunction.h>
#include "OriginalRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"

class MTFilterFunc : public FilterFunction<omnistream::VectorBatch> {
public:
    MTFilterFunc() : filterColIdx(1), filterResult(filterFlag)
    {
        filterFlag = !filterFlag;
    }

    std::vector<int> filterBatch(void* input) override
    {
        auto* batch = reinterpret_cast<omnistream::VectorBatch*>(input);
        std::vector<int> newBatch;
        auto rowCount = batch->GetRowCount();
        newBatch.reserve(rowCount);
        auto colVecs = batch->GetVectors();
        auto colVec = reinterpret_cast<omniruntime::vec::Vector<bool>*>(colVecs[filterColIdx]);
        for (int i = 0; i < rowCount; i++) {
            if (colVec->GetValue(i) == filterResult) {
                newBatch.push_back(i);
            }
        }
        return newBatch;
    }

private:
    static bool filterFlag;
    int filterColIdx;
    bool filterResult;
};

#endif // OMNISTREAM_MTFILTERFUNC_H
