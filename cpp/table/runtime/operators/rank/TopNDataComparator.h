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

#pragma once

#include "table/data/util/VectorBatchUtil.h"

template <typename K>
class AppendOnlyTopNFunction;

/**
 * Comparator used inside std::set<long>.
 * Calls back into AppendOnlyTopNFunction to compare rows by sort key.
 */
template <typename K>
struct TopNDataComparator {
    AppendOnlyTopNFunction<K>* fn = nullptr;

    TopNDataComparator() = default;
    explicit TopNDataComparator(AppendOnlyTopNFunction<K>* f) : fn(f)
    {
    }

    inline bool operator()(int64_t a, int64_t b) const
    {
        if (a == b) return false;

        int rowIdA = omnistream::VectorBatchUtil::getRowId(static_cast<ComboId>(a));
        int rowIdB = omnistream::VectorBatchUtil::getRowId(static_cast<ComboId>(b));

        auto* vbA = fn->GetVectorBatch(static_cast<ComboId>(a));
        auto* vbB = fn->GetVectorBatch(static_cast<ComboId>(b));

        if (vbA == nullptr || vbB == nullptr) {
            std::cout << "Error: VectorBatch is null in TopNDataComparator." << std::endl;
            return false;
        }

        return fn->CompareRowData(vbA, rowIdA, vbB, rowIdB);
    }
};
