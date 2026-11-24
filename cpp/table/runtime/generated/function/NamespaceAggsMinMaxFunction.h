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

#include "NamespaceAggsBasicFunction.h"

using namespace omnistream;
enum class NamespaceAggsBasicFunctionType;

template <typename N>
class NamespaceAggsMinMaxFunction : public NamespaceAggsBasicFunction<N> {
public:
    NamespaceAggsMinMaxFunction(
        std::vector<int32_t> argIndexes,
        std::vector<int32_t> inputTypeIds,
        std::vector<int32_t> accIndexes,
        std::vector<int32_t> accTypeIds,
        int32_t aggValueIndex,
        int32_t aggValueTypeId,
        NamespaceAggsBasicFunctionType type);

    void accumulate(RowData* input) override;
    void retract(RowData* input) override;
    void accumulate(
        const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices) override;
    void retract(
        const std::vector<omnistream::VectorBatch*>& inputBatches, const std::vector<int64_t>& indices) override;
    void merge(N ns, RowData* otherAcc) override;
    void setAccumulators(N ns, RowData* acc) override;
    RowData* getAccumulators() override;
    void updateAggValue(RowData* input) override;

private:
    void update(int64_t candidate);

    NamespaceAggsBasicFunctionType type_;
    int64_t value_ = 0L;
};
