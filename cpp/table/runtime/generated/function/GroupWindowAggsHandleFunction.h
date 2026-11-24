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

#include <memory>
#include <string>
#include <vector>

#include "NamespaceAggsBasicFunction.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
class JoinedRowData;

template <typename N>
class GroupWindowAggsHandleFunction : public NamespaceAggsHandleFunction<N> {
public:
    explicit GroupWindowAggsHandleFunction(
        std::vector<std::unique_ptr<NamespaceAggsBasicFunction<N>>> functions,
        std::vector<int32_t> aggValueTypeIds,
        std::vector<int32_t> windowPropertyTypeIds,
        std::vector<int32_t> outputValueTypeIds,
        int32_t accumulatorArity,
        std::string shiftTimeZone = "UTC");

    void open(StateDataViewStore* store) override;

    void setAccumulators(N namespaceVal, RowData* accumulators) override;

    void accumulate(RowData* inputRow) override;

    void retract(RowData* inputRow) override;

    void merge(N namespaceVal, RowData* otherAcc) override;

    RowData* createAccumulators() override;

    RowData* getAccumulators() override;

    RowData* getValue(N namespaceVal) override;

    void cleanup(N namespaceVal) override;

    void close() override;

private:
    int64_t convertWindowBoundaryTimestamp(int64_t utcTimestampMills, int32_t propertyTypeId) const;

    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<N>>> functions_;
    std::vector<int32_t> aggValueTypeIds_;
    std::vector<int32_t> windowPropertyTypeIds_;
    std::vector<int32_t> outputValueTypeIds_;
    std::string shiftTimeZone_;
    RowData* currentAcc_{};
};
