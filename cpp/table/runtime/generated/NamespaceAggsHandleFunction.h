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

#include "NamespaceAggsHandleFunctionBase.h"

template <typename N>
class NamespaceAggsHandleFunction : public NamespaceAggsHandleFunctionBase<N> {
public:
    // Gets the result of the aggregation from the current accumulators and namespace properties
    virtual RowData* getValue(N namespaceVal) = 0;

    explicit NamespaceAggsHandleFunction(int32_t accumulatorArity)
        : NamespaceAggsHandleFunctionBase<N>(accumulatorArity)
    {
    }

    ~NamespaceAggsHandleFunction() override = default;
};
