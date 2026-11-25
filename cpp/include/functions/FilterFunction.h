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

#ifndef FLINK_TNEL_FILTERFUNCTION_H
#define FLINK_TNEL_FILTERFUNCTION_H

#include <memory>
#include <vector>
#include "basictypes/Object.h"
#include "common.h"
#include "Function.h"

/**
 * T: such as Object, VectorBatch
 * */
template <typename T>
class FilterFunction : public omnistream::Function {
public:
    virtual ~FilterFunction() = default;

    virtual bool filter(Object* input)
    {
        NOT_IMPL_EXCEPTION;
    };

    virtual std::vector<int> filterBatch(void* input)
    {
        NOT_IMPL_EXCEPTION;
    };
};

template <typename T>
using FilterFunctionUnique = std::unique_ptr<FilterFunction<T>>;
#endif // FLINK_TNEL_FILTERFUNCTION_H
