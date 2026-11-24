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

#ifndef FLINK_TNEL_ITERATION_RUNTIME_CONTEXT_H
#define FLINK_TNEL_ITERATION_RUNTIME_CONTEXT_H

#include <string>
#include "RuntimeContext.h"

class IterationRuntimeContext : public RuntimeContext {
public:
    virtual ~IterationRuntimeContext();

    virtual int getSuperstepNumber() const = 0;

    template <typename T>
    T* getIterationAggregator(const std::string& name);

    template <typename T>
    T* getPreviousIterationAggregate(const std::string& name);
};

#endif // FLINK_TNEL_ITERATION_RUNTIME_CONTEXT_H
