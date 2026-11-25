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

#include "functions/AbstractRichFunction.h"
#include <stdexcept>

RuntimeContext* AbstractRichFunction::getRuntimeContext()
{
    if (runtimeContext) {
        return runtimeContext;
    } else {
        throw std::runtime_error("The runtime context has not been initialized.");
    }
}

IterationRuntimeContext* AbstractRichFunction::getIterationRuntimeContext()
{
    if (!runtimeContext) {
        throw std::runtime_error("The runtime context has not been initialized.");
    }
    IterationRuntimeContext* iterationContext = static_cast<IterationRuntimeContext*>(runtimeContext);
    if (iterationContext) {
        return iterationContext;
    } else {
        throw std::runtime_error("This function is not part of an iteration step.");
    }
}

void AbstractRichFunction::setRuntimeContext(RuntimeContext* context)
{
    runtimeContext = context;
}

AbstractRichFunction::~AbstractRichFunction() = default;

void AbstractRichFunction::open(const Configuration& parameters)
{
}

void AbstractRichFunction::close()
{
}

void AbstractRichFunction::open(Configuration* parameters)
{
}
