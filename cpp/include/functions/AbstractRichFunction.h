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

#ifndef FLINK_TNEL_ABSTRACT_RICH_FUNCTION_H
#define FLINK_TNEL_ABSTRACT_RICH_FUNCTION_H

#include "RichFunction.h"
#include "RuntimeContext.h"
#include "IterationRuntimeContext.h"
#include "Configuration.h"

class AbstractRichFunction : public RichFunction {
public:
    ~AbstractRichFunction();
    void open(const Configuration& parameters) override;
    void open(Configuration* parameters) override;
    void close() override;
    void setRuntimeContext(RuntimeContext* context) override;
    RuntimeContext* getRuntimeContext() override;
    IterationRuntimeContext* getIterationRuntimeContext() override;

private:
    RuntimeContext* runtimeContext;
};
#endif // FLINK_TNEL_ABSTRACT_RICH_FUNCTION_H
