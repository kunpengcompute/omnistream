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

#ifndef FLINK_TNEL_RUNTIME_CONTEXT_H
#define FLINK_TNEL_RUNTIME_CONTEXT_H

#include "core/api/common/state/State.h"
#include "core/api/common/state/StateDescriptor.h"
#include "core/api/common/state/MapStateDescriptorWrapper.h"
#include "core/api/common/state/ValueStateDescriptorWrapper.h"
#include "core/api/common/state/MapStateWrapper.h"
#include "core/api/common/state/ValueStateWrapper.h"

class RuntimeContext {
public:
    virtual ~RuntimeContext() = default;
    virtual void close() = 0;

    virtual int getNumberOfParallelSubtasks() = 0;
    virtual int getIndexOfThisSubtask() = 0;
    State* getState(StateDescriptor* stateProperties);
    State* getMapState(StateDescriptor* descriptor);

    // dataStream need
    MapStateWrapper* getMapState(MapStateDescriptorWrapper* stateProperties);
    ValueStateWrapper* getState(ValueStateDescriptorWrapper* stateProperties);
};

#endif // FLINK_TNEL_RUNTIME_CONTEXT_H
