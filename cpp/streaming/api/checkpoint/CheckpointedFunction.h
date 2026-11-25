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

#ifndef OMNISTREAM_CHECKPOINTEDFUNCTION_H
#define OMNISTREAM_CHECKPOINTEDFUNCTION_H

#include "runtime/state/StateSnapshotContextSynchronousImpl.h"
#include "runtime/state/StateInitializationContextImpl.h"

class CheckpointedFunction {
public:
    virtual ~CheckpointedFunction() = default;
    // 此处snapshotState和initializeState的参数应为对应类型的父类，OmniStreamTask未实现，故使用具体类。
    virtual void snapshotState(StateSnapshotContextSynchronousImpl* context)
    {
    }
    virtual void initializeState(StateInitializationContextImpl* context)
    {
    }
};

#endif // OMNISTREAM_CHECKPOINTEDFUNCTION_H
