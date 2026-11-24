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

#ifndef OMNISTREAM_STREAMINGFUNCTIONUTILS_H
#define OMNISTREAM_STREAMINGFUNCTIONUTILS_H

#include <exception>
#include "core/include/common.h"
#include "include/functions/Function.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/api/common/ExecutionConfig.h"
#include "streaming/api/checkpoint/CheckpointedFunction.h"
#include "runtime/state/OperatorStateBackend.h"
#include "runtime/state/StateSnapshotContextSynchronousImpl.h"

class StreamingFunctionUtils {
public:
    static void setOutputType(
        omnistream::Function* userFunction, const TypeInformation& outTypeInfo, const ExecutionConfig& executionConfig)
    {
        NOT_IMPL_EXCEPTION;
    }

    static void snapshotFunctionState(
        StateSnapshotContextSynchronousImpl* context, OperatorStateBackend* backend, omnistream::Function* userFunction)
    {
        if (context == nullptr) {
            throw std::invalid_argument("context is nullptr");
        }
        if (backend == nullptr) {
            throw std::invalid_argument("backend is nullptr");
        }

        while (true) {
            if (trySnapshotFunctionState(context, backend, userFunction)) {
                break;
            }
            // TODO　WrappingFunction to be supported.
            break;
        }
    }

    static void restoreFunctionState(StateInitializationContextImpl* context, omnistream::Function* userFunction)
    {
        if (context == nullptr) {
            throw std::invalid_argument("context is nullptr");
        }

        while (true) {
            if (tryRestoreFunction(context, userFunction)) {
                break;
            }
            // TODO　WrappingFunction to be supported.
            break;
        }
    }

private:
    static bool trySetOutputType(
        omnistream::Function* userFunction, const TypeInformation& outTypeInfo, const ExecutionConfig& executionConfig)
    {
        NOT_IMPL_EXCEPTION;
    }

    static bool trySnapshotFunctionState(
        StateSnapshotContextSynchronousImpl* context, OperatorStateBackend* backend, omnistream::Function* userFunction)
    {
        auto* checkpointed = dynamic_cast<CheckpointedFunction*>(userFunction);
        if (checkpointed != nullptr) {
            checkpointed->snapshotState(context);
            return true;
        }
        // TODO ListCheckpointed to be supported.
        return false;
    }

    static bool tryRestoreFunction(StateInitializationContextImpl* context, omnistream::Function* userFunction)
    {
        auto* checkpointed = dynamic_cast<CheckpointedFunction*>(userFunction);
        if (checkpointed != nullptr) {
            checkpointed->initializeState(context);
            return true;
        }
        // TODO ListCheckpointed to be supported.
        return false;
    }
};

#endif // OMNISTREAM_STREAMINGFUNCTIONUTILS_H
