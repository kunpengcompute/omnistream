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

#ifndef OMNIFLINK_INIT_CONTEXT_IMPL_H
#define OMNIFLINK_INIT_CONTEXT_IMPL_H

#include <optional>
#include <cstdint>

#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/runtime/tasks/ProcessingTimeServiceImpl.h"

template <typename K>
class InitContextImpl {
private:
    ProcessingTimeServiceImpl* processingTimeService;
    std::optional<uint64_t> restoredCheckpointId;
    StreamingRuntimeContext<K>* runtimeContext;

public:
    InitContextImpl(
        StreamingRuntimeContext<K>* runtimeContext,
        ProcessingTimeServiceImpl* processingTimeService,
        std::optional<uint64_t> restoredCheckpointId)
        : runtimeContext(runtimeContext),
          processingTimeService(processingTimeService),
          restoredCheckpointId(restoredCheckpointId)
    {
    }

    int getNumberOfParallelSubtasks()
    {
        return runtimeContext->getNumberOfParallelSubtasks();
    }

    ProcessingTimeServiceImpl* getProcessingTimeService()
    {
        return processingTimeService;
    }

    int getSubtaskId()
    {
        return runtimeContext->getIndexOfThisSubtask();
    }

    std::optional<uint64_t> getRestoredCheckpointId()
    {
        return restoredCheckpointId;
    }
};

#endif // OMNIFLINK_INIT_CONTEXT_IMPL_H
