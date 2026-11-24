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

#include "table/data/vectorbatch/VectorBatch.h"
#include "core/typeutils/TypeSerializer.h"
#include "table/runtime/operators/window/SlicingWindowOperator.h"
#include "streaming/api/operators/Output.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"

template <typename K, typename W>
class SlicingWindowProcessor {
public:
    SlicingWindowProcessor() {};
    virtual void open(
        AbstractKeyedStateBackend<K>* state,
        const nlohmann::json& config,
        StreamingRuntimeContext<K>* runtimeCtx,
        InternalTimerServiceImpl<K, int64_t>* internalTimerService) = 0;
    virtual void initializeWatermark(int64_t watermark) = 0;
    virtual bool processBatch(omnistream::VectorBatch* vectorbatch) = 0;
    virtual void advanceProgress(long progress) = 0;
    virtual void prepareCheckpoint() = 0;
    virtual void fireWindow(W window) = 0;
    virtual void clearWindow(W window) = 0;
    virtual void close() = 0;
    virtual TypeSerializer* createWindowSerializer() = 0;
    virtual Output* getOutput() = 0;
};
