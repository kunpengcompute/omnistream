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

#ifndef FLINK_TNEL_CLOSEDCONTEXT_H
#define FLINK_TNEL_CLOSEDCONTEXT_H

#include "functions/SourceContext.h"

class ClosedContext : public SourceContext {
public:
    explicit ClosedContext(Object* checkpointLock)
    {
        this->checkpointLock = checkpointLock;
    }

    void collectWithTimestamp(void* element, int64_t timestamp) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void collect(void* element) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void emitWatermark(Watermark* mark) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void markAsTemporarilyIdle() override
    {
        NOT_IMPL_EXCEPTION;
    }

    Object* getCheckpointLock() override
    {
        return checkpointLock;
    }

    void close() override
    {
        // nothing to be done
    }

private:
    Object* checkpointLock;
};

#endif
