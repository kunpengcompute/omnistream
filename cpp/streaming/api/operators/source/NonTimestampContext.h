/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_NONTIMESTAMPCONTEXT_H
#define FLINK_TNEL_NONTIMESTAMPCONTEXT_H

#include <mutex>

#include "functions/SourceContext.h"

class NonTimestampContext : public SourceContext {
public:
    NonTimestampContext(Object* checkpointLock, Output* output, bool isStream = false)
        : lock(checkpointLock),
          output(output),
          isStream(isStream)
    {
        reuse = new StreamRecord();
    }

    ~NonTimestampContext() override
    {
        delete reuse;
    }

    void collect(void* element) override
    {
        std::lock_guard<std::recursive_mutex> guard(lock->mutex);
        if (isStream) {
            output->collect(reuse->replace(element));
        } else {
            output->collect(new StreamRecord(element));
        }
    }

    void collectWithTimestamp(void* element, int64_t timestamp) override
    {
        // ignore the timestamp
        collect(element);
    }

    void emitWatermark(Watermark* mark) override
    {
        // do nothing
    }

    void markAsTemporarilyIdle() override
    {
        // do nothing
    }

    Object* getCheckpointLock() override
    {
        return lock;
    }

    void close() override
    {
    }

private:
    Object* lock;
    Output* output;
    StreamRecord* reuse;
    bool isStream;
};

#endif // FLINK_TNEL_NONTIMESTAMPCONTEXT_H
