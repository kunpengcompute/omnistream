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

#ifndef OMNIFLINK_SOURCECONTEXT_H
#define OMNIFLINK_SOURCECONTEXT_H
#include "basictypes/Object.h"
#include "../../streaming/api/watermark/Watermark.h"
class SourceContext {
public:
    virtual ~SourceContext() = default;

    // Collect method
    virtual void collect(void* element) = 0;

    virtual void collectWithTimestamp(void* var1, int64_t var2) = 0;

    virtual void emitWatermark(Watermark* var1) = 0;

    virtual void markAsTemporarilyIdle() = 0;

    virtual void close() = 0;

    virtual Object* getCheckpointLock() = 0;
};

#endif // OMNIFLINK_SOURCECONTEXT_H
