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

#ifndef OMNISTREAM_OBJECTBUFFER_H
#define OMNISTREAM_OBJECTBUFFER_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <common.h>

/**
#include <core/AbstractEvent.h>
#include <io/CheckpointBarrier.h>
#include <io/EndOfChannelStateEvent.h>
#include <io/ObjectBufferRecycler.h>
**/

#include "Buffer.h"
#include "ObjectBufferDataType.h"
#include "ObjectBufferRecycler.h"

namespace omnistream {
class ObjectBuffer : public Buffer {
public:
    ~ObjectBuffer() override = default;

    int EventType() const override
    {
        return 0;
    };

    Segment* GetSegment() override
    {
        return GetObjectSegment();
    }

    virtual ObjectSegment* GetObjectSegment() = 0;
    virtual std::pair<uint8_t*, size_t> GetBytes() = 0;

    int GetOffset() const override
    {
        return 0;
    };

protected:
    ObjectBuffer() = default;
};

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFER_H
