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

#ifndef EVENTBUFFER_H
#define EVENTBUFFER_H
#include <common.h>

#include "ObjectBuffer.h"

namespace omnistream {
class EventBuffer : public ObjectBuffer {
public:
    explicit EventBuffer(int event) : event_(event)
    {
        LOG_TRACE("constructor");
        readerIndex_ = 0;
        isCompressed_ = false;
    };
    ~EventBuffer() override
    {
        LOG_TRACE("destrucor");
    };

public:
    bool isBuffer() const override
    {
        return false;
    }

    void RecycleBuffer() override
    {
    }

    Buffer* RetainBuffer() override
    {
        LOG("EventBuffer::RetainBuffer");
        return this;
    }

    Buffer* ReadOnlySlice() override
    {
        LOG("EventBuffer::ReadOnlySlice");
        return this;
    }

    Buffer* ReadOnlySlice(int index, int length) override
    {
        LOG(">>>>");
        return new EventBuffer(event_);
    }

    int GetMaxCapacity() const override
    {
        return 0;
    }

    int GetReaderIndex() const override
    {
        return readerIndex_;
    }

    void SetReaderIndex(int readerIndex) override
    {
        readerIndex_ = readerIndex;
    }

    int GetSize() const override
    {
        return currentSize;
    }

    int GetBufferType() override
    {
        return 0;
    }

    void SetSize(int writerIndex) override
    {
        currentSize = writerIndex;
    }

    int ReadableObjects() const override
    {
        return 0;
    }

    bool IsCompressed() const override
    {
        return false;
    }

    void SetCompressed(bool isCompressed) override
    {
        isCompressed_ = isCompressed;
    }

    ObjectBufferDataType GetDataType() const override
    {
        NOT_IMPL_EXCEPTION;
    };

    void SetDataType(ObjectBufferDataType dataType) override {};

    int RefCount() const override
    {
        LOG_TRACE("ref cnt is 0");
        return 0;
    }

    std::string ToDebugString(bool includeHash) const override
    {
        return {};
    };

    ObjectSegment* GetObjectSegment() override
    {
        NOT_IMPL_EXCEPTION;
    };

    std::shared_ptr<BufferRecycler> GetRecycler() override
    {
        NOT_IMPL_EXCEPTION;
    };

    std::pair<uint8_t*, size_t> GetBytes() override
    {
        return std::make_pair(reinterpret_cast<uint8_t*>(&event_), sizeof(event_));
    };

private:
    std::shared_ptr<ObjectBufferRecycler> recycler = nullptr;
    // DataType dataType;
    int currentSize = 1;
    bool isCompressed_;
    int readerIndex_;
    int event_; //
};
} // namespace omnistream

#endif
