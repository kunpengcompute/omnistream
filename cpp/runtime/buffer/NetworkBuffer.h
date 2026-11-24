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

#ifndef NETWORKBUFFER_H
#define NETWORKBUFFER_H

#include <memory>
#include <mutex>
#include <stdexcept>
#include <cstring>
#include <cstdlib>

#include "Buffer.h"
#include "ObjectBuffer.h"
#include "core/memory/MemorySegment.h"

using namespace omnistream;
// check
namespace omnistream::datastream {

class NetworkBuffer : public Buffer {
public:
    NetworkBuffer(MemorySegment* memorySegment, std::shared_ptr<BufferRecycler> recycler, bool segmentOwner = false);
    NetworkBuffer(
        MemorySegment* memorySegment,
        int bufferLength,
        int readIndex,
        std::shared_ptr<BufferRecycler> recycler,
        int bufferType,
        bool segmentOwner = false);

    NetworkBuffer(
        MemorySegment* memorySegment,
        int bufferLength,
        int readIndex,
        std::shared_ptr<BufferRecycler> recycler,
        bool segmentOwner = false);

    NetworkBuffer(
        MemorySegment* memorySegment,
        int bufferLength,
        int readIndex,
        std::shared_ptr<BufferRecycler> recycler,
        ObjectBufferDataType dataType,
        bool segmentOwner = false);
    // todo: check whether use
    explicit NetworkBuffer(MemorySegment* memorySegment, bool segmentOwner = false)
        : NetworkBuffer(memorySegment, nullptr, segmentOwner)
    {
    }

    // todo: check whether use
    explicit NetworkBuffer(int event_) : NetworkBuffer(nullptr)
    {
        bufferType = 1;
        event_type = event_;
        currentSize = 1;
        dataType = ObjectBufferDataType::EVENT_BUFFER;
    }

    ~NetworkBuffer() override
    {
        if (segmentOwner) {
            delete memorySegment;
        }
    }

    bool isBuffer() const override
    {
        return dataType.isBuffer();
    }

    void RecycleBuffer() override
    {
        // data buffer has recyler, event buffer does not
        if (recycler == nullptr) {
            return;
        }

        int prev = refCount_.fetch_sub(1);
        if (prev <= 0) {
            THROW_LOGIC_EXCEPTION("NetworkBuffer::RecycleBuffer() prev <= 0");
        }
        if (prev != 1) {
            return;
        }
        recycler->recycle(this->getMemorySegment());
        delete this;
    }

    Buffer* RetainBuffer() override
    {
        int prev = refCount_.fetch_add(1);
        if (prev <= 0) {
            THROW_LOGIC_EXCEPTION("NetworkBuffer::RetainBuffer() prev <= 0");
        }
        return this;
    }

    Buffer* ReadOnlySlice() override
    {
        LOG("EventBuffer::ReadOnlySlice");
        return ReadOnlySlice(GetReaderIndex(), GetSize() - GetReaderIndex());
    }

    Buffer* ReadOnlySlice(int index, int length) override;

    int GetMaxCapacity() const override
    {
        return memorySegment->getSize();
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
        return dataType;
    }

    void SetDataType(ObjectBufferDataType dataType_) override
    {
        this->dataType = dataType_;
    };

    int RefCount() const override
    {
        return refCount_.load();
    }

    std::string ToDebugString(bool includeHash) const override
    {
        std::stringstream ss;
        ss << "buffertype =" << std::to_string(bufferType) << ", event_type " << std::to_string(event_type);
        return ss.str();
    };

    Segment* GetSegment() override
    {
        return getMemorySegment();
    }

    MemorySegment* getMemorySegment();
    std::shared_ptr<BufferRecycler> GetRecycler() override;

    static std::pair<uint8_t*, size_t> GetBytes()
    {
        NOT_IMPL_EXCEPTION;
    };

    [[nodiscard]] int EventType() const override
    {
        return event_type;
    }

    int GetBufferType() override
    {
        return bufferType;
    }

    int GetOffset() const override
    {
        return 0;
    };

    virtual int GetMemorySegmentOffset() const
    {
        return 0;
    }

    void SetBufferType(int bufferType_)
    {
        bufferType = bufferType_;
        if (bufferType_ == 1) {
            dataType = ObjectBufferDataType::EVENT_BUFFER;
        }
    }

private:
    MemorySegment* memorySegment;
    std::shared_ptr<BufferRecycler> recycler;

    int bufferType; // 0 buffer, 1. event  for now
    int event_type;

    int currentSize;
    bool isCompressed_ = false;
    int readerIndex_;

    std::atomic<int> refCount_ = 1;
    ObjectBufferDataType dataType = ObjectBufferDataType::DATA_BUFFER;
    bool segmentOwner = false;
};

enum class DataType {
    NONE,
    DATA_BUFFER,
    EVENT_BUFFER,
    PRIORITIZED_EVENT_BUFFER,
    ALIGNED_CHECKPOINT_BARRIER,
    TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER,
    RECOVERY_COMPLETION
};
} // namespace omnistream::datastream

#endif // NETWORKBUFFER_H
