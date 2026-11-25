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

#ifndef VECTORBATCHBUFFER_H
#define VECTORBATCHBUFFER_H
#include <atomic>

#include "ObjectBuffer.h"

namespace omnistream {
class VectorBatchBuffer : public ObjectBuffer {
public:
    VectorBatchBuffer(ObjectSegment* segment, std::shared_ptr<BufferRecycler> recycler);

    explicit VectorBatchBuffer(ObjectSegment* segment) : objectSegment(segment), recycler(nullptr)
    {
        bufferType = 0;
        event_type = -1;
        readerIndex_ = -1;
        isCompressed_ = false;
        refCount_.store(1, std::memory_order_relaxed);
    }

    explicit VectorBatchBuffer(std::shared_ptr<ObjectSegment> segment, std::shared_ptr<BufferRecycler> recycler)
        : objectSegment(segment.get()),
          ownedSegment_(std::move(segment)),
          recycler(std::move(recycler))
    {
        bufferType = 0;
        event_type = -1;
        readerIndex_ = -1;
        isCompressed_ = false;
        refCount_.store(1, std::memory_order_relaxed);
    }

    explicit VectorBatchBuffer(int event_)
        : objectSegment(nullptr),
          recycler(nullptr),
          isCompressed_(false),
          readerIndex_(-1)
    {
        // only use for event type
        bufferType = 1;
        event_type = event_;
        currentSize = 1;
        refCount_.store(1, std::memory_order_relaxed);
    }

    ~VectorBatchBuffer() override = default;

    bool isBuffer() const override
    {
        return bufferType == 0;
    }

    void RecycleBuffer() override;

    void RecycleBytes(int64_t bytes);

    bool IsRecycled() const
    {
        return isRecycled_.load(std::memory_order_acquire);
    }

    Buffer* RetainBuffer() override
    {
        LOG_TRACE("retain ");
        int current = refCount_.load(std::memory_order_relaxed);
        while (current > 0) {
            if (refCount_.compare_exchange_weak(
                    current, current + 1, std::memory_order_relaxed, std::memory_order_acquire)) {
                return this;
            }
        }
        throw std::runtime_error("RetainBuffer on a released VectorBatchBuffer");
    }

    Buffer* ReadOnlySlice() override
    {
        LOG_TRACE("ReadOnlySlice  ");
        return this;
    }

    Buffer* ReadOnlySlice(int index, int length) override;

    int GetMaxCapacity() const override
    {
        return objectSegment->getSize();
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
        if (bufferType == 0) {
            return ObjectBufferDataType::DATA_BUFFER;
        } else {
            return ObjectBufferDataType::EVENT_BUFFER;
        }
    }

    void SetDataType(ObjectBufferDataType dataType) override {};

    int RefCount() const override
    {
        return refCount_.load(std::memory_order_relaxed);
    }

    std::string ToDebugString(bool includeHash) const override
    {
        std::stringstream ss;
        ss << "buffertype =" << std::to_string(bufferType) << ", event_type " << std::to_string(event_type);
        return ss.str();
    };

    ObjectSegment* GetObjectSegment() override;
    std::shared_ptr<BufferRecycler> GetRecycler() override;

    std::pair<uint8_t*, size_t> GetBytes() override
    {
        if (objectSegment == nullptr) {
            return {nullptr, 0};
        }
        return {objectSegment->getData(), objectSegment->getDataLength()};
    };

    [[nodiscard]] int EventType() const override
    {
        return event_type;
    }

    int GetBufferType() override
    {
        return bufferType;
    }

protected:
    void recycleBuffer(bool selfDelete);

private:
    void recycleRemainingBytes();

    ObjectSegment* objectSegment;
    std::shared_ptr<ObjectSegment> ownedSegment_;
    std::shared_ptr<BufferRecycler> recycler;
    // ObjectBufferDataType dataType;
    int bufferType; // 0 vectorbatch, 1. event  for now

    // workaround  event buffer, objecgtsegment == nulllptr, recyler = nullptr
    int event_type;

    int currentSize = 0;
    bool isCompressed_;
    std::atomic<bool> isRecycled_{false};
    int readerIndex_;

    std::atomic<int> refCount_{0};
    std::atomic<int64_t> accountedBytes_{0};
};

} // namespace omnistream

#endif // VECTORBATCHBUFFER_H
