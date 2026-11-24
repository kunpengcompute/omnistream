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

#ifndef BUFFERBUILDER_H
#define BUFFERBUILDER_H

#include <memory>
#include <string>
#include <atomic>

#include "Buffer.h"
#include "BufferConsumer.h"
#include "PositionMarker.h"
// Inline required for performance
namespace omnistream {

class BufferBuilder {
public:
    explicit BufferBuilder(Buffer* buffer) : buffer(buffer)
    {
        positionMarker = new SettablePositionMarker();
        maxCapacity = buffer->GetMaxCapacity();
    }

    virtual ~BufferBuilder()
    {
        if (positionMarker) {
            positionMarker->release();
            positionMarker = nullptr;
        }
    }

    virtual std::shared_ptr<BufferConsumer> createBufferConsumerFromBeginning() = 0;
    virtual std::shared_ptr<BufferConsumer> createBufferConsumer(int currentReaderPosition) = 0;

    inline std::shared_ptr<BufferConsumer> createBufferConsumer();
    virtual int appendAndCommit(void* source) = 0;
    inline void commit();
    inline int finish();
    inline bool isFinished();
    inline bool isFull();
    inline int getWritableBytes();
    inline int getMaxCapacity() const;
    inline void trim(int newSize);
    inline void close();
    virtual std::string toString() = 0;
    virtual Segment* GetSegment() = 0;

protected:
    class alignas(128) SettablePositionMarker final : public PositionMarker {
    public:
        SettablePositionMarker() : position(0), cachedPosition(0)
        {
        }
        inline int get() const override;
        inline bool isFinished() const;
        inline int getCached() const;
        inline int markFinished();
        inline void move(int offset);
        inline void set(int value);
        inline void commit();

        std::atomic<int> position = 0;
        int cachedPosition = 0;
    };

    SettablePositionMarker* positionMarker;
    Buffer* buffer;
    int maxCapacity;
    bool bufferConsumerCreated = false;
    int count = 0;
};

inline std::shared_ptr<BufferConsumer> BufferBuilder::createBufferConsumer()
{
    return createBufferConsumer(positionMarker->cachedPosition);
}

inline void BufferBuilder::commit()
{
    positionMarker->commit();
}

inline int BufferBuilder::finish()
{
    int writtenBytes = positionMarker->markFinished();
    commit();
    return writtenBytes;
}

inline bool BufferBuilder::isFinished()
{
    return positionMarker->isFinished();
}

inline bool BufferBuilder::isFull()
{
    int cached = positionMarker->getCached();
    int capacity = getMaxCapacity();
    LOG_PART("BufferBuilder::isFull GetMaxCapacity : " << capacity);
    LOG_PART("BufferBuilder::isFull positionMarker->getCached() : " << cached);
    if (unlikely(cached > capacity)) {
        throw std::runtime_error("Cached position exceeds max capacity");
    }
    return cached == capacity;
}

inline int BufferBuilder::getWritableBytes()
{
    return getMaxCapacity() - positionMarker->getCached();
}

inline int BufferBuilder::getMaxCapacity() const
{
    return maxCapacity;
}

inline void BufferBuilder::trim(int newSize)
{
    maxCapacity = std::min(std::max(newSize, positionMarker->getCached()), buffer->GetMaxCapacity());
}

inline void BufferBuilder::close()
{
    if (buffer == nullptr) {
        THROW_LOGIC_EXCEPTION("BufferBuilder::close(), buffer is nullptr");
    }
    buffer->RecycleBuffer();
    buffer = nullptr;
}

inline bool BufferBuilder::SettablePositionMarker::isFinished() const
{
    return PositionMarker::isFinished(cachedPosition);
}

inline int BufferBuilder::SettablePositionMarker::get() const
{
    return position.load(std::memory_order_acquire);
}

inline int BufferBuilder::SettablePositionMarker::getCached() const
{
    return PositionMarker::getAbsolute(cachedPosition);
}

inline int BufferBuilder::SettablePositionMarker::markFinished()
{
    int currentPosition = getCached();
    int newValue = -currentPosition;
    if (newValue == 0) {
        newValue = FINISHED_EMPTY;
    }
    set(newValue);
    return currentPosition;
}

inline void BufferBuilder::SettablePositionMarker::move(int offset)
{
    set(cachedPosition + offset);
}

inline void BufferBuilder::SettablePositionMarker::set(int value)
{
    cachedPosition = value;
}

inline void BufferBuilder::SettablePositionMarker::commit()
{
    position.store(cachedPosition, std::memory_order_release);
}
} // namespace omnistream

#endif // BUFFERBUILDER_H
