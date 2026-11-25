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

#include "MemoryBufferBuilder.h"

#include <core/utils/ByteBuffer.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>

namespace omnistream::datastream {
MemoryBufferBuilder::MemoryBufferBuilder(MemorySegment* memorySegment, std::shared_ptr<BufferRecycler> recycler)
    : BufferBuilder(new NetworkBuffer(memorySegment, recycler)),
      memorySegment(memorySegment)
{
    positionMarker->addRef();
    auto* marker = positionMarker;
    taskId = TimerThreadPool::GetTimerThreadPoolInstance()->addPeriodicTask(200, [marker]() { marker->commit(); });
}

MemoryBufferBuilder::~MemoryBufferBuilder()
{
    TimerThreadPool::GetTimerThreadPoolInstance()->cancel(taskId);
    positionMarker->release();
}

int MemoryBufferBuilder::appendAndCommit(void* source)
{
    int writtenBytes = append(source);
    commitCount++;
    if (commitCount > MAX_COMMIT_COUNT) {
        commit();
        commitCount = 0;
    }
    return writtenBytes;
}

int MemoryBufferBuilder::append(void* source)
{
    if (isFinished()) {
        throw std::runtime_error("BufferBuilder is finished");
    }
    LOG_PART(
        " Put a record to buffer builder :" << this << " at positionMarker->getCached()"
                                            << positionMarker->getCached());

    auto record = reinterpret_cast<StreamRecord*>(source);
    auto value = reinterpret_cast<ByteBuffer*>(record->getValue());

    int needed = value->remaining();
    int maxCapacity = getMaxCapacity();
    int cached = positionMarker->getCached();
    int available = maxCapacity - cached;
    int toCopy = std::min(needed, available);

    LOG("put source to memorySegment");
    int position_ = value->position();
    memorySegment->put(cached, value->getValue(), position_, toCopy);
    value->setPosition(position_ + toCopy);

    LOG("toCopy is " << toCopy << " current mark is " << positionMarker->getCached() << " value position is "
                     << value->position());
    positionMarker->move(toCopy);
    return toCopy;
}

int MemoryBufferBuilder::appendRawBytes(const uint8_t* source, int length)
{
    if (isFinished()) {
        throw std::runtime_error("BufferBuilder is finished");
    }
    if (length < 0) {
        throw std::invalid_argument("length must be non-negative");
    }
    if (length == 0) {
        return 0;
    }
    if (source == nullptr) {
        throw std::invalid_argument("source must not be null when length > 0");
    }

    int available = getMaxCapacity() - positionMarker->getCached();
    int toCopy = std::min(length, available);
    if (toCopy <= 0) {
        return 0;
    }

    memorySegment->put(positionMarker->getCached(), source, 0, toCopy);
    positionMarker->move(toCopy);
    commit();
    return toCopy;
}

std::shared_ptr<BufferConsumer> MemoryBufferBuilder::createBufferConsumerFromBeginning()
{
    return createBufferConsumer(0);
}

std::shared_ptr<BufferConsumer> MemoryBufferBuilder::createBufferConsumer(int currentReaderPosition)
{
    if (bufferConsumerCreated) {
        throw std::runtime_error("Two BufferConsumer shouldn't exist for one BufferBuilder");
    }
    bufferConsumerCreated = true;
    positionMarker->addRef();
    return std::make_shared<MemoryBufferConsumer>(
        reinterpret_cast<NetworkBuffer*>(buffer->RetainBuffer()), positionMarker, currentReaderPosition);
}

std::string MemoryBufferBuilder::toString()
{
    std::stringstream ss;
    ss << "ObjectBufferBuilder{maxCapacity=" << maxCapacity << ", committedBytes=" << positionMarker->getCached()
       << ", finished=" << isFinished() << "}";
    return ss.str();
}
Segment* MemoryBufferBuilder::GetSegment()
{
    return memorySegment;
}
} // namespace omnistream::datastream
