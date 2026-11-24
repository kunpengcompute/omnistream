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

#include "MemoryBufferConsumer.h"

namespace omnistream::datastream {
MemoryBufferConsumer::MemoryBufferConsumer(NetworkBuffer* buffer, int size)
    : MemoryBufferConsumer(buffer, new FixedSizePositionMarker(-size), 0)
{
    LOG_TRACE("inside constructor two parameter");
    if (!isFinished()) {
        THROW_LOGIC_EXCEPTION("BufferConsumer with static size must be finished after construction!");
    }
}

MemoryBufferConsumer::MemoryBufferConsumer(
    NetworkBuffer* buffer_, PositionMarker* currentWriterPosition, int currentReaderPosition)
    : BufferConsumer(buffer_, currentWriterPosition, currentReaderPosition)
{
    LOG("ObjectBufferConsumer init will running");
    if (currentReaderPosition > this->writerPosition->getCached()) {
        THROW_LOGIC_EXCEPTION("Reader position larger than writer position");
    }
}

std::shared_ptr<BufferConsumer> MemoryBufferConsumer::copy()
{
    NOT_IMPL_EXCEPTION;
    //        std::shared_ptr<NetworkBuffer> networkBuffer =
    //        std::reinterpret_pointer_cast<NetworkBuffer>(buffer->RetainBuffer()); return
    //        std::make_shared<MemoryBufferConsumer>(networkBuffer, writerPosition->getInnerPositionMarker(),
    //        currentReaderPosition);
}

std::shared_ptr<BufferConsumer> MemoryBufferConsumer::copyWithReaderPosition(int readerPosition)
{
    NOT_IMPL_EXCEPTION;
    //        std::shared_ptr<NetworkBuffer> networkBuffer =
    //        std::reinterpret_pointer_cast<NetworkBuffer>(buffer->RetainBuffer()); return
    //        std::make_shared<MemoryBufferConsumer>(networkBuffer, writerPosition->getInnerPositionMarker(),
    //        readerPosition);
}

Buffer* MemoryBufferConsumer::build()
{
    return buildNetworkBuffer();
}

NetworkBuffer* MemoryBufferConsumer::buildNetworkBuffer()
{
    LOG_TRACE("Starting Build...");
    NetworkBuffer* networkBuffer =
        reinterpret_cast<NetworkBuffer*>(requireBuffer("MemoryBufferConsumer::buildNetworkBuffer"));
    writerPosition->update();
    int cachedWriterPosition = writerPosition->getCached();
    LOG("ObjectBufferConsumer::build() before get slice");
    LOG("buffer " << (networkBuffer->isBuffer() ? "buffer" : "event"));

    auto slice = networkBuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition);
    LOG("ObjectBufferConsumer::build() after get slice");
    currentReaderPosition = cachedWriterPosition;
    slice->RetainBuffer();

    return reinterpret_cast<NetworkBuffer*>(slice);
}

bool MemoryBufferConsumer::isStartOfDataBuffer() const
{
    return true;
}

std::string MemoryBufferConsumer::toDebugString(bool includeHash)
{
    NetworkBuffer* tempBuffer;
    try {
        std::shared_ptr<MemoryBufferConsumer> copiedBufferConsumer =
            std::dynamic_pointer_cast<MemoryBufferConsumer>(copy());
        tempBuffer = dynamic_cast<NetworkBuffer*>(copiedBufferConsumer->build());
        if (!copiedBufferConsumer->isFinished()) {
            throw std::runtime_error("copiedBufferConsumer is not finished");
        }
        return tempBuffer->ToDebugString(includeHash);
    } catch (...) {
        if (tempBuffer != nullptr) {
            tempBuffer->RecycleBuffer();
        }
        throw;
    }
}

std::string MemoryBufferConsumer::toString()
{
    std::stringstream ss;
    ss << "BufferConsumer{buffer=" << (buffer ? "present" : "nullptr") << "buffer address" << buffer
       << ", writerPosition=" << writerPosition->getCached() << ", currentReaderPosition=" << currentReaderPosition
       << "}";
    return ss.str();
}

} // namespace omnistream::datastream
