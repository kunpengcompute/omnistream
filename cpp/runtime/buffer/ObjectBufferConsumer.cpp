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

// BufferConsumer.cpp
#include "ObjectBufferConsumer.h"

#include <common.h>
#include <sstream>
#include <stdexcept>

#include "EventBuffer.h"
#include "VectorBatchBuffer.h"

namespace omnistream {

ObjectBufferConsumer::ObjectBufferConsumer(VectorBatchBuffer* buffer, int size)
    : ObjectBufferConsumer(buffer, new FixedSizePositionMarker(-size), 0)
{
    LOG_TRACE("inside constructor two parameter");
    if (!isFinished()) {
        THROW_LOGIC_EXCEPTION("BufferConsumer with static size must be finished after construction!");
    }
}

ObjectBufferConsumer::ObjectBufferConsumer(
    VectorBatchBuffer* buffer_, PositionMarker* currentWriterPosition, int currentReaderPosition)
    : BufferConsumer(buffer_, currentWriterPosition, currentReaderPosition)
{
    LOG("ObjectBufferConsumer init will running");
    if (currentReaderPosition > this->writerPosition->getCached()) {
        THROW_LOGIC_EXCEPTION("Reader position larger than writer position");
    }
}

ObjectBufferConsumer::~ObjectBufferConsumer()
{
    LOG_TRACE("destruction ObjectBufferConsumer");
}

std::shared_ptr<BufferConsumer> ObjectBufferConsumer::copy()
{
    NOT_IMPL_EXCEPTION;
}

std::shared_ptr<BufferConsumer> ObjectBufferConsumer::copyWithReaderPosition(int readerPosition)
{
    NOT_IMPL_EXCEPTION;
}

Buffer* ObjectBufferConsumer::build()
{
    return buildVectorBatchBuffer();
}

VectorBatchBuffer* ObjectBufferConsumer::buildVectorBatchBuffer()
{
    LOG_TRACE("Starting Build...");
    Buffer* rawBuffer = requireBuffer("ObjectBufferConsumer::buildVectorBatchBuffer");
    LOG_TRACE("buffer internal " << rawBuffer->ToDebugString(false));
    if (rawBuffer->isBuffer()) {
        // vector batch
        VectorBatchBuffer* vbbuffer = dynamic_cast<VectorBatchBuffer*>(rawBuffer);
        writerPosition->update();
        int cachedWriterPosition = writerPosition->getCached();
        LOG("ObjectBufferConsumer::build() before get slice");
        LOG("buffer " << (vbbuffer->isBuffer() ? "buffer" : "event"));

        auto slice = vbbuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition);
        LOG("ObjectBufferConsumer::build() after get slice");
        slice->RetainBuffer();
        currentReaderPosition = cachedWriterPosition;

        VectorBatchBuffer* vbslice = dynamic_cast<VectorBatchBuffer*>(slice);
        return vbslice;
    } else {
        // event buffer
        LOG_TRACE("build event  buffer");
        auto vbbuffer = dynamic_cast<VectorBatchBuffer*>(rawBuffer);
        writerPosition->update();
        int cachedWriterPosition = writerPosition->getCached();
        LOG("ObjectBufferConsumer::build() before get slice");
        LOG("buffer " << (vbbuffer->isBuffer() ? "buffer" : "event"));
        ObjectBuffer* slice = dynamic_cast<ObjectBuffer*>(
            vbbuffer->ReadOnlySlice(currentReaderPosition, cachedWriterPosition - currentReaderPosition));
        LOG("ObjectBufferConsumer::build() after get slice");
        // todo: why not call slice->RetainBuffer();
        // slice->RetainBuffer();
        currentReaderPosition = cachedWriterPosition;
        //  NOT_IMPL_EXCEPTION
        VectorBatchBuffer* vbslice = dynamic_cast<VectorBatchBuffer*>(slice);
        return vbslice;
    }
}

bool ObjectBufferConsumer::isStartOfDataBuffer() const
{
    return requireBuffer("ObjectBufferConsumer::isStartOfDataBuffer")->GetDataType() ==
               ObjectBufferDataType::DATA_BUFFER &&
           currentReaderPosition == 0;
}

std::string ObjectBufferConsumer::toDebugString(bool includeHash)
{
    ObjectBuffer* tempBuffer;
    try {
        std::shared_ptr<ObjectBufferConsumer> copiedBufferConsumer =
            std::dynamic_pointer_cast<ObjectBufferConsumer>(copy());
        tempBuffer = dynamic_cast<ObjectBuffer*>(copiedBufferConsumer->build());
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

std::string ObjectBufferConsumer::toString()
{
    std::stringstream ss;
    ss << "BufferConsumer{buffer=" << (buffer ? "present" : "nullptr") << "buffer address" << buffer
       << ", writerPosition=" << writerPosition->getCached() << ", currentReaderPosition=" << currentReaderPosition
       << "}";
    return ss.str();
}

} // namespace omnistream
