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

#include "BufferConsumer.h"

#include <cstdint>

namespace omnistream {
Buffer* BufferConsumer::requireBuffer(const char* method) const
{
    (void)method;
    if (buffer == nullptr) {
        throw std::runtime_error("BufferConsumer access after close");
    }
    return buffer;
}

bool BufferConsumer::isFinished() const
{
    return writerPosition->isFinished();
}

void BufferConsumer::skip(int bytesToSkip)
{
    writerPosition->update();
    int cachedWriterPosition = writerPosition->getCached();
    int bytesReadable = cachedWriterPosition - currentReaderPosition;
    if (bytesToSkip > bytesReadable) {
        throw std::runtime_error("bytes to skip beyond readable range");
    }
    currentReaderPosition += bytesToSkip;
}

int BufferConsumer::getWrittenBytes()
{
    return writerPosition->getCached();
}

int BufferConsumer::getCurrentReaderPosition() const
{
    return currentReaderPosition;
}

bool BufferConsumer::isDataAvailable()
{
    return currentReaderPosition < writerPosition->getLatest();
}

int BufferConsumer::getBufferType()
{
    return requireBuffer("getBufferType")->GetBufferType();
}

int BufferConsumer::getBufferSize() const
{
    return requireBuffer("getBufferSize")->GetMaxCapacity();
}

bool BufferConsumer::isClose() const
{
    return isStop;
}

void BufferConsumer::close()
{
    if (buffer == nullptr) {
        THROW_LOGIC_EXCEPTION("BufferConsumer::close(), buffer is nullptr");
    }
    buffer->RecycleBuffer();
    buffer = nullptr;
    isStop = true;
}

bool BufferConsumer::isBuffer() const
{
    return requireBuffer("isBuffer")->isBuffer();
}

ObjectBufferDataType BufferConsumer::getDataType() const
{
    return requireBuffer("getDataType")->GetDataType();
}

void BufferConsumer::SetDataType(const ObjectBufferDataType& dataType)
{
    requireBuffer("SetDataType")->SetDataType(dataType);
}

} // namespace omnistream
