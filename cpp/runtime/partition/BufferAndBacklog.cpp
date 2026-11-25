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

// BufferAndBacklog.cpp
#include "BufferAndBacklog.h"
#include <sstream>

namespace omnistream {

BufferAndBacklog::BufferAndBacklog()
    : buffer(nullptr),
      buffersInBacklog(0),
      nextDataType(ObjectBufferDataType::NONE),
      sequenceNumber(0)
{
}

BufferAndBacklog::BufferAndBacklog(
    Buffer* buffer, int buffersInBacklog, const ObjectBufferDataType& nextDataType, int sequenceNumber)
    : buffersInBacklog(buffersInBacklog),
      nextDataType(nextDataType),
      sequenceNumber(sequenceNumber)
{
    if (buffer == nullptr) {
        THROW_RUNTIME_ERROR(" pass in buffer is null");
    }
    this->buffer = buffer;
}

BufferAndBacklog::~BufferAndBacklog()
{
}

Buffer* BufferAndBacklog::getBuffer() const
{
    return buffer;
}

int BufferAndBacklog::getBuffersInBacklog() const
{
    return buffersInBacklog;
}

ObjectBufferDataType BufferAndBacklog::getNextDataType() const
{
    return nextDataType;
}

int BufferAndBacklog::getSequenceNumber() const
{
    return sequenceNumber;
}

bool BufferAndBacklog::isDataAvailable() const
{
    return nextDataType != ObjectBufferDataType::NONE;
}

bool BufferAndBacklog::isEventAvailable() const
{
    return nextDataType.isEvent(); // Assuming VectorBatchBuffer::DataType::EVENT is defined for event type
}

BufferAndBacklog BufferAndBacklog::fromBufferAndLookahead(
    Buffer* current, const ObjectBufferDataType& nextDataType, int backlog, int sequenceNumber_)
{
    return BufferAndBacklog(current, backlog, nextDataType, sequenceNumber_);
}

std::string BufferAndBacklog::toString() const
{
    std::stringstream ss;
    ss << "BufferAndBacklog{"
       << "buffer=" << (buffer ? "present" : "nullptr") // Indicate if buffer is present
       << ", buffersInBacklog=" << buffersInBacklog << ", nextDataType=" << nextDataType.toString() //
       << ", sequenceNumber=" << sequenceNumber << "}";
    return ss.str();
}

} // namespace omnistream
