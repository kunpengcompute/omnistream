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

// BufferOrEvent.cpp
#include "BufferOrEvent.h"

#include <sstream>
#include <stdexcept>
#include "InputChannelInfo.h"

#include "io/network/api/serialization/EventSerializer.h"

namespace omnistream {

BufferOrEvent::BufferOrEvent(Buffer* buffer, InputChannelInfo channelInfo, bool moreAvailable, bool morePriorityEvents)
    : buffer_(buffer),
      event_(nullptr),
      hasPriority_(false),
      channelInfo_(channelInfo),
      size_(buffer ? buffer->GetSize() : 0),
      moreAvailable_(moreAvailable),
      morePriorityEvents_(morePriorityEvents)
{
    if (!buffer) {
        throw std::invalid_argument("Buffer cannot be null");
    }
}

BufferOrEvent::BufferOrEvent(
    std::shared_ptr<AbstractEvent> event,
    bool hasPriority,
    InputChannelInfo channelInfo,
    bool moreAvailable,
    int size,
    bool morePriorityEvents)
    : buffer_(nullptr),
      event_(event),
      hasPriority_(hasPriority),
      channelInfo_(channelInfo),
      size_(size),
      moreAvailable_(moreAvailable),
      morePriorityEvents_(morePriorityEvents)
{
}

// Visible for testing
BufferOrEvent::BufferOrEvent(Buffer* buffer, InputChannelInfo channelInfo)
    : BufferOrEvent(buffer, channelInfo, true, false)
{
}

// Visible for testing
BufferOrEvent::BufferOrEvent(std::shared_ptr<AbstractEvent> event, InputChannelInfo channelInfo)
    : BufferOrEvent(event, false, channelInfo, true, 0, false)
{
}

bool BufferOrEvent::isBuffer() const
{
    return buffer_ != nullptr;
}

bool BufferOrEvent::isEvent() const
{
    return event_ != nullptr;
}

Buffer* BufferOrEvent::getBuffer() const
{
    return buffer_;
}

std::shared_ptr<AbstractEvent> BufferOrEvent::getEvent() const
{
    return event_;
}

InputChannelInfo BufferOrEvent::getChannelInfo() const
{
    return channelInfo_;
}

void BufferOrEvent::setChannelInfo(InputChannelInfo channelInfo)
{
    channelInfo_ = channelInfo;
}

bool BufferOrEvent::moreAvailable() const
{
    return moreAvailable_;
}

bool BufferOrEvent::morePriorityEvents() const
{
    return morePriorityEvents_;
}

std::string BufferOrEvent::toString() const
{
    std::stringstream ss;
    ss << "BufferOrEvent [";
    if (isBuffer()) {
        ss << "ObjectBuffer";
    } else {
        ss << (event_ ? "AbstractEvent (prio=" + std::to_string(hasPriority_) + ")" : "null event");
    }
    ss << ", channelInfo = ";

    ss << "InputChannelInfo" << channelInfo_.toString();

    ss << ", size = " << size_ << "]";
    return ss.str();
}

void BufferOrEvent::setMoreAvailable(bool moreAvailable)
{
    moreAvailable_ = moreAvailable;
}

int BufferOrEvent::getSize() const
{
    return size_;
}

bool BufferOrEvent::hasPriority() const
{
    return hasPriority_;
}

} // namespace omnistream
