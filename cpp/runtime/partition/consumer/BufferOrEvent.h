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

// BufferOrEvent.h
#ifndef OMNISTREAM_BUFFEROREVENT_H
#define OMNISTREAM_BUFFEROREVENT_H

#include <memory>
#include <string>
#include <buffer/ObjectBuffer.h>
#include <event/AbstractEvent.h>

#include "InputChannelInfo.h"

namespace omnistream {

class BufferOrEvent {
public:
    BufferOrEvent(Buffer* buffer, InputChannelInfo channelInfo, bool moreAvailable, bool morePriorityEvents);

    BufferOrEvent(
        std::shared_ptr<AbstractEvent>,
        bool hasPriority,
        InputChannelInfo channelInfo,
        bool moreAvailable,
        int size,
        bool morePriorityEvents);

    // Visible for testing
    BufferOrEvent(Buffer* buffer, InputChannelInfo channelInfo);

    // Visible for testing
    BufferOrEvent(std::shared_ptr<AbstractEvent>, InputChannelInfo channelInfo);

    bool isBuffer() const;
    bool isEvent() const;

    Buffer* getBuffer() const;
    std::shared_ptr<AbstractEvent> getEvent() const;
    InputChannelInfo getChannelInfo() const;

    void setChannelInfo(InputChannelInfo channelInfo);

    bool moreAvailable() const;
    bool morePriorityEvents() const;

    std::string toString() const;

    void setMoreAvailable(bool moreAvailable);

    int getSize() const;
    bool hasPriority() const;

private:
    Buffer* buffer_;
    std::shared_ptr<AbstractEvent> event_;
    bool hasPriority_;
    InputChannelInfo channelInfo_;
    int size_;
    bool moreAvailable_;
    bool morePriorityEvents_;
};

} // namespace omnistream

#endif // OMNISTREAM_BUFFEROREVENT_H
