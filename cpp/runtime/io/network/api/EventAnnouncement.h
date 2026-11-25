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

#ifndef OMNISTREAM_EVENTANNOUNCEMENT_H
#define OMNISTREAM_EVENTANNOUNCEMENT_H

#pragma once

#include <memory>
#include <string>
#include "../../../event/RuntimeEvent.h"

namespace omnistream {

/**
 * EventAnnouncement is used by timeoutable aligned checkpoint barriers.
 * It announces an upcoming event together with the sequence number at which the announced
 * event will appear in the input channel.
 *
 * This is needed so that, upon aligned timeout, the runtime can "overtake" the announced
 * barrier by converting it into a priority event at the correct position.
 */
class EventAnnouncement : public RuntimeEvent {
public:
    EventAnnouncement(std::shared_ptr<AbstractEvent> announcedEvent, int sequenceNumber)
        : announcedEvent_(std::move(announcedEvent)),
          sequenceNumber_(sequenceNumber)
    {
    }

    std::shared_ptr<AbstractEvent> GetAnnouncedEvent() const
    {
        return announcedEvent_;
    }
    int GetSequenceNumber() const
    {
        return sequenceNumber_;
    }

    std::string GetEventClassName() override
    {
        return "EventAnnouncement";
    }

private:
    std::shared_ptr<AbstractEvent> announcedEvent_;
    int sequenceNumber_;
};

} // namespace omnistream

#endif // OMNISTREAM_EVENTANNOUNCEMENT_H
