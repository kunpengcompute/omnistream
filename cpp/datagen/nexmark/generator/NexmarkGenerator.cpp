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

#include "NexmarkGenerator.h"

bool NexmarkGenerator::NextEvent::operator==(const NexmarkGenerator::NextEvent& other) const
{
    return (
        wallclockTimestamp == other.wallclockTimestamp && eventTimestamp == other.eventTimestamp &&
        watermark == other.watermark && event == other.event);
}

int NexmarkGenerator::NextEvent::hashCode() const
{
    std::hash<int64_t> intHasher;
    return static_cast<int>(
        intHasher(wallclockTimestamp) ^ intHasher(eventTimestamp) ^ intHasher(watermark) ^ event->hash());
}

bool NexmarkGenerator::NextEvent::operator<(const NexmarkGenerator::NextEvent& other) const
{
    if (wallclockTimestamp < other.wallclockTimestamp) {
        return true;
    } else if (wallclockTimestamp > other.wallclockTimestamp) {
        return false;
    } else {
        return event->hash() < other.event->hash();
    }
}

GeneratorConfig NexmarkGenerator::splitAtEventId(int64_t eventId)
{
    int64_t newMaxEvents = eventId - (config.firstEventId + config.firstEventNumber);
    GeneratorConfig remainConfig = config.copyWithConfig(
        config.firstEventId, config.maxEvents - newMaxEvents, config.firstEventNumber + newMaxEvents);
    config = config.copyWithConfig(config.firstEventId, newMaxEvents, config.firstEventNumber);
    return remainConfig;
}

NexmarkGenerator::NextEvent NexmarkGenerator::nextEvent()
{
    if (wallclockBaseTime < 0) {
        // Get current time in milliseconds.
        wallclockBaseTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
    }
    // When, in event time, we should generate the event. Monotonic.
    int64_t eventTimestamp = config.timestampForEvent(config.nextEventNumber(eventsCountSoFar));
    // When, in event time, the event should say it was generated. Depending on outOfOrderGroupSize may have local
    // jitter.
    int64_t adjustedEventTimestamp = config.timestampForEvent(config.nextAdjustedEventNumber(eventsCountSoFar));
    // The minimum of this and all future adjusted event timestamps. Accounts for jitter in the event timestamp.
    int64_t watermark = config.timestampForEvent(config.nextEventNumberForWatermark(eventsCountSoFar));
    // When, in wallclock time, we should emit the event.
    int64_t wallclockTimestamp = wallclockBaseTime + (eventTimestamp - config.baseTime);

    int64_t newEventId = getNextEventId();
    int64_t rem = newEventId % config.totalProportion;

    std::unique_ptr<Event> event;
    if (rem < config.personProportion) {
        event = personGenerator.nextPerson(newEventId, adjustedEventTimestamp, config);
    } else if (rem < config.personProportion + config.auctionProportion) {
        event = auctionGenerator.nextAuction(eventsCountSoFar, newEventId, adjustedEventTimestamp, config);
    } else {
        event = bidGenerator.nextBid(newEventId, adjustedEventTimestamp, config);
    }

    eventsCountSoFar++;
    return NextEvent(wallclockTimestamp, adjustedEventTimestamp, std::move(event), watermark);
}
