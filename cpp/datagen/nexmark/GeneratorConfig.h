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

#ifndef OMNISTREAM_GENERATORCONFIG_H
#define OMNISTREAM_GENERATORCONFIG_H

#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <limits>
#include <cstdint>
#include <functional>
#include <cmath>
#include "model/Event.h"
#include "NexmarkConfiguration.h"

class GeneratorConfig {
public:
    /**
     * We start the ids at specific values to help ensure the queries find a match even on small
     * synthesized dataset sizes.
     */
    static const int64_t FIRST_AUCTION_ID = 1000LL;

    static const int64_t FIRST_PERSON_ID = 1000LL;
    static const int64_t FIRST_CATEGORY_ID = 10LL;

    /** Proportions of people/auctions/bids to synthesize. */
    int personProportion;
    int auctionProportion;
    int bidProportion;
    int totalProportion;

    /** Environment options. */
public:
    /** Time for first event (ms since epoch). */
    int64_t baseTime;

    /**
     * Event id of first event to be generated. Event ids are unique over all generators, and are used
     * as a seed to generate each event's data.
     */
    int64_t firstEventId;

    /** Maximum number of events to generate. */
    int64_t maxEvents;

    /**
     * First event number. Generators running in parallel time may share the same event number, and
     * the event number is used to determine the event timestamp.
     */
    int64_t firstEventNumber;

    // If only the configuration is provided. First use default value. Then update value to new value in config.
    // If the other four field is provided together with config. Use the explicitly provided value.
    explicit GeneratorConfig(const NexmarkConfiguration& nexmarkConfig)
        : GeneratorConfig(nexmarkConfig, getNow(), 0, 0, 0)
    {
        if (nexmarkConfig.configuration.contains("configMap") && !nexmarkConfig.configuration["configMap"].is_null()) {
            auto configMap = nexmarkConfig.configuration["configMap"];
            SET_FIELD(baseTime);
            SET_FIELD(firstEventId)
            SET_FIELD(firstEventNumber)
            if (configMap.contains("maxEvents") && configMap["maxEvents"] != 0) {
                maxEvents = configMap["maxEvents"];
            }
        }
    }
    GeneratorConfig(
        const NexmarkConfiguration& configuration,
        int64_t baseTime,
        int64_t firstEventId,
        int64_t maxEventsOrZero,
        int64_t firstEventNumber)
        : personProportion(configuration.personProportion),
          auctionProportion(configuration.auctionProportion),
          bidProportion(configuration.bidProportion),
          totalProportion(
              configuration.auctionProportion + configuration.personProportion + configuration.bidProportion),
          baseTime(baseTime),
          firstEventId(firstEventId),
          // Passing 0 as ratePeriodSec as original value not provided.
          maxEvents(
              (maxEventsOrZero == 0)
                  ? std::numeric_limits<int64_t>::max() /
                        (totalProportion *
                         std::max(
                             std::max(configuration.avgPersonByteSize, configuration.avgAuctionByteSize),
                             configuration.avgBidByteSize))
                  : maxEventsOrZero),
          firstEventNumber(firstEventNumber),
          configuration(configuration),
          interEventDelayUs({1000000.0 / configuration.firstEventRate * configuration.numEventGenerators}),
          stepLengthSec(configuration.rateShape.stepLengthSec(0)),
          epochPeriodMs(0LL),
          eventsPerEpoch(0LL)
    {
    }

    inline long getNow() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }
    /** Return a copy of this config. */
    GeneratorConfig copy() const
    {
        GeneratorConfig result(configuration, baseTime, firstEventId, maxEvents, firstEventNumber);
        return result;
    }

    /**
     * Split this config into {@code n} sub-configs with roughly equal number of possible events, but
     * distinct value spaces. The generators will run on parallel timelines. This config should no
     * longer be used.
     */
    std::vector<GeneratorConfig> split(int n) const;

    /** Return copy of this config except with given parameters. */
    GeneratorConfig copyWithConfig(int64_t firstEventId_, int64_t maxEvents_, int64_t firstEventNumber_) const
    {
        return GeneratorConfig(configuration, baseTime, firstEventId_, maxEvents_, firstEventNumber_);
    }

    /** Return an estimate of the bytes needed by {@code numEvents}. */
    int64_t estimatedBytesForEvents(int64_t numEvents) const;

    int getAvgPersonByteSize() const
    {
        return configuration.avgPersonByteSize;
    }

    int getNumActivePeople() const
    {
        return configuration.numActivePeople;
    }

    int getHotSellersRatio() const
    {
        return configuration.hotSellersRatio;
    }

    int getNumInFlightAuctions() const
    {
        return configuration.numInFlightAuctions;
    }

    int getHotAuctionRatio() const
    {
        return configuration.hotAuctionRatio;
    }

    int getHotBiddersRatio() const
    {
        return configuration.hotBiddersRatio;
    }

    int getAvgBidByteSize() const
    {
        return configuration.avgBidByteSize;
    }

    int getAvgAuctionByteSize() const
    {
        return configuration.avgAuctionByteSize;
    }

    double getProbDelayedEvent() const
    {
        return configuration.probDelayedEvent;
    }

    long getOccasionalDelaySec() const
    {
        return configuration.occasionalDelaySec;
    }

    /** Return an estimate of the byte-size of all events a generator for this config would yield. */
    int64_t getEstimatedSizeBytes() const
    {
        return estimatedBytesForEvents(maxEvents);
    }

    /**
     * Return the first 'event id' which could be generated from this config. Though events don't have
     * ids we can simulate them to help bookkeeping.
     */
    int64_t getStartEventId() const
    {
        return firstEventId + firstEventNumber;
    }

    /** Return one past the last 'event id' which could be generated from this config. */
    int64_t getStopEventId() const
    {
        return firstEventId + firstEventNumber + maxEvents;
    }

    /** Return the next event number for a generator which has so far emitted {@code numEvents}. */
    int64_t nextEventNumber(int64_t numEvents) const
    {
        return firstEventNumber + numEvents;
    }

    /**
     * Return the next event number for a generator which has so far emitted {@code numEvents}, but
     * adjusted to account for {@code outOfOrderGroupSize}.
     */
    int64_t nextAdjustedEventNumber(int64_t numEvents) const
    {
        int64_t n = configuration.outOfOrderGroupSize;
        int64_t eventNumber = nextEventNumber(numEvents);
        if (n == 0) {
            GErrorLog("split num is 0");
            throw std::runtime_error("split num is 0");
        }
        int64_t base = (eventNumber / n) * n;
        int64_t offset = (eventNumber * 953LL) % n;
        return base + offset;
    }

    /**
     * Return the event number who's event time will be a suitable watermark for a generator which has
     * so far emitted {@code numEvents}.
     */
    int64_t nextEventNumberForWatermark(int64_t numEvents) const
    {
        int64_t n = configuration.outOfOrderGroupSize;
        if (n == 0) {
            GErrorLog("split num is 0");
            throw std::runtime_error("split num is 0");
        }
        int64_t eventNumber = nextEventNumber(numEvents);
        return (eventNumber / n) * n;
    }

    /**
     * What timestamp should the event with {@code eventNumber} have for this generator?
     */
    int64_t timestampForEvent(int64_t eventNumber) const
    {
        return baseTime + (static_cast<int64_t>(eventNumber * interEventDelayUs[0]) / 1000LL);
    }

private:
    NexmarkConfiguration configuration;

    /**
     * Delay between events, in microseconds. If the array has more than one entry then the rate is
     * changed every {@link #stepLengthSec}, and wraps around.
     */
    std::vector<double> interEventDelayUs;

    /** Delay before changing the current inter-event delay. */
    int64_t stepLengthSec;

    /**
     * True period of epoch in milliseconds. Derived from above. (Ie time to run through cycle for all
     * interEventDelayUs entries).
     */
    int64_t epochPeriodMs;

    /**
     * Number of events per epoch. Derived from above. (Ie number of events to run through cycle for
     * all interEventDelayUs entries).
     */
    int64_t eventsPerEpoch;
};
#endif // OMNISTREAM_GENERATORCONFIG_H
