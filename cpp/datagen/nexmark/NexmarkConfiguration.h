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

#ifndef OMNISTREAM_NEXMARKCONFIGURATION_H
#define OMNISTREAM_NEXMARKCONFIGURATION_H

#include <string>
#include "utils/NexmarkUtils.h"
#include "nlohmann/json.hpp"
#include "core/include/common.h"

#define SET_FIELD(field)                                  \
    if (configMap.contains(#field)) {                     \
        field = configMap[#field].get<decltype(field)>(); \
    };
struct NexmarkConfiguration {
    NexmarkConfiguration() : configuration(nlohmann::json::object()) {};
    explicit NexmarkConfiguration(const nlohmann::json& configuration) : configuration(configuration)
    {
        if (configuration.contains("configMap") && !configuration["configMap"].is_null()) {
            auto configMap = configuration["configMap"];
            // possible in combing keys in the config map
            SET_FIELD(hotBiddersRatio)
            SET_FIELD(outOfOrderGroupSize)
            SET_FIELD(firstEventRate)
            SET_FIELD(nextEventRate)
            SET_FIELD(windowSizeSec)
            SET_FIELD(avgAuctionByteSize)
            SET_FIELD(avgPersonByteSize)
            SET_FIELD(probDelayedEvent)
            SET_FIELD(avgBidByteSize)
            SET_FIELD(occasionalDelaySec)
            SET_FIELD(isRateLimited)
            SET_FIELD(numActivePeople)
            SET_FIELD(preloadSeconds)
            SET_FIELD(hotSellersRatio)
            SET_FIELD(streamTimeout)
            SET_FIELD(auctionProportion)
            SET_FIELD(numInFlightAuctions)
            SET_FIELD(numEventGenerators)
            SET_FIELD(ratePeriodSec)
            SET_FIELD(useWallclockEventTime)
            SET_FIELD(windowPeriodSec)
            SET_FIELD(personProportion)
            SET_FIELD(numEvents)
            SET_FIELD(bidProportion)
            SET_FIELD(hotAuctionRatio)
            SET_FIELD(watermarkHoldbackSec)
        } else {
            LOG("Nexmakr datagen will use default config");
        }
    }
    nlohmann::json configuration;
    /**
     * Number of events to generate. If zero, generate as many as possible without overflowing
     * internal counters etc.
     */
    long long numEvents = 0;

    /** Number of event generators to use. Each generates events in its own timeline. */
    int numEventGenerators = 1;

    /** Shape of event rate curve. */
    NexmarkUtils::RateShape rateShape = NexmarkUtils::RateShape::SQUARE;

    /** Initial overall event rate (in {@link #rateUnit}). */
    int firstEventRate = 10000;

    /** Next overall event rate (in {@link #rateUnit}). */
    int nextEventRate = 10000;

    /** Unit for rates. */
    NexmarkUtils::RateUnit rateUnit = NexmarkUtils::RateUnit::PER_SECOND;

    /** Overall period of rate shape, in seconds. */
    int ratePeriodSec = 600;

    /**
     * Time in seconds to preload the subscription with data, at the initial input rate of the
     * pipeline.
     */
    int preloadSeconds = 0;

    /** Timeout for stream pipelines to stop in seconds. */
    int streamTimeout = 240;

    /**
     * If true, and in streaming mode, generate events only when they are due according to their
     * timestamp.
     */
    bool isRateLimited = false;

    /**
     * If true, use wallclock time as event time. Otherwise, use a deterministic time in the past so
     * that multiple runs will see exactly the same event streams and should thus have exactly the
     * same results.
     */
    bool useWallclockEventTime = false;

    /**
     * Person Proportion.
     */
    int personProportion = 1;

    /**
     * Auction Proportion.
     */
    int auctionProportion = 3;

    /**
     * Bid Proportion.
     */
    int bidProportion = 46;

    /** Average idealized size of a 'new person' event, in bytes. */
    int avgPersonByteSize = 200;

    /** Average idealized size of a 'new auction' event, in bytes. */
    int avgAuctionByteSize = 500;

    /** Average idealized size of a 'bid' event, in bytes. */
    int avgBidByteSize = 100;

    /** Ratio of bids to 'hot' auctions compared to all other auctions. */
    int hotAuctionRatio = 2;

    /** Ratio of auctions for 'hot' sellers compared to all other people. */
    int hotSellersRatio = 4;

    /** Ratio of bids for 'hot' bidders compared to all other people. */
    int hotBiddersRatio = 4;

    /** Window size, in seconds, for queries 3, 5, 7 and 8. */
    long long windowSizeSec = 10;

    /** Sliding window period, in seconds, for query 5. */
    long long windowPeriodSec = 5;

    /** Number of seconds to hold back events according to their reported timestamp. */
    long long watermarkHoldbackSec = 0;

    /** Average number of auction which should be inflight at any time, per generator. */
    int numInFlightAuctions = 100;

    /** Maximum number of people to consider as active for placing auctions or bids. */
    int numActivePeople = 1000;

    /** Length of occasional delay to impose on events (in seconds). */
    long long occasionalDelaySec = 3;

    /** Probability that an event will be delayed by delayS. */
    double probDelayedEvent = 0.1;

    /**
     * Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies every
     * 1000 events per generator are emitted in pseudo-random order.
     */
    long long outOfOrderGroupSize = 1;
};

#endif // OMNISTREAM_NEXMARKCONFIGURATION_H
