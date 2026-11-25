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

#include "EventGenerator.h"
#include "AuctionGenerator.h"
std::unique_ptr<Auction> AuctionGenerator::nextAuction(
    long eventsCountSoFar, long eventId, long timestamp, const GeneratorConfig& config)
{
    long id = lastBase0AuctionId(config, eventId) + GeneratorConfig::FIRST_AUCTION_ID;

    long seller;
    if (random.nextInt(config.getHotSellersRatio()) > 0) {
        seller = (PersonGenerator::lastBase0PersonId(config, eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
    } else {
        seller = PersonGenerator::nextBase0PersonId(eventId, random, config);
    }
    seller += GeneratorConfig::FIRST_PERSON_ID;

    long category = GeneratorConfig::FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
    long initialBid = random.nextPrice();
    long expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
    std::string name = StringsGenerator::nextString(random, 20);
    std::string desc = StringsGenerator::nextString(random, 100);
    long reserve = initialBid + random.nextPrice();
    int currentSize = 8 + static_cast<int>(name.size() + desc.size()) + 8 + 8 + 8 + 8 + 8;
    std::string_view extra =
        StringsGenerator::nextExtra(random, currentSize, config.getAvgAuctionByteSize(), extraBuffer);
    name.copy(nameBuffer, name.size());
    desc.copy(descBuffer, desc.size());
    return std::make_unique<Auction>(
        id,
        std::string_view(nameBuffer, name.size()),
        std::string_view(descBuffer, desc.size()),
        initialBid,
        reserve,
        timestamp,
        expires,
        seller,
        category,
        extra);
}

long AuctionGenerator::lastBase0AuctionId(const GeneratorConfig& config, long eventId)
{
    if (config.totalProportion == 0) {
        GErrorLog("event num is 0");
        throw std::runtime_error("event num is 0");
    }
    long epoch = eventId / config.totalProportion;
    long offset = eventId % config.totalProportion;
    if (offset < config.personProportion) {
        epoch--;
        offset = config.auctionProportion - 1;
    } else if (offset >= config.personProportion + config.auctionProportion) {
        offset = config.auctionProportion - 1;
    } else {
        offset -= config.personProportion;
    }
    return epoch * config.auctionProportion + offset;
}

long AuctionGenerator::nextAuctionLengthMs(
    long eventsCountSoFar, SplittableRandom& random, long timestamp, const GeneratorConfig& config)
{
    long currentEventNumber = config.nextAdjustedEventNumber(eventsCountSoFar);
    if (config.totalProportion == 0) {
        GErrorLog("event num is 0");
        throw std::runtime_error("event num is 0");
    }
    long numEventsForAuctions = (config.getNumInFlightAuctions() * config.totalProportion) / config.auctionProportion;
    long futureAuction = config.timestampForEvent(currentEventNumber + numEventsForAuctions);
    long horizonMs = futureAuction - timestamp;
    long bound = std::max(horizonMs * 2, 1L);
    return 1L + random.nextLong(bound);
}

long AuctionGenerator::nextBase0AuctionId(long nextEventId, SplittableRandom& random, const GeneratorConfig& config)
{
    long minAuction = std::max(lastBase0AuctionId(config, nextEventId) - config.getNumInFlightAuctions(), 0L);
    long maxAuction = lastBase0AuctionId(config, nextEventId);
    long range = maxAuction - minAuction + 1 + AUCTION_ID_LEAD;
    return minAuction + random.nextLong(range);
}
