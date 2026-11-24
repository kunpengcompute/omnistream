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

#include "BidGenerator.h"

// The static fields of BidGenerator

const std::array<std::string, 4> BidGenerator::HOT_CHANNELS = {"Google", "Facebook", "Baidu", "Apple"};
const std::array<std::string, 4> BidGenerator::HOT_URLS = []() -> std::array<std::string, 4> {
    SplittableRandom random(0);
    return {getBaseUrl(random), getBaseUrl(random), getBaseUrl(random), getBaseUrl(random)};
}();

std::vector<std::tuple<std::string, std::string>> BidGenerator::CHANNEL_URL_CACHE =
    []() -> std::vector<std::tuple<std::string, std::string>> {
    SplittableRandom random(0);
    return BidGenerator::createChannelUrlCache(random);
}();

std::vector<std::tuple<std::string, std::string>> BidGenerator::createChannelUrlCache(SplittableRandom& random)
{
    std::vector<std::tuple<std::string, std::string>> cache;
    cache.reserve(CHANNELS_NUMBER);
    for (int i = 0; i < CHANNELS_NUMBER; ++i) {
        std::string url = getBaseUrl(random);
        if (random.nextInt(10) > 0) {
            url += "&channel_id=" + std::to_string(reverseInt(i));
        }
        cache.emplace_back("channel-" + std::to_string(i), url);
    }
    return cache;
}

std::unique_ptr<Bid> BidGenerator::nextBid(long eventId, long timestamp, const GeneratorConfig& config)
{
    long auction;
    // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
    if (random.nextInt(config.getHotAuctionRatio()) > 0) {
        // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
        auction = (AuctionGenerator::lastBase0AuctionId(config, eventId) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
    } else {
        auction = AuctionGenerator::nextBase0AuctionId(eventId, random, config);
    }
    auction += GeneratorConfig::FIRST_AUCTION_ID;

    long bidder;
    // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
    if (random.nextInt(config.getHotBiddersRatio()) > 0) {
        // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
        // last HOT_BIDDER_RATIO people.
        bidder = (PersonGenerator::lastBase0PersonId(config, eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
    } else {
        bidder = PersonGenerator::nextBase0PersonId(eventId, random, config);
    }
    bidder += GeneratorConfig::FIRST_PERSON_ID;

    long price = random.nextPrice();

    std::string_view channel;
    std::string_view url;
    if (random.nextInt(HOT_CHANNELS_RATIO) > 0) {
        int i = random.nextInt((int)HOT_CHANNELS.size());
        channel = HOT_CHANNELS[i];
        url = HOT_URLS[i];
    } else {
        const std::tuple<std::string, std::string>& channelAndUrl = getNextChannelAndurl(random);
        channel = std::get<0>(channelAndUrl);
        url = std::get<1>(channelAndUrl);
    }

    bidder += GeneratorConfig::FIRST_PERSON_ID;

    int currentSize = 8 + 8 + 8 + 8;
    std::string_view extra = StringsGenerator::nextExtra(random, currentSize, config.getAvgBidByteSize(), extraBuffer);
    // Convert timestamp (in milliseconds) to time_point.
    // std::chrono::system_clock::time_point timePoint =
    // std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp));
    return std::make_unique<Bid>(auction, bidder, price, channel, url, timestamp, extra);
}
