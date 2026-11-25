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

#ifndef OMNISTREAM_BIDGENERATOR_H
#define OMNISTREAM_BIDGENERATOR_H

#include "EventGenerator.h"
#include "PersonGenerator.h"
#include "AuctionGenerator.h"
/** Generates bids. */
class BidGenerator {
public:
    BidGenerator() : random(0)
    {
        channelBuffer = new char[100];
        urlBuffer = new char[200];
        extraBuffer = new char[2048];
    }
    /** Generate and return a random bid with next available id. */
    std::unique_ptr<Bid> nextBid(long eventId, long timestamp, const GeneratorConfig& config);

    virtual ~BidGenerator()
    {
        delete[] channelBuffer;
        delete[] urlBuffer;
        delete[] extraBuffer;
    };

private:
    SplittableRandom random;
    char* channelBuffer;
    char* urlBuffer;
    char* extraBuffer;

    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    static const int HOT_AUCTION_RATIO = 100;
    static const int HOT_BIDDER_RATIO = 100;
    static const int HOT_CHANNELS_RATIO = 2;
    static const int CHANNELS_NUMBER = 10000;

    static const std::array<std::string, 4> HOT_CHANNELS;
    static const std::array<std::string, 4> HOT_URLS;
    static std::vector<std::tuple<std::string, std::string>> CHANNEL_URL_CACHE;

    // Returns a base URL string.
    static std::string getBaseUrl(SplittableRandom& random)
    {
        return std::string("https://www.nexmark.com/") + StringsGenerator::nextString(random, 5, '_') + '/' +
               StringsGenerator::nextString(random, 5, '_') + '/' + StringsGenerator::nextString(random, 5, '_') + '/' +
               "item.htm?query=1";
    }

    // Creates and returns the channel URL cache.
    static std::vector<std::tuple<std::string, std::string>> createChannelUrlCache(SplittableRandom& random);

    // Returns the next channel and URL from the cache.
    static const std::tuple<std::string, std::string>& getNextChannelAndurl(SplittableRandom& random)
    {
        int channelNumber = random.nextInt(CHANNELS_NUMBER);
        return CHANNEL_URL_CACHE[channelNumber];
    }

    static inline int reverseInt(uint32_t n)
    {
        uint32_t result = 0;
        for (int i = 0; i < 32; i++) {
            result = (result << 1) | (n & 1);
            n >>= 1;
        }
        int reversed = static_cast<int>(result);
        return reversed < 0 ? static_cast<int>(0u - result) : reversed;
    }
};

#endif // OMNISTREAM_BIDGENERATOR_H
