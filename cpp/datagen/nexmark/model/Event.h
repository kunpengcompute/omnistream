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

#ifndef OMNISTREAM_EVENT_H
#define OMNISTREAM_EVENT_H

#include <iostream>
#include <memory>
#include <string>
#include <functional>
#include <chrono>
#include "core/include/common.h"
enum class EventType {
    PERSON = 0,
    AUCTION = 1,
    BID = 2
};
class Event {
public:
    explicit Event(const EventType& eventType) : type(eventType) {};
    virtual ~Event() = default;
    virtual bool operator==(const Event& other) const = 0;
    virtual std::size_t hash() const = 0;
    virtual std::string toString() const = 0;
    [[nodiscard]] EventType getEventType() const
    {
        return type;
    }
    static std::vector<int> BidTypes;
    static std::vector<int> AuctionTypes;
    static std::vector<int> PersonTypes;

private:
    EventType type;
};

class Bid : public Event {
public:
    long auction; // foreign key: Auction.id
    long bidder;  // foreign key: Person.id
    long price;
    std::string_view channel;
    std::string_view url;
    long dateTime; // using system_clock for Instant
    std::string_view extra;

    Bid(long auction,
        long bidder,
        long price,
        std::string_view channel,
        std::string_view url,
        long dateTime,
        std::string_view extra)
        : Event(EventType::BID),
          auction(auction),
          bidder(bidder),
          price(price),
          channel(channel),
          url(url),
          dateTime(dateTime),
          extra(extra)
    {
    }

    bool operator==(const Event& other) const override;
    size_t hash() const override;
    std::string toString() const override;
};

class Auction : public Event {
public:
    long id; // primary key
    std::string_view itemName;
    std::string_view description;
    long initialBid;
    long reserve;
    long dateTime;
    long expires;
    long seller;   // foreign key: Person.id
    long category; // foreign key: Category.id
    std::string_view extra;

    Auction(
        long id,
        std::string_view itemName,
        std::string_view description,
        long initialBid,
        long reserve,
        long dateTime,
        long expires,
        long seller,
        long category,
        std::string_view extra)
        : Event(EventType::AUCTION),
          id(id),
          itemName(itemName),
          description(description),
          initialBid(initialBid),
          reserve(reserve),
          dateTime(dateTime),
          expires(expires),
          seller(seller),
          category(category),
          extra(extra)
    {
    }

    std::string toString() const override;
    bool operator==(const Event& auction) const override;
    size_t hash() const override;
};

class Person : public Event {
public:
    long id; // primary key
    std::string name;
    std::string_view emailAddress;
    std::string creditCard;
    std::string_view city;
    std::string_view state;
    long dateTime;
    std::string_view extra;

    Person(
        long id,
        std::string name,
        std::string_view emailAddress,
        std::string creditCard,
        std::string_view city,
        std::string_view state,
        long dateTime,
        std::string_view extra)
        : Event(EventType::PERSON),
          id(id),
          name(std::move(name)),
          emailAddress(emailAddress),
          creditCard(std::move(creditCard)),
          city(city),
          state(state),
          dateTime(dateTime),
          extra(extra)
    {
    }

    std::string toString() const override;
    bool operator==(const Event& other) const override;
    size_t hash() const override;
};

#endif // OMNISTREAM_EVENT_H
