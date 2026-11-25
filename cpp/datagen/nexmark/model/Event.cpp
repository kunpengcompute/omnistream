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

#include "Event.h"
//"BIGINT", "BIGINT", "BIGINT", "STRING", "STRING", "TIMESTAMP", "STRING"
std::vector<int> Event::BidTypes = {2, 2, 2, 15, 15, 12, 15};
// "BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "TIMESTAMP",  "TIMESTAMP", "BIGINT", "BIGINT", "STRING"
std::vector<int> Event::AuctionTypes = {2, 15, 15, 2, 2, 12, 12, 2, 2, 15};
// "BIGINT", "STRING", "STRING", "STRING", "STRING", "STRING", "TIMESTAMP", "STRING"
std::vector<int> Event::PersonTypes = {2, 15, 15, 15, 15, 15, 12, 15};

bool Bid::operator==(const Event& _other) const
{
    auto ptr = dynamic_cast<const Bid*>(&_other);
    if (ptr == nullptr) {
        return false;
    }
    const Bid& other = *ptr;
    return auction == other.auction && bidder == other.bidder && price == other.price && channel == other.channel &&
           url == other.url && dateTime == other.dateTime && extra == other.extra;
}

size_t Bid::hash() const
{
    return std::hash<long>()(auction) ^ std::hash<long>()(bidder) ^ std::hash<long>()(price) ^
           std::hash<std::string_view>()(channel) ^ std::hash<std::string_view>()(url) ^ std::hash<long>()(dateTime) ^
           std::hash<std::string_view>()(extra);
}

std::string Bid::toString() const
{
    return "Bid{auction=" + std::to_string(auction) + ", bidder=" + std::to_string(bidder) +
           ", price=" + std::to_string(price) + ", channel='" + std::string(channel) + '\'' + ", url='" +
           std::string(url) + '\'' + ", dateTime=" + std::to_string(dateTime) + ", extra='" + std::string(extra) +
           '\'' + '}';
}

std::string Auction::toString() const
{
    return "Auction{" + std::to_string(id) + ", itemName='" + std::string(itemName) + '\'' + ", description='" +
           std::string(description) + '\'' + ", initialBid=" + std::to_string(initialBid) +
           ", reserve=" + std::to_string(reserve) + ", dateTime=" + std::to_string(dateTime) +
           ", expires=" + std::to_string(expires) + ", seller=" + std::to_string(seller) +
           ", category=" + std::to_string(category) + ", extra='" + std::string(extra) + '\'' + '}';
}

bool Auction::operator==(const Event& other) const
{
    auto ptr = dynamic_cast<const Auction*>(&other);
    if (ptr == nullptr) {
        return false;
    }
    const Auction& auction = *ptr;
    return id == auction.id && initialBid == auction.initialBid && reserve == auction.reserve &&
           dateTime == auction.dateTime && expires == auction.expires && seller == auction.seller &&
           category == auction.category && itemName == auction.itemName && description == auction.description &&
           extra == auction.extra;
}

size_t Auction::hash() const
{
    return std::hash<long>()(id) ^ std::hash<std::string_view>()(itemName) ^
           std::hash<std::string_view>()(description) ^ std::hash<long>()(initialBid) ^ std::hash<long>()(reserve) ^
           std::hash<long>()(dateTime) ^ std::hash<long>()(expires) ^ std::hash<long>()(seller) ^
           std::hash<long>()(category) ^ std::hash<std::string_view>()(extra);
}

std::string Person::toString() const
{
    return "Person{" + std::to_string(id) + ", name='" + name + '\'' + ", emailAddress='" + std::string(emailAddress) +
           '\'' + ", creditCard='" + creditCard + '\'' + ", city='" + std::string(city) + '\'' + ", state='" +
           std::string(state) + '\'' + ", dateTime=" + std::to_string(dateTime) + ", extra='" + std::string(extra) +
           '\'' + '}';
}

bool Person::operator==(const Event& _other) const
{
    auto ptr = dynamic_cast<const Person*>(&_other);
    if (ptr == nullptr) {
        return false;
    }
    const Person& other = *ptr;
    return id == other.id && dateTime == other.dateTime && name == other.name && emailAddress == other.emailAddress &&
           creditCard == other.creditCard && city == other.city && state == other.state && extra == other.extra;
}

size_t Person::hash() const
{
    return std::hash<long>()(id) ^ std::hash<long>()(dateTime) ^ std::hash<std::string>()(name) ^
           std::hash<std::string_view>()(emailAddress) ^ std::hash<std::string>()(creditCard) ^
           std::hash<std::string_view>()(city) ^ std::hash<std::string_view>()(state) ^
           std::hash<std::string_view>()(extra);
}
