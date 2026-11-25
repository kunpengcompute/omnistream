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

#include "AvailabilityWithBacklog.h"
// AvailabilityWithBacklog.cpp
#include <sstream>
#include <stdexcept>
#include "AvailabilityWithBacklog.h"

namespace omnistream {

AvailabilityWithBacklog::AvailabilityWithBacklog() : isAvailable(false), backlog(0)
{
}

AvailabilityWithBacklog::AvailabilityWithBacklog(bool isAvailable, int backlog)
    : isAvailable(isAvailable),
      backlog(backlog)
{
    if (backlog < 0) {
        throw std::invalid_argument("Backlog must be non-negative.");
    }
}

AvailabilityWithBacklog::AvailabilityWithBacklog(const AvailabilityWithBacklog& other_)
    : isAvailable(other_.isAvailable),
      backlog(other_.backlog)
{
}

AvailabilityWithBacklog& AvailabilityWithBacklog::operator=(const AvailabilityWithBacklog& other_)
{
    if (this != &other_) {
        isAvailable = other_.isAvailable;
        backlog = other_.backlog;
    }
    return *this;
}

bool AvailabilityWithBacklog::operator==(const AvailabilityWithBacklog& other_) const
{
    return isAvailable == other_.isAvailable && backlog == other_.backlog;
}

bool AvailabilityWithBacklog::operator!=(const AvailabilityWithBacklog& other_) const
{
    return !(*this == other_);
}

AvailabilityWithBacklog::~AvailabilityWithBacklog()
{
}

bool AvailabilityWithBacklog::getIsAvailable() const
{
    return isAvailable;
}

int AvailabilityWithBacklog::getBacklog() const
{
    return backlog;
}

void AvailabilityWithBacklog::setIsAvailable(bool isAvailable_)
{
    this->isAvailable = isAvailable_;
}

void AvailabilityWithBacklog::setBacklog(int backlog_)
{
    if (backlog_ < 0) {
        throw std::invalid_argument("Backlog must be non-negative.");
    }
    this->backlog = backlog_;
}

std::string AvailabilityWithBacklog::toString() const
{
    std::stringstream ss;
    ss << "AvailabilityWithBacklog{isAvailable=" << (isAvailable ? "true" : "false") << ", backlog=" << backlog << "}";
    return ss.str();
}

} // namespace omnistream
