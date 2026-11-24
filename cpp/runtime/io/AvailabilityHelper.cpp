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

#include "AvailabilityHelper.h"
#include <sstream>

namespace omnistream {

AvailabilityHelper::AvailabilityHelper() : availableFuture(std::make_shared<CompletableFuture>())
{
}

AvailabilityHelper::~AvailabilityHelper()
{
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::and_(const std::shared_ptr<CompletableFuture>& other)
{
    return AvailabilityProvider::and_(availableFuture, other);
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::and_(const std::shared_ptr<AvailabilityProvider>& other)
{
    return and_(other->GetAvailableFuture());
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::or_(const std::shared_ptr<CompletableFuture>& other)
{
    return AvailabilityProvider::or_(availableFuture, other);
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::or_(const std::shared_ptr<AvailabilityProvider>& other)
{
    return or_(other->GetAvailableFuture());
}

void AvailabilityHelper::resetUnavailable()
{
    std::unique_lock<std::recursive_mutex> lock(availableFutureMutex);
    if (isAvailable()) {
        availableFuture = std::make_shared<CompletableFuture>();
    }
}

void AvailabilityHelper::resetAvailable()
{
    std::unique_lock<std::recursive_mutex> lock(availableFutureMutex);
    availableFuture = AVAILABLE;
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::getUnavailableToResetAvailable()
{
    std::unique_lock<std::recursive_mutex> lock(availableFutureMutex);
    std::shared_ptr<CompletableFuture> toNotify = availableFuture;
    availableFuture = AVAILABLE;
    return toNotify;
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::getUnavailableToResetUnavailable()
{
    std::unique_lock<std::recursive_mutex> lock(availableFutureMutex);
    std::shared_ptr<CompletableFuture> toNotify = availableFuture;
    availableFuture = std::make_shared<CompletableFuture>();
    return toNotify;
}

std::shared_ptr<CompletableFuture> AvailabilityHelper::GetAvailableFuture()
{
    std::unique_lock<std::recursive_mutex> lock(availableFutureMutex);
    return availableFuture;
}

std::string AvailabilityHelper::toString()
{
    if (availableFuture == AVAILABLE) {
        return "AVAILABLE";
    }
    return availableFuture->toString();
}

} // namespace omnistream
