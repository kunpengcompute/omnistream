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

#ifndef OMNISTREAM_AVAILABILITY_PROVIDER_H
#define OMNISTREAM_AVAILABILITY_PROVIDER_H

#include <memory>
#include <sstream>
#include "core/utils/threads/CompletableFuture.h"

namespace omnistream {

class AvailabilityProvider {
public:
    AvailabilityProvider() = default;
    virtual ~AvailabilityProvider() = default;

    static std::shared_ptr<CompletableFuture> AVAILABLE;

    virtual std::shared_ptr<CompletableFuture> GetAvailableFuture() = 0;

    static std::shared_ptr<CompletableFuture> and_(
        std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second);
    static std::shared_ptr<CompletableFuture> or_(
        std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second);

    virtual bool isAvailable()
    {
        auto future = GetAvailableFuture();
        return future == AVAILABLE || future->isDone();
    }

    bool isApproximatelyAvailable()
    {
        return GetAvailableFuture() == AVAILABLE;
    }

    virtual std::string toString()
    {
        std::stringstream ss;
        ss << "AvailabilityProvider: ";
        if (isAvailable()) {
            ss << "Available";
        } else {
            ss << "Not Available";
        }
        return ss.str();
    }
};

} // namespace omnistream

#endif // OMNISTREAM_AVAILABILITY_PROVIDER_H
