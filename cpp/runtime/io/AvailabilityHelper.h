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

#ifndef OMNISTREAM_AVAILABILITYHELPER_H
#define OMNISTREAM_AVAILABILITYHELPER_H

#include <memory>
#include <string>
#include "AvailabilityProvider.h"

namespace omnistream {

class AvailabilityHelper : public AvailabilityProvider {
public:
    AvailabilityHelper();
    ~AvailabilityHelper() override;

    std::shared_ptr<CompletableFuture> and_(const std::shared_ptr<CompletableFuture>& other);
    std::shared_ptr<CompletableFuture> and_(const std::shared_ptr<AvailabilityProvider>& other);
    std::shared_ptr<CompletableFuture> or_(const std::shared_ptr<CompletableFuture>& other);
    std::shared_ptr<CompletableFuture> or_(const std::shared_ptr<AvailabilityProvider>& other);

    void resetUnavailable();
    void resetAvailable();
    std::shared_ptr<CompletableFuture> getUnavailableToResetAvailable();
    std::shared_ptr<CompletableFuture> getUnavailableToResetUnavailable();
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    std::string toString() override;

private:
    std::shared_ptr<CompletableFuture> availableFuture;

protected:
    std::recursive_mutex availableFutureMutex;
};

} // namespace omnistream

#endif // OMNISTREAM_AVAILABILITYHELPER_H
