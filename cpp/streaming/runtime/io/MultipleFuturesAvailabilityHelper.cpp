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

#include "MultipleFuturesAvailabilityHelper.h"

namespace omnistream {

void MultipleFuturesAvailabilityHelper::notifyCompletion()
{
    // This lock is to ensure that the availableFuture won't be changed or deleted by the mailBox thread
    // (resetToUnAvailable).
    std::lock_guard<std::recursive_mutex> lock(availableFutureMutex);
    availableFuture->complete();
}

MultipleFuturesAvailabilityHelper::MultipleFuturesAvailabilityHelper(int size)
{
    futuresToCombine.resize(size);
}

std::shared_ptr<CompletableFuture> MultipleFuturesAvailabilityHelper::getAvailableFuture()
{
    return availableFuture;
}

void MultipleFuturesAvailabilityHelper::resetToUnAvailable()
{
    std::lock_guard<std::recursive_mutex> lock(availableFutureMutex);
    if (availableFuture->isDone()) {
        availableFuture = std::make_shared<CompletableFuture>();
    }
}

void MultipleFuturesAvailabilityHelper::anyOf(int idx, std::shared_ptr<CompletableFuture> availabilityFuture)
{
    if (futuresToCombine[idx] == nullptr || futuresToCombine[idx]->isDone()) {
        futuresToCombine[idx] = availabilityFuture;
        auto inner = std::make_shared<InnerRunnable>(this);
        availabilityFuture->thenRun(inner);
    }
}

} // namespace omnistream
