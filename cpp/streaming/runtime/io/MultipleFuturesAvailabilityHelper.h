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

#ifndef OMNISTREAM_MULTIPLEFUTURESAVAILABILITYHELPER_H
#define OMNISTREAM_MULTIPLEFUTURESAVAILABILITYHELPER_H

#include "core/utils/threads/CompletableFuture.h"

namespace omnistream {
class MultipleFuturesAvailabilityHelper {
public:
    class InnerRunnable : public Runnable {
    public:
        explicit InnerRunnable(MultipleFuturesAvailabilityHelper* outer) : outer(outer)
        {
        }

        void run() override
        {
            outer->notifyCompletion();
        }

    private:
        MultipleFuturesAvailabilityHelper* outer;
    };

    explicit MultipleFuturesAvailabilityHelper(int size);

    std::shared_ptr<CompletableFuture> getAvailableFuture();

    void resetToUnAvailable();

    void anyOf(int idx, std::shared_ptr<CompletableFuture> availabilityFuture);

private:
    std::vector<std::shared_ptr<CompletableFuture>> futuresToCombine;
    std::shared_ptr<CompletableFuture> availableFuture = std::make_shared<CompletableFuture>();
    std::recursive_mutex availableFutureMutex;

    void notifyCompletion();
};
} // namespace omnistream

#endif
