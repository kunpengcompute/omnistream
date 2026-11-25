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

#ifndef OMNISTREAM_SIMPLETIMERSERVICE_H
#define OMNISTREAM_SIMPLETIMERSERVICE_H
#include <cstdint>

#include "TimerService.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/operators/InternalTimerService.h"

class SimpleTimerService : public omnistream::streaming::TimerService {
public:
    explicit SimpleTimerService(InternalTimerService<VoidNamespace>* internalTimerService)
        : internalTimerService(internalTimerService)
    {
    }

    ~SimpleTimerService() = default;

    int64_t currentProcessingTime() override
    {
        return internalTimerService->currentProcessingTime();
    }

    int64_t currentWatermark() override
    {
        return internalTimerService->currentWatermark();
    }

    void registerProcessingTimeTimer(int64_t time) override
    {
        internalTimerService->registerProcessingTimeTimer(VoidNamespace(), time);
    }

    void registerEventTimeTimer(int64_t time) override
    {
        if (++timerSize > 10000) {
            internalTimerService->deleteFirstEventTimeTimer();
        }
        internalTimerService->registerEventTimeTimer(VoidNamespace(), time);
    }

    void deleteProcessingTimeTimer(int64_t time) override
    {
        internalTimerService->deleteProcessingTimeTimer(VoidNamespace(), time);
    }

    void deleteEventTimeTimer(int64_t time) override
    {
        internalTimerService->deleteEventTimeTimer(VoidNamespace(), time);
    }

private:
    InternalTimerService<VoidNamespace>* internalTimerService;
    uint32_t timerSize = 0;
};
#endif // OMNISTREAM_SIMPLETIMERSERVICE_H
