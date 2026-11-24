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

#pragma once

#include <cstdint>
#include "ProcessingTimeCallback.h"
#include "ScheduledFutureTask.h"

class ProcessingTimeService {
public:
    virtual ~ProcessingTimeService() = default;
    virtual std::shared_ptr<ScheduledFutureTask> registerTimer(int64_t timestamp, ProcessingTimeCallback* target) = 0;
    virtual std::shared_ptr<ScheduledFutureTask> registerTimer(
        int64_t timestamp, std::shared_ptr<ProcessingTimeCallback> target) = 0;
    virtual int64_t getCurrentProcessingTime() = 0;
    virtual std::shared_ptr<ScheduledFutureTask> scheduleWithFixedDelay(
        std::shared_ptr<ProcessingTimeCallback> callback, long initialDelay, long period) = 0;
};
