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

template <typename N>
class InternalTimerService {
public:
    virtual ~InternalTimerService() = default;

    virtual int64_t currentProcessingTime() = 0;

    /** Returns the current event-time watermark. */
    virtual int64_t currentWatermark() = 0;

    /**
     * Registers a timer to be fired when processing time passes the given time. The nameSpace you
     * pass here will be provided when the timer fires.
     */
    virtual void registerProcessingTimeTimer(N nameSpace, int64_t time) = 0;

    /** Deletes the timer for the given key and nameSpace. */
    virtual void deleteProcessingTimeTimer(N nameSpace, int64_t time) = 0;

    virtual void registerEventTimeTimer(N nameSpace, int64_t time) = 0;

    virtual void deleteEventTimeTimer(N nameSpace, int64_t time) = 0;

    virtual void advanceWatermark(int64_t time) = 0;

    // temp fix for too many timers in priority queue
    // this function should to be deleted when RocksDBCachingPriorityQueueSet is implemented in the future
    virtual void deleteFirstEventTimeTimer() = 0;
};
