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

#ifndef OMNISTREAM_TIMERSERVICE_H
#define OMNISTREAM_TIMERSERVICE_H

#include <cstdint>

namespace omnistream::streaming {
class TimerService {
public:
    /** Error string for {@link UnsupportedOperationException} on registering timers. */
    const std::string UNSUPPORTED_REGISTER_TIMER_MSG = "Setting timers is only supported on a keyed streams.";

    /** Error string for {@link UnsupportedOperationException} on deleting timers. */
    const std::string UNSUPPORTED_DELETE_TIMER_MSG = "Deleting timers is only supported on a keyed streams.";

    /** Returns the current processing time. */
    virtual int64_t currentProcessingTime() = 0;

    /** Returns the current event-time watermark. */
    virtual int64_t currentWatermark() = 0;

    /**
     * Registers a timer to be fired when processing time passes the given time.
     *
     * <p>Timers can internally be scoped to keys and/or windows. When you set a timer in a keyed
     * context, such as in an operation on {@link
     * org.apache.flink.streaming.api.datastream.KeyedStream} then that context will also be active
     * when you receive the timer notification.
     */
    virtual void registerProcessingTimeTimer(int64_t time) = 0;

    /**
     * Registers a timer to be fired when the event time watermark passes the given time.
     *
     * <p>Timers can internally be scoped to keys and/or windows. When you set a timer in a keyed
     * context, such as in an operation on {@link
     * org.apache.flink.streaming.api.datastream.KeyedStream} then that context will also be active
     * when you receive the timer notification.
     */
    virtual void registerEventTimeTimer(int64_t time) = 0;

    /**
     * Deletes the processing-time timer with the given trigger time. This method has only an effect
     * if such a timer was previously registered and did not already expire.
     *
     * <p>Timers can internally be scoped to keys and/or windows. When you delete a timer, it is
     * removed from the current keyed context.
     */
    virtual void deleteProcessingTimeTimer(int64_t time) = 0;

    /**
     * Deletes the event-time timer with the given trigger time. This method has only an effect if
     * such a timer was previously registered and did not already expire.
     *
     * <p>Timers can internally be scoped to keys and/or windows. When you delete a timer, it is
     * removed from the current keyed context.
     */
    virtual void deleteEventTimeTimer(int64_t time) = 0;
};
} // namespace omnistream::streaming
#endif // OMNISTREAM_TIMERSERVICE_H
