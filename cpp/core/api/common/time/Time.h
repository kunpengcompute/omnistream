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

#ifndef OMNISTREAM_TIME_H
#define OMNISTREAM_TIME_H

#include <iostream>
#include "basictypes/java_util_concurrent_TimeUnit.h"
namespace omnistream::datastream {
class Time {
public:
    explicit Time(int64_t size, JavaUtilTimeUnit* unit)
    {
        this->unit = unit;
        this->size = size;
    }

    JavaUtilTimeUnit* getUnit()
    {
        return unit;
    }

    int64_t getSize()
    {
        return size;
    }

    int64_t toMilliseconds()
    {
        return unit->toMillis(size);
    }

    static Time* of(int64_t size, JavaUtilTimeUnit* unit)
    {
        return new Time(size, unit);
    }

    /** Creates a new {@link Time} that represents the given number of milliseconds. */
    static Time* milliseconds(int64_t milliseconds)
    {
        return of(milliseconds, &JavaUtilTimeUnit::MILLIS);
    }

    /** Creates a new {@link Time} that represents the given number of seconds. */
    static Time* seconds(int64_t seconds)
    {
        return of(seconds, &JavaUtilTimeUnit::SECS);
    }

    /** Creates a new {@link Time} that represents the given number of minutes. */
    static Time* minutes(int64_t minutes)
    {
        return of(minutes, &JavaUtilTimeUnit::MINS);
    }

    /** Creates a new {@link Time} that represents the given number of hours. */
    static Time* hours(int64_t hours)
    {
        return of(hours, &JavaUtilTimeUnit::HS);
    }

    /** Creates a new {@link Time} that represents the given number of days. */
    static Time* days(int64_t days)
    {
        return of(days, &JavaUtilTimeUnit::DS);
    }

    /**
     * Creates a new {@link Time} that represents the number of milliseconds in the given duration.
     */
    // need to realize: static Time *fromDuration(Duration duration);
private:
    JavaUtilTimeUnit* unit;
    int64_t size;
};
} // namespace omnistream::datastream

#endif // OMNISTREAM_TIME_H
