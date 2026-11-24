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

#include <string>

class TimestampData {
public:
    TimestampData(long millisecond, int nanoOfMillisecond);

    long getMillisecond() const;
    int getNanoOfMillisecond() const;

    static TimestampData fromEpochMillis(long milliseconds);
    static TimestampData fromEpochMillis(long milliseconds, int nanosOfMillisecond);

    static long stringToEpochMillis(const std::string& str);
    static long stringToMillisOfDay(const std::string& str);
    static TimestampData fromString(const std::string& str);
    static TimestampData fromTimeString(const std::string& str);
    static TimestampData fromLocalTimeString(const std::string& str);

    static bool isCompact(int percision);

private:
    static const long MILLIS_PER_DAY = 86400000;
    long millisecond;
    int nanoOfMillisecond;
};
