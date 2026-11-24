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

#include <stdexcept>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <charconv>
#include "TimestampData.h"
#include "common.h"

TimestampData::TimestampData(long millisecond, int nanoOfMillisecond)
    : millisecond(millisecond),
      nanoOfMillisecond(nanoOfMillisecond)
{
    if (nanoOfMillisecond < 0 || nanoOfMillisecond > 999999) {
        throw std::invalid_argument("nanoOfMillisecond must be between 0 and 999999.");
    }
}

long TimestampData::getMillisecond() const
{
    return millisecond;
}

int TimestampData::getNanoOfMillisecond() const
{
    return nanoOfMillisecond;
}

TimestampData TimestampData::fromEpochMillis(long milliseconds)
{
    return TimestampData(milliseconds, 0);
}

TimestampData TimestampData::fromEpochMillis(long milliseconds, int nanosOfMillisecond)
{
    return TimestampData(milliseconds, nanosOfMillisecond);
}

bool TimestampData::isCompact(int percision)
{
    return percision <= 3;
}

/**
 * Convert string to milliseconds since Unix Epoch
 * @param str Support formats like "2025-02-07 12:00:00.000" and "1989-03-04 08:00:00"
 * @return
 */
long TimestampData::stringToEpochMillis(const std::string& str)
{
    const char* start = str.data();
    const char* end = str.data() + str.size();
    while (start < end && *start == ' ') ++start;
    while (end > start && *(end - 1) == ' ') --end;
    if (start >= end) {
        THROW_RUNTIME_ERROR("Empty datetime string after trimming spaces");
    }
    const char* dotPtr = nullptr;
    const char* tempPtr = start;
    // Find the first pointer of '.', and ignore '.' in the end
    while (tempPtr < end - 1) {
        if (*tempPtr == '.') {
            dotPtr = tempPtr;
            break;
        }
        ++tempPtr;
    }

    const char* datetimeEnd = dotPtr ? dotPtr : end;

    std::tm t = {};
    int parse_count =
        sscanf(start, "%d-%d-%d %d:%d:%d", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec);
    if (parse_count != 6) {
        THROW_RUNTIME_ERROR("Failed to parse datetime string" + std::string(start, datetimeEnd));
    }
    // tm_year starts from 1900, tm_mon starts from 0
    t.tm_year -= 1900;
    t.tm_mon -= 1;

    int milliseconds = 0;

    // Calculate milliseconds
    if (dotPtr != nullptr) {
        const char* msStart = dotPtr + 1;
        const char* msEnd = end - msStart > 3 ? msStart + 3 : end;
        int msInt = 0;
        std::from_chars_result res = std::from_chars(msStart, msEnd, msInt);
        if (res.ec != std::errc{}) {
            THROW_RUNTIME_ERROR("Failed to parse milliseconds: " + std::string(msStart, msEnd));
        }
        // Obtain actual milliseconds
        size_t msDigits = res.ptr - msStart;
        if (msDigits == 1) {
            milliseconds = msInt * 100; // .1 → 1 * 100 = 100ms
        } else if (msDigits == 2) {
            milliseconds = msInt * 10; // .12 → 12 * 10 = 120ms, .01 -> 1 * 10 = 10ms, .10 -> 10 * 10 = 100ms
        } else if (msDigits == 3) {
            milliseconds = msInt; // .120 → 120 = 120ms, .012 -> 12 = 12ms, .001 -> 1 = 1ms
        }
    }

    // Convert std::tm to time_t (seconds since epoch)
    std::time_t time_since_epoch = timegm(&t);

    // Convert to milliseconds
    return static_cast<long>(time_since_epoch) * 1000 + milliseconds;
}

/**
 * Convert time string to milliseconds since start of day
 * @param str Support formats like "12:44:31" and "12:44:31.000"
 * @return
 */
long TimestampData::stringToMillisOfDay(const std::string& str)
{
    const char* start = str.data();
    const char* end = str.data() + str.size();
    while (start < end && *start == ' ') ++start;
    while (end > start && *(end - 1) == ' ') --end;
    if (start >= end) {
        THROW_RUNTIME_ERROR("Empty time string after trimming spaces");
    }

    const char* dotPtr = nullptr;
    const char* tempPtr = start;
    // Find the first pointer of '.', and ignore '.' in the end
    while (tempPtr < end - 1) {
        if (*tempPtr == '.') {
            dotPtr = tempPtr;
            break;
        }
        ++tempPtr;
    }

    const char* timeEnd = dotPtr ? dotPtr : end;

    int hour = 0;
    int minute = 0;
    int second = 0;
    int parse_count = sscanf(start, "%d:%d:%d", &hour, &minute, &second);
    if (parse_count != 3) {
        THROW_RUNTIME_ERROR("Failed to parse time string" + std::string(start, timeEnd));
    }

    int milliseconds = 0;

    // Calculate milliseconds
    if (dotPtr != nullptr) {
        const char* msStart = dotPtr + 1;
        const char* msEnd = end - msStart > 3 ? msStart + 3 : end;
        int msInt = 0;
        std::from_chars_result res = std::from_chars(msStart, msEnd, msInt);
        if (res.ec != std::errc{}) {
            THROW_RUNTIME_ERROR("Failed to parse milliseconds: " + std::string(msStart, msEnd));
        }
        // Obtain actual milliseconds
        size_t msDigits = res.ptr - msStart;
        if (msDigits == 1) {
            milliseconds = msInt * 100; // .1 → 1 * 100 = 100ms
        } else if (msDigits == 2) {
            milliseconds = msInt * 10; // .12 → 12 * 10 = 120ms, .01 -> 1 * 10 = 10ms, .10 -> 10 * 10 = 100ms
        } else if (msDigits == 3) {
            milliseconds = msInt; // .120 → 120 = 120ms, .012 -> 12 = 12ms, .001 -> 1 = 1ms
        }
    }

    return static_cast<long>(hour * 3600 + minute * 60 + second) * 1000 + milliseconds; // Convert to milliseconds
}

TimestampData TimestampData::fromTimeString(const std::string& str)
{
    return TimestampData(stringToMillisOfDay(str), 0);
}

TimestampData TimestampData::fromString(const std::string& str)
{
    return TimestampData(stringToEpochMillis(str), 0);
}

TimestampData TimestampData::fromLocalTimeString(const std::string& str)
{
    size_t pos = str.find_last_of('Z');
    if (pos == std::string::npos) {
        throw std::invalid_argument("Invalid timestamp_with_lzt string");
    }
    return TimestampData(stringToEpochMillis(str.substr(0, pos)), 0);
}
