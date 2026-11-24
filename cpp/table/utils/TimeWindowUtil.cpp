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

#include "table/utils/TimeWindowUtil.h"

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <limits>

#include "OmniOperatorJIT/core/src/codegen/time_util.h"

namespace {
constexpr int64_t MILLIS_PER_HOUR = 60LL * 60LL * 1000LL;

bool IsUtcTimeZone(const std::string& shiftTimeZone)
{
    return shiftTimeZone.empty() || shiftTimeZone == "UTC" || shiftTimeZone == "Z";
}

bool HasFixedOffsetFastPath(const std::string& shiftTimeZone, int64_t& offsetMillis)
{
    if (shiftTimeZone == "Asia/Shanghai" || shiftTimeZone == "GMT+08:00") {
        offsetMillis = 8 * MILLIS_PER_HOUR;
        return true;
    }
    if (shiftTimeZone == "Etc/GMT-8") {
        offsetMillis = 8 * MILLIS_PER_HOUR;
        return true;
    }
    if (shiftTimeZone == "Etc/GMT+8") {
        offsetMillis = -8 * MILLIS_PER_HOUR;
        return true;
    }
    return false;
}

void SetTimeZone(const std::string& shiftTimeZone)
{
    setenv("TZ", omniruntime::codegen::function::TimeZoneUtil::GetTZ(shiftTimeZone.c_str()), 1);
    tzset();
}

struct TimestampParts {
    time_t seconds;
    int millis;
};

TimestampParts SplitMillis(int64_t epochMills)
{
    TimestampParts parts;
    parts.seconds = epochMills / 1000;
    parts.millis = static_cast<int>(epochMills % 1000);
    if (parts.millis < 0) {
        parts.seconds -= 1;
        parts.millis += 1000;
    }
    return parts;
}

int64_t CombineMillis(time_t seconds, int millis)
{
    return seconds * 1000LL + millis;
}
} // namespace

int64_t TimeWindowUtil::toUtcTimestampMills(int64_t epochMills, const std::string& shiftTimeZone)
{
    if (IsUtcTimeZone(shiftTimeZone) || epochMills == INT64_MAX) {
        return epochMills;
    }

    int64_t fixedOffsetMillis = 0;
    if (HasFixedOffsetFastPath(shiftTimeZone, fixedOffsetMillis)) {
        return epochMills + fixedOffsetMillis;
    }

    const TimestampParts parts = SplitMillis(epochMills);
    SetTimeZone(shiftTimeZone);
    struct tm localTime{};
    localtime_r(&parts.seconds, &localTime);

    SetTimeZone("UTC");
    tzset();
    return CombineMillis(timegm(&localTime), parts.millis);
}

int64_t TimeWindowUtil::toEpochMills(int64_t utcTimestampMills, const std::string& shiftTimeZone)
{
    if (IsUtcTimeZone(shiftTimeZone) || utcTimestampMills == INT64_MAX) {
        return utcTimestampMills;
    }

    int64_t fixedOffsetMillis = 0;
    if (HasFixedOffsetFastPath(shiftTimeZone, fixedOffsetMillis)) {
        return utcTimestampMills - fixedOffsetMillis;
    }

    const TimestampParts parts = SplitMillis(utcTimestampMills);
    SetTimeZone("UTC");
    struct tm utcTime{};
    gmtime_r(&parts.seconds, &utcTime);

    SetTimeZone(shiftTimeZone);
    return CombineMillis(mktime(&utcTime), parts.millis);
}

int64_t TimeWindowUtil::toEpochMillsForTimer(int64_t utcTimestampMills, const std::string& shiftTimeZone)
{
    if (IsUtcTimeZone(shiftTimeZone) || utcTimestampMills == INT64_MAX) {
        return utcTimestampMills;
    }

    return toEpochMills(utcTimestampMills, shiftTimeZone);
}

int64_t TimeWindowUtil::toCleanupTimerMills(
    int64_t windowMaxTimestamp, int64_t allowedLateness, const std::string& shiftTimeZone)
{
    if (windowMaxTimestamp == INT64_MAX) {
        return INT64_MAX;
    }
    const int64_t cleanupUtcTimestamp = windowMaxTimestamp + allowedLateness;
    if (cleanupUtcTimestamp < windowMaxTimestamp) {
        return INT64_MAX;
    }
    return toEpochMillsForTimer(cleanupUtcTimestamp, shiftTimeZone);
}

bool TimeWindowUtil::isWindowFired(int64_t windowEnd, int64_t currentProgress, const std::string& shiftTimeZone)
{
    if (windowEnd == LONG_MAX) {
        return false;
    }
    const int64_t windowTriggerTime = toEpochMillsForTimer(windowEnd - 1, shiftTimeZone);
    return currentProgress >= windowTriggerTime;
}

bool TimeWindowUtil::isWindowFired(int64_t windowEnd, int64_t currentProgress)
{
    if (windowEnd == LONG_MAX) {
        return false;
    }
    return currentProgress >= windowEnd - 1;
}
