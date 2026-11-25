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

#ifndef OMNISTREAM_TIMEWINDOWUTIL_H
#define OMNISTREAM_TIMEWINDOWUTIL_H

#include <cstdint>
#include <climits>
#include <string>

class TimeWindowUtil {
public:
    static int64_t toUtcTimestampMills(int64_t epochMills, const std::string& shiftTimeZone);

    static int64_t toEpochMills(int64_t utcTimestampMills, const std::string& shiftTimeZone);

    static int64_t toEpochMillsForTimer(int64_t utcTimestampMills, const std::string& shiftTimeZone);

    static int64_t toCleanupTimerMills(
        int64_t windowMaxTimestamp, int64_t allowedLateness, const std::string& shiftTimeZone);

    static bool isWindowFired(int64_t windowEnd, int64_t currentProgress, const std::string& shiftTimeZone);

    static bool isWindowFired(int64_t windowEnd, int64_t currentProgress);
};

#endif // OMNISTREAM_TIMEWINDOWUTIL_H
