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

#ifndef OMNISTREAM_JAVA_UTIL_CONCURRENT_TIMEUNIT_H
#define OMNISTREAM_JAVA_UTIL_CONCURRENT_TIMEUNIT_H

#include <chrono>
#include <thread>
#include <ratio>
#include <stdexcept>
#include <cstdint>
#include <type_traits>

class JavaUtilTimeUnit {
public:
    // 时间单位枚举
    enum Value {
        NANOSECONDS,
        MICROSECONDS,
        MILLISECONDS,
        SECONDS,
        MINUTES,
        HOURS,
        DAYS
    };

    // 构造函数
    constexpr JavaUtilTimeUnit(Value unit);

    // 转换为纳秒
    template <typename Rep = int64_t>
    constexpr int64_t toNanos(Rep duration) const;

    // 转换为微秒
    template <typename Rep = int64_t>
    constexpr int64_t toMicros(Rep duration) const;

    // 转换为毫秒
    template <typename Rep = int64_t>
    constexpr int64_t toMillis(Rep duration) const;

    // 转换为秒
    template <typename Rep = int64_t>
    constexpr int64_t toSeconds(Rep duration) const;

    // 转换为分钟
    template <typename Rep = int64_t>
    constexpr int64_t toMinutes(Rep duration) const;

    // 转换为小时
    template <typename Rep = int64_t>
    constexpr int64_t toHours(Rep duration) const;

    // 转换为天
    template <typename Rep = int64_t>
    constexpr int64_t toDays(Rep duration) const;

    // 时间单位转换（通用方法）
    template <typename Rep = int64_t>
    constexpr int64_t convert(Rep sourceDuration, const JavaUtilTimeUnit& sourceUnit) const;

    // 线程休眠
    void sleep(int64_t timeout) const;

    // 获取单位值
    constexpr Value value() const;

    // 静态常量单位
    static JavaUtilTimeUnit NANOS;
    static JavaUtilTimeUnit MICROS;
    static JavaUtilTimeUnit MILLIS;
    static JavaUtilTimeUnit SECS;
    static JavaUtilTimeUnit MINS;
    static JavaUtilTimeUnit HS;
    static JavaUtilTimeUnit DS;

private:
    // 内部转换实现
    template <typename Rep, typename Period>
    constexpr int64_t convert(Rep duration) const;

    Value unit_;
};

#endif // OMNISTREAM_JAVA_UTIL_CONCURRENT_TIMEUNIT_H
