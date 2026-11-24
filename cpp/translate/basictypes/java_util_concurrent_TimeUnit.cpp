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

#include "basictypes/java_util_concurrent_TimeUnit.h"

// 构造函数
constexpr JavaUtilTimeUnit::JavaUtilTimeUnit(Value unit) : unit_(unit)
{
}

// 转换为纳秒
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toNanos(Rep duration) const
{
    return convert<Rep, std::nano>(duration);
}

// 转换为微秒
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toMicros(Rep duration) const
{
    return convert<Rep, std::micro>(duration);
}

// 转换为毫秒
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toMillis(Rep duration) const
{
    return convert<Rep, std::milli>(duration);
}

// 转换为秒
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toSeconds(Rep duration) const
{
    return convert<Rep, std::ratio<1>>(duration);
}

// 转换为分钟
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toMinutes(Rep duration) const
{
    return convert<Rep, std::ratio<60>>(duration);
}

// 转换为小时
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toHours(Rep duration) const
{
    return convert<Rep, std::ratio<3600>>(duration);
}

// 转换为天
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::toDays(Rep duration) const
{
    return convert<Rep, std::ratio<86400>>(duration);
}

// 时间单位转换（通用方法）
template <typename Rep>
constexpr int64_t JavaUtilTimeUnit::convert(Rep sourceDuration, const JavaUtilTimeUnit& sourceUnit) const
{
    // 先将源单位转换为纳秒
    int64_t nanos = sourceUnit.toNanos(sourceDuration);
    // 再将纳秒转换到目标单位
    switch (unit_) {
        case NANOSECONDS: return nanos;
        case MICROSECONDS: return nanos / 1000;
        case MILLISECONDS: return nanos / 1000000;
        case SECONDS: return nanos / 1000000000;
        case MINUTES: return nanos / 60000000000LL;
        case HOURS: return nanos / 3600000000000LL;
        case DAYS: return nanos / 86400000000000LL;
        default: throw std::invalid_argument("Invalid time unit");
    }
}

// 线程休眠
void JavaUtilTimeUnit::sleep(int64_t timeout) const
{
    switch (unit_) {
        case NANOSECONDS: std::this_thread::sleep_for(std::chrono::nanoseconds(timeout)); break;
        case MICROSECONDS: std::this_thread::sleep_for(std::chrono::microseconds(timeout)); break;
        case MILLISECONDS: std::this_thread::sleep_for(std::chrono::milliseconds(timeout)); break;
        case SECONDS: std::this_thread::sleep_for(std::chrono::seconds(timeout)); break;
        case MINUTES: std::this_thread::sleep_for(std::chrono::minutes(timeout)); break;
        case HOURS: std::this_thread::sleep_for(std::chrono::hours(timeout)); break;
        case DAYS: std::this_thread::sleep_for(std::chrono::hours(24 * timeout)); break;
        default: throw std::invalid_argument("Invalid time unit for sleep");
    }
}

// 获取单位值
constexpr JavaUtilTimeUnit::Value JavaUtilTimeUnit::value() const
{
    return unit_;
}

// 内部转换实现
template <typename Rep, typename Period>
constexpr int64_t JavaUtilTimeUnit::convert(Rep duration) const
{
    using namespace std::chrono;
    using SourceDuration = std::chrono::duration<Rep, Period>;
    using TargetDuration = std::chrono::duration<Rep, std::nano>;

    // 根据当前单位创建适当的持续时间
    switch (unit_) {
        case NANOSECONDS: return SourceDuration(duration).count();
        case MICROSECONDS: return duration_cast<TargetDuration>(microseconds(duration)).count();
        case MILLISECONDS: return duration_cast<TargetDuration>(milliseconds(duration)).count();
        case SECONDS: return duration_cast<TargetDuration>(seconds(duration)).count();
        case MINUTES: return duration_cast<TargetDuration>(minutes(duration)).count();
        case HOURS: return duration_cast<TargetDuration>(hours(duration)).count();
        case DAYS: return duration_cast<TargetDuration>(hours(24 * duration)).count();
        default: throw std::invalid_argument("Invalid time unit");
    }
}

template int64_t JavaUtilTimeUnit::toMillis<int64_t>(int64_t) const;
// 初始化静态常量
JavaUtilTimeUnit JavaUtilTimeUnit::NANOS = JavaUtilTimeUnit(JavaUtilTimeUnit::NANOSECONDS);
JavaUtilTimeUnit JavaUtilTimeUnit::MICROS = JavaUtilTimeUnit(JavaUtilTimeUnit::MICROSECONDS);
JavaUtilTimeUnit JavaUtilTimeUnit::MILLIS = JavaUtilTimeUnit(JavaUtilTimeUnit::MILLISECONDS);
JavaUtilTimeUnit JavaUtilTimeUnit::SECS = JavaUtilTimeUnit(JavaUtilTimeUnit::SECONDS);
JavaUtilTimeUnit JavaUtilTimeUnit::MINS = JavaUtilTimeUnit(JavaUtilTimeUnit::MINUTES);
JavaUtilTimeUnit JavaUtilTimeUnit::HS = JavaUtilTimeUnit(JavaUtilTimeUnit::HOURS);
JavaUtilTimeUnit JavaUtilTimeUnit::DS = JavaUtilTimeUnit(JavaUtilTimeUnit::DAYS);
