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

#ifndef FLINK_TNEL_DateTimeFormatter_H
#define FLINK_TNEL_DateTimeFormatter_H

#include <cstdint>
#include <unordered_map>
#include <string>
#include <ctime>
#include <stdexcept>
#include "basictypes/String.h"
#include "java_time_temporal_TemporalAccessor.h"

class DateTimeFormatter : public Object {
public:
    DateTimeFormatter();

    ~DateTimeFormatter();

    static DateTimeFormatter* ofPattern(const std::string& pattern);

    String* format(TemporalAccessor* temporal);

private:
    static std::unordered_map<std::string, std::string> patterns;
    std::string pattern;

    DateTimeFormatter(const std::string& pat);
};

#endif // FLINK_TNEL_DateTimeFormatter_H
