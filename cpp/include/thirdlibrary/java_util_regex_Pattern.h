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

#ifndef ME_CHECK_JAVA_UTIL_REGEX_PATTERN_H
#define ME_CHECK_JAVA_UTIL_REGEX_PATTERN_H
#include <regex>
#include "java_util_regex_Matcher.h"
#include "basictypes/String.h"

class Pattern : public Object {
public:
    Pattern();

    Pattern(std::string str);

    static Pattern* compile(std::string str);

    static String* quote(String* str);

    Matcher* matcher(std::string str);

    Matcher* matcher(String* str);

    std::regex pattern;
};
#endif // ME_CHECK_JAVA_UTIL_REGEX_PATTERN_H
