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

#include "thirdlibrary/java_util_regex_Pattern.h"

Pattern::Pattern() = default;

Pattern::Pattern(std::string str)
{
    this->pattern = std::regex(str);
}

Pattern* Pattern::compile(std::string str)
{
    return new Pattern(str);
}

String* Pattern::quote(String* str)
{
    return str;
}

Matcher* Pattern::matcher(std::string str)
{
    return new Matcher(str, pattern);
}

Matcher* Pattern::matcher(String* str)
{
    return matcher(str->toString());
}
