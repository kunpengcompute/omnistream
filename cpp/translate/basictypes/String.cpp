/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "basictypes/String.h"
#include "basictypes/ObjectPool.h"

String::String() : hash(0)
{
}

String::String(const String* str) : inner(str->inner), hash(0)
{
}

String::String(const char* str, const size_t size) : inner(str, str + size), hash(0)
{
}

String::String(const std::string& str) : inner(str), hash(0)
{
}

String::String(std::string&& str) noexcept : inner(std::move(str)), hash(0)
{
}

String::String(const String& str) = default;

String::String(String&& str) = default;

String& String::operator=(const String& str) = default;

String& String::operator=(String&& str) = default;

String::~String() = default;

void String::putRefCount()
{
    if (--refCount <= 0) {
        if (this->isPool) {
            // inner.clear();
            // this->refCount = 1;
            ObjectPool<String>* stringObjectPool = ObjectPool<String>::getInstance();
            this->next = stringObjectPool->head;
            stringObjectPool->head = this;
        } else {
            delete this;
        }
    }
}

String* String::replace(const std::string& target, const std::string& replacement)
{
    std::string str = this->toString();
    const std::string& from = target;
    const std::string& to = replacement;
    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
    return new String(str);
}

String* String::replace(const String* target, const String* replacement)
{
    return this->replace(target->inner, replacement->inner);
}

Array* String::split(const std::string& pattern)
{
    ObjectPool<Array>* arrayObjectPool = ObjectPool<Array>::getInstance();
    Array* curArray = arrayObjectPool->getObject();
    ObjectPool<String>* stringObjectPool = ObjectPool<String>::getInstance();

    size_t start = 0;
    size_t index = inner.find(pattern);
    while (index != std::string::npos) {
        String* subStr = stringObjectPool->getObject();
        subStr->setValue(inner.substr(start, index - start));
        curArray->push_back(subStr);
        start = index + pattern.length();
        index = inner.find(pattern, start);
    }
    String* subStr = stringObjectPool->getObject();
    subStr->setValue(inner.substr(start));
    curArray->push_back(subStr);
    return curArray;
}

Array* String::split(const std::regex& re)
{
    std::string str = this->toString();
    std::sregex_token_iterator it(str.begin(), str.end(), re, -1);
    auto* arr = new Array();
    for (const std::sregex_token_iterator reg_end; it != reg_end; ++it) {
        auto* tmp = new String(it->str());
        arr->push_back(tmp);
    }
    return arr;
}

Array* String::split(const String* patt)
{
    return split(patt->inner);
}

String* String::replaceAll_tune(const std::string& replace) const
{
    const auto result = ReplaceAllAcc(inner, replace);
    return new String(result);
}

String* String::replaceAll_tune(const std::string& pattern, const std::string& replace) const
{
    if (pattern == "[^A-Za-z0-9_/.]+") {
        const auto result = ReplaceAllAcc(inner, replace);
        return new String(result);
    }
    static const auto regex = std::regex(pattern);
    const std::string res = std::regex_replace(inner, regex, replace);
    return new String(res);
}

String* String::replaceAll(const std::string& pattern, const std::string& replace) const
{
    std::regex regex_;
    if (pattern == "[^A-Za-z0-9_/.]+") {
        static const auto regex = std::regex(pattern);
        regex_ = regex;
    } else {
        const std::regex tmp(pattern);
        regex_ = tmp;
    }
    const std::string res = std::regex_replace(inner, regex_, replace);
    return new String(res);
}
