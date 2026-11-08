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
#ifndef FLINK_TNEL_STRING_H
#define FLINK_TNEL_STRING_H

#include <string_view>
#include <regex>
#include "Array.h"
#include "khsel_ops.h"

class String : public Object {
public:
    String();

    explicit String(const String* str);

    explicit String(const char* str, const size_t size);

    explicit String(const std::string& str);

    explicit String(std::string&& str) noexcept;

    String(const String& str);

    String(String&& str);

    String& operator=(const String& str);

    String& operator=(String&& str);

    ~String() override;

    virtual inline std::string_view getValue();

    virtual inline char* getData();

    virtual inline size_t getSize();

    inline void setValue(const std::string& val) override;

    inline void setValue(const std::string_view& val);

    inline void setData(const char* pointer);

    inline int hashCode() override;

    inline bool equals(Object* obj) override;

    inline std::string toString() override;

    inline Object* clone() override;

    inline void resize(const int64_t size);

    inline char* data();

    String* replace(const std::string& target, const std::string& replacement);

    String* replace(const String* target, const String* replacement);

    Array* split(const std::string& pattern);

    Array* split(const std::regex& re);

    Array* split(const String* patt);

    /* only work for replace pattern: "[^A-Za-z0-9_/.]+" */
    String* replaceAll_tune(const std::string& replace) const;

    String* replaceAll_tune(const std::string& pattern, const std::string& replace) const;

    String* replaceAll(const std::string& pattern, const std::string& replace) const;

    inline int32_t lastIndexOf(const std::string& s_patt) const;

    inline int32_t lastIndexOf(const String* s_patt) const;

    inline int32_t length() const;

    inline String* substring(const int32_t idx) const;

    inline String* substring(const int32_t start, const int32_t end) const;

    inline bool equals(const String* obj) const;

    inline bool equals(String* obj);

    inline bool equals(const std::string& str) const;

    inline bool contains(const std::string& str) const;

    inline bool contains(const String* str) const;

    inline bool endsWith(const String* str) const;

    inline bool endsWith(const std::string& str);

    inline bool startsWith(const String* str) const;

    inline bool startsWith(const std::string& str) const;

    inline static std::unique_ptr<String> valueOf(Object* obj);

    inline static String* valueOf(int32_t val);

    inline static String* valueOf(int64_t val);

    inline std::string_view ref();

    virtual inline void setData(char* pointer);

    inline void setValue(char* pointer, size_t size);

    virtual inline void setSize(size_t size);

    void putRefCount() override;

    String* next = nullptr;
    int64_t sizeInBytes() const override
    {
        return static_cast<int64_t>(sizeof(String) + inner.size());
    }

private:
    std::string inner;
    int hash;
};

inline std::string_view String::getValue()
{
    return inner;
}

inline char* String::getData()
{
    return inner.data();
}

inline size_t String::getSize()
{
    return static_cast<size_t>(inner.size());
}

inline void String::setValue(const std::string& val)
{
    inner = val;
    hash = 0;
}

inline void String::setValue(const std::string_view& val)
{
    inner = val;
    hash = 0;
}

inline void String::setData(const char* pointer)
{
    inner = pointer;
    hash = 0;
}

inline int String::hashCode()
{
    size_t usedSize = inner.size();
    if (usedSize == 0) {
        return 0;
    }
    int64_t hash = 0;
    for (size_t i = 0; i < usedSize; ++i) {
        hash = 31 * hash + static_cast<int>(inner[i]);
        if (hash > INT32_MAX || hash < INT32_MIN) {
            hash = (int)hash;
        }
    }
    return (int)hash;
}

inline bool String::equals(Object* obj)
{
    const auto* str = reinterpret_cast<String*>(obj);
    return this->inner == str->inner;
}

inline std::string String::toString()
{
    return inner;
}

inline Object* String::clone()
{
    return new String(inner);
}

inline void String::resize(const int64_t size)
{
    inner.resize(size);
}

inline char* String::data()
{
    return inner.data();
}

// std::unique_ptr<Array> String::splitToUniquePtr(const std::string &pattern) {
//    return std::unique_ptr<Array>(this->split(pattern));
// }

inline int32_t String::lastIndexOf(const std::string& s_patt) const
{
    return static_cast<int32_t>(inner.rfind(s_patt));
}

inline int32_t String::lastIndexOf(const String* s_patt) const
{
    return static_cast<int32_t>(inner.rfind(s_patt->inner));
}

inline int32_t String::length() const
{
    return static_cast<int32_t>(inner.size());
}

inline String* String::substring(const int32_t idx) const
{
    std::string s = this->inner.substr(idx);
    return new String(std::move(s));
}

inline String* String::substring(const int32_t start, const int32_t end) const
{
    std::string s = this->inner.substr(start, end);
    return new String(std::move(s));
}

inline bool String::equals(const String* obj) const
{
    auto& cur = this->inner;
    auto& val = obj->inner;
    return cur == val;
}

inline bool String::equals(String* obj)
{
    auto& cur = this->inner;
    auto& val = obj->inner;
    return cur == val;
}

inline bool String::equals(const std::string& str) const
{
    auto& cur = this->inner;
    return cur == str;
}

inline bool String::contains(const std::string& str) const
{
    if (!&str) {
        throw std::invalid_argument("Input string is null");
    }

    return (this->inner.find(str) != std::string::npos);
}

inline bool String::contains(const String* str) const
{
    return (this->inner.find(str->inner) != std::string::npos);
}

inline bool String::endsWith(const String* str) const
{
    auto& s1 = this->inner;
    auto& s2 = str->inner;
    return std::equal(s2.rbegin(), s2.rend(), s1.rbegin());
}

inline bool String::endsWith(const std::string& str)
{
    auto& s1 = this->inner;
    return std::equal(str.rbegin(), str.rend(), s1.rbegin());
}

inline bool String::startsWith(const String* str) const
{
    return this->inner.find(str->inner) == 0;
}

inline bool String::startsWith(const std::string& str) const
{
    return inner.find(str) == 0;
}

inline std::unique_ptr<String> String::valueOf(Object* obj)
{
    if (obj) {
        return std::make_unique<String>(obj->toString());
    }
    return nullptr;
}

inline String* String::valueOf(int32_t obj)
{
    return new String(std::to_string(obj));
}

inline String* String::valueOf(int64_t obj)
{
    return new String(std::to_string(obj));
}

inline std::string_view String::ref()
{
    return this->inner;
}

inline void String::setData(char* pointer)
{
    inner = pointer;
}

inline void String::setValue(char* pointer, size_t size)
{
    inner.assign(pointer, size);
}

inline void String::setSize(size_t size)
{
    inner.resize(size);
}

#endif // FLINK_TNEL_STRING_H
