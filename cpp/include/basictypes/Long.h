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

#ifndef FLINK_TNEL_LONG_H
#define FLINK_TNEL_LONG_H

#include <iostream>
#include "String.h"
#include "basictypes/ObjectPool.h"

class Long : public Object {
public:
    Long();

    Long(int64_t val);

    ~Long();

    inline int64_t getValue() const;

    inline void setValue(int64_t val);

    inline int hashCode() override;

    inline bool equals(Object* obj) override;

    inline std::string toString() override;

    inline Object* clone() override;

    inline int64_t longValue();

    static inline Long* valueOf(String* str);

    static inline Long* valueOf_tune(String* str);

    static inline Long* valueOf(std::string str);

    static inline Long* valueOf_tune(std::string str);

    static inline Long* valueOf(int64_t val);

    static inline Long* valueOf_tune(int64_t val);

    inline void putRefCount() override;

    int64_t value;
    Long* next = nullptr;
    inline void setValue(const std::string& basicString) override;

    int64_t sizeInBytes() const override
    {
        return static_cast<int64_t>(sizeof(*this));
    }

protected:
    static std::uint64_t parseLong(std::string_view s);
};

inline int64_t Long::getValue() const
{
    return value;
}

inline void Long::setValue(int64_t val)
{
    value = val;
}

inline int Long::hashCode()
{
    return (int)(value ^ (static_cast<uint64_t>(value) >> 32));
}

inline bool Long::equals(Object* obj)
{
    Long* ptr = reinterpret_cast<Long*>(obj);
    int64_t val = ptr->getValue();
    return value == val ? true : false;
}

inline std::string Long::toString()
{
    return std::to_string(value);
}

inline Object* Long::clone()
{
    return new Long(value);
}

inline int64_t Long::longValue()
{
    return value;
}

inline Long* Long::valueOf(String* str)
{
    return valueOf_tune(str);
}

inline Long* Long::valueOf_tune(String* str)
{
    std::string_view value = str->getValue();
    uint64_t val = parseLong(value);
    ObjectPool<Long>* longObjectPool = ObjectPool<Long>::getInstance();
    Long* curLong = longObjectPool->getObject();
    curLong->setValue(val);
    return curLong;
}

inline Long* Long::valueOf(std::string str)
{
    return valueOf_tune(str);
}

inline Long* Long::valueOf_tune(std::string str)
{
    uint64_t val = parseLong(str);
    ObjectPool<Long>* longObjectPool = ObjectPool<Long>::getInstance();
    Long* curLong = longObjectPool->getObject();
    curLong->setValue(val);
    return curLong;
}

inline Long* Long::valueOf(int64_t val)
{
    return valueOf_tune(val);
}

inline Long* Long::valueOf_tune(int64_t val)
{
    ObjectPool<Long>* longObjectPool = ObjectPool<Long>::getInstance();
    Long* curLong = longObjectPool->getObject();
    curLong->setValue(val);
    return curLong;
}

inline std::uint64_t Long::parseLong(std::string_view s)
{
    std::uint64_t result = 0;
    for (char digit : s) {
        if (digit < 48 || digit > 57) {
            throw std::out_of_range("parseLong out digit range");
        } // 0: 48; 9: 57
        result *= 10;
        result += digit - '0';
    }
    return result;
}

inline void Long::putRefCount()
{
    if (--refCount <= 0) {
        if (this->isPool) {
            // this->refCount = 1;
            ObjectPool<Long>* longObjectPool = ObjectPool<Long>::getInstance();
            this->next = longObjectPool->head;
            longObjectPool->head = this;
        } else {
            delete this;
        }
    }
}

inline void Long::setValue(const std::string& basicString)
{
    this->value = std::stol(basicString);
}

#endif // FLINK_TNEL_LONG_H
