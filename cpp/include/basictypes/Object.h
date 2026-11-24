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

#ifndef FLINK_TNEL_OBJECT_H
#define FLINK_TNEL_OBJECT_H

#include <string>
#include <atomic>
#include <mutex>
#include <cstdint>
#include "nlohmann/json.hpp"
#include "core/include/common.h"

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
class Class;
class Object {
public:
    Object();

    Object(nlohmann::json jsonObj);

    virtual ~Object();

    virtual int hashCode();

    virtual bool equals(Object* obj);

    virtual std::string toString();

    virtual Object* clone();

    Object(const Object& obj);

    Object(Object&& obj);

    Object& operator=(const Object& obj);

    Object& operator=(Object&& obj);

    virtual void putRefCount();

    inline void getRefCount()
    {
        // relaxed is enough to acquire a reference: the caller already holds one, so the object
        // cannot be destroyed underneath this increment.
        refCount.fetch_add(1, std::memory_order_relaxed);
    }

    void setRefCount(uint64_t count);

    bool isCloned();

    uint64_t getRefCountNumber();

    virtual void setValue(const std::string& value);

    virtual Class* getObjectClass();

    static Class* getClass();

    // Approximate retained byte size of this object, including out-of-line bytes it owns.
    // Default is the shallow base size; value-bearing subclasses override to add their payload.
    virtual int64_t sizeInBytes() const
    {
        return static_cast<int64_t>(sizeof(Object));
    }

public:
    std::recursive_mutex mutex;
    bool isClone = false;
    bool isPool = false;
    // Touched from task threads and from Flink's metrics ViewUpdater thread. A plain integer here
    // loses increments under contention, which means either a premature delete in putRefCount or a
    // permanent leak.
    std::atomic<uint64_t> refCount{1};
};

namespace std {
template <>
struct hash<Object*> {
    std::size_t operator()(Object* nsPtr) const noexcept
    {
        return nsPtr->hashCode();
    }
};

template <>
struct equal_to<Object*> {
    bool operator()(Object* lhs, Object* rhs) const noexcept
    {
        return lhs->equals(rhs);
    }
};
} // namespace std

#endif // FLINK_TNEL_OBJECT_H
