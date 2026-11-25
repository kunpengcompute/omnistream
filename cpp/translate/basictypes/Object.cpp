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

#include "basictypes/Object.h"
#include "basictypes/Class.h"

Object::Object() = default;

Object::Object(nlohmann::json jsonObj)
{
    return;
}

Object::~Object() = default;

int Object::hashCode()
{
    return 0;
}

bool Object::equals(Object* obj)
{
    return false;
}

std::string Object::toString()
{
    return std::string();
}

Object* Object::clone()
{
    return nullptr;
}

Object::Object(const Object& obj)
{
    this->refCount = 1;
    this->isClone = obj.isClone;
}

Object::Object(Object&& obj)
{
    this->refCount = obj.refCount.load(std::memory_order_relaxed);
    this->isClone = obj.isClone;
}

Object& Object::operator=(const Object& obj)
{
    this->refCount = obj.refCount.load(std::memory_order_relaxed);
    this->isClone = obj.isClone;
    return *this;
}

Object& Object::operator=(Object&& obj)
{
    this->refCount = obj.refCount.load(std::memory_order_relaxed);
    this->isClone = obj.isClone;
    return *this;
}

void Object::putRefCount()
{
    // fetch_sub returns the value before the decrement, so "was not 1" means references remain.
    // acq_rel pairs the release of this thread's writes with the acquire performed by whichever
    // thread drops the count to zero, so the destructor sees a fully published object and cannot
    // be reordered before the decrement.
    if (__builtin_expect(refCount.fetch_sub(1, std::memory_order_acq_rel) != 1, true)) {
        return;
    }
    // Requires the object to have been heap allocated. Stack- or static-allocated Objects must
    // never reach putRefCount.
    delete this;
}

void Object::setRefCount(uint64_t count)
{
    refCount = count;
}

bool Object::isCloned()
{
    return isClone;
}

uint64_t Object::getRefCountNumber()
{
    return refCount;
}

void Object::setValue(const std::string& value)
{
    NOT_IMPL_EXCEPTION;
}

Class* Object::getClass()
{
    return nullptr;
}

Class* Object::getObjectClass()
{
    return nullptr;
}
