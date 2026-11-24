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

#include <stdexcept>
#include "basictypes/Array.h"
#include "basictypes/ObjectPool.h"

void Array::append(Object* value)
{
    if (value) value->getRefCount();
    push_back(value);
}

void Array::set(int index, Object* obj)
{
    if (index >= length || index < 0) {
        throw std::out_of_range("Index out of range[set]");
    }
    dataSize_ -= data_[index] ? data_[index]->sizeInBytes() : 0;
    if (data_[index]) data_[index]->putRefCount();
    data_[index] = obj;
    if (obj) ((Object*)obj)->getRefCount();
    dataSize_ += obj ? obj->sizeInBytes() : 0;
}

Object* Array::get(int index)
{
    if (index >= length || index < 0) {
        throw std::out_of_range("Index out of range[get]");
    }
    return data_[index];
}

bool Array::equals(Object* obj)
{
    if (obj == nullptr || dynamic_cast<Array*>(obj) == nullptr) {
        return false;
    }
    auto a = reinterpret_cast<Array*>(obj);
    for (int i = 0; i < this->length; i++) {
        Object* src = (Object*)(this->data_[i]);
        Object* dst = (Object*)(a->data_[i]);
        if (!src->equals(dst)) return false;
    }
    return true;
}

Object* Array::clone()
{
    Array* a = new Array(this->length);
    for (int i = 0; i < this->length; i++) {
        Object* arr = (Object*)this->data_[i];
        a->data_[i] = (Object*)arr->clone();
    }
    // Clone is content-equal, so it carries the same total element bytes.
    a->dataSize_ = this->dataSize_;
    return a;
}

void Array::clear()
{
    for (int i = 0; i < this->length; ++i) {
        if (data_ != nullptr && data_[i]) {
            data_[i]->putRefCount();
            data_[i] = nullptr;
        }
    }
    length = 0;
}

void Array::putRefCount()
{
    if (--refCount <= 0) {
        if (this->isPool) {
            this->clear();
            this->refCount = 1;
            ObjectPool<Array>* arrayObjectPool = ObjectPool<Array>::getInstance();
            this->next = arrayObjectPool->head;
            arrayObjectPool->head = this;
        } else {
            delete this;
        }
    }
}
