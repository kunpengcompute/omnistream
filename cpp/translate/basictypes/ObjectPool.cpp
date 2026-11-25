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

#include "basictypes/ObjectPool.h"
#include "basictypes/Long.h"
#include "basictypes/String.h"
#include "basictypes/Tuple2.h"

constexpr int POOL_SIZE = 16;

template <typename T>
ObjectPool<T>* ObjectPool<T>::getInstance()
{
    thread_local static ObjectPool<T> instance(POOL_SIZE);
    return &instance;
}

template <typename T>
ObjectPool<T>::ObjectPool(size_t poolSize)
{
    while (poolSize > 0) {
        T* object = new T();
        object->isPool = true;
        object->next = head;
        head = object;
        poolSize--;
    }
}

template <typename T>
void ObjectPool<T>::capacityExpansion()
{
    size_t size = capacityExpansionNum;
    while (size > 0) {
        T* object = new T();
        object->isPool = true;
        object->next = head;
        head = object;
        size--;
    }
    this->capacityExpansionNum = (this->capacityExpansionNum << 1);
}

template <typename T>
T* ObjectPool<T>::getObject()
{
    // ObjectPool<T> *objectPool = ObjectPool<T>::getInstance();
    T* curObject = this->head;
    if (curObject == nullptr) {
        this->capacityExpansion();
        curObject = this->head;
    }
    /*if (curObject == nullptr) {
        throw std::runtime_error("curObject ptr is null");
    }*/
    this->head = curObject->next;
    curObject->next = nullptr;
    curObject->refCount = 1;
    return curObject;
}

template <typename T>
ObjectPool<T>::~ObjectPool()
{
    while (head != nullptr) {
        T* cur = head;
        head = head->next;
        delete cur;
    }
}

template class ObjectPool<Long>;
template class ObjectPool<String>;
template class ObjectPool<Array>;
template class ObjectPool<Tuple2>;
