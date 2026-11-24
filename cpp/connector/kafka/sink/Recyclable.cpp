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

#include "Recyclable.h"
#include <stdexcept>

template <typename T>
Recyclable<T>::Recyclable(T* object, std::function<void(T*)> recycler) : object(object),
                                                                         recycler(recycler)
{
    if (object == nullptr) {
        throw std::invalid_argument("Object cannot be null");
    }
    if (recycler == nullptr) {
        throw std::invalid_argument("Recycler cannot be null");
    }
}

template <typename T>
Recyclable<T>::~Recyclable()
{
    Close();
}

template <typename T>
T* Recyclable<T>::GetObject()
{
    if (IsRecycled()) {
        throw std::runtime_error("Already recycled");
    }
    return object;
}

template <typename T>
bool Recyclable<T>::IsRecycled()
{
    return object == nullptr;
}

template <typename T>
void Recyclable<T>::Close()
{
    if (recycler && object) {
        recycler(object);
        object = nullptr;
    }
}

template Recyclable<FlinkKafkaInternalProducer>::Recyclable(
    FlinkKafkaInternalProducer* object, std::function<void(FlinkKafkaInternalProducer*)> recycler);
template Recyclable<FlinkKafkaInternalProducer>::~Recyclable();
template FlinkKafkaInternalProducer* Recyclable<FlinkKafkaInternalProducer>::GetObject();
template bool Recyclable<FlinkKafkaInternalProducer>::IsRecycled();
template void Recyclable<FlinkKafkaInternalProducer>::Close();
