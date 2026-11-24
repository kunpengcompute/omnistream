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

#include "basictypes/JavaArray.h"

template <typename T>
void JavaArray<T>::set(int index, T value)
{
    if (index >= length || index < 0) {
        throw std::out_of_range("Index out of range");
    }
    data_[index] = value;
}

template <typename T>
T JavaArray<T>::get(int index)
{
    if (index >= length || index < 0) {
        throw std::out_of_range("Index out of range");
    }
    return data_[index];
}

// short、int、long、float、double、bool、char
template <typename T>
bool JavaArray<T>::equals(Object* obj)
{
    JavaArray* a = (JavaArray*)obj;
    return (*this == *a);
}

template <typename T>
Object* JavaArray<T>::clone()
{
    int length = this->length;
    JavaArray* a = new JavaArray(length);
    errno_t ret = memcpy_s((void*)a->data_, sizeof(T) * length, (void*)this->data_, sizeof(T) * length);
    if (ret != 0) {
        throw std::runtime_error("memcpy_s failed" + std::to_string(ret));
    }
    return (Object*)a;
}

template class JavaArray<long>;
template class JavaArray<int>;
template class JavaArray<short>;
template class JavaArray<char>;
template class JavaArray<float>;
template class JavaArray<double>;
template class JavaArray<bool>;
template class JavaArray<uint8_t>;
