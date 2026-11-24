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

#ifndef TYPESERIALIZER_NEW_H
#define TYPESERIALIZER_NEW_H

template <typename T>
class TypeSerializer {
public:
    TypeSerializer() = default;
    virtual bool isImmutableType() = 0;
    virtual TypeSerializer<T> duplicate() = 0;
    virtual T createInstance() = 0;
    virtual T copy(T* from) = 0;
    virtual T copy(T* from, T* reuse) = 0;
};

#endif
