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

#ifndef OMNISTREAM_KEYSELECT_H
#define OMNISTREAM_KEYSELECT_H

#include <memory>
#include "basictypes/Object.h"

/**
 * T: Object
 * */
template <typename T>
class KeySelect {
public:
    virtual T* getKey(T* value) = 0;
};

template <typename T>
using KeySelectUnique = std::unique_ptr<KeySelect<T>>;
#endif // OMNISTREAM_KEYSELECT_H
