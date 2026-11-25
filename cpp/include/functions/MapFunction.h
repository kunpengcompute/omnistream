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

#ifndef OMNIFLINK_MAPFUNCTION_H
#define OMNIFLINK_MAPFUNCTION_H

#include <memory>
#include "basictypes/Object.h"
#include "Function.h"

template <typename T>
class MapFunction : public omnistream::Function {
public:
    virtual ~MapFunction() = default;
    virtual T* map(T* obj) = 0;
};

template <typename T>
using MapFunctionUnique = std::unique_ptr<MapFunction<T>>;

#endif // OMNIFLINK_MAPFUNCTION_H
