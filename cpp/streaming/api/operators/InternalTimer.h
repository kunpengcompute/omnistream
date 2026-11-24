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

#pragma once
/**
 * K: such as Object*
 * N: such as VoidNamespace*
 * */
#include <cstdint>
#include <iostream>

template <typename K, typename N>
class InternalTimer {
public:
    virtual ~InternalTimer() = default;
    virtual int64_t getTimestamp() const = 0;
    virtual K getKey() const = 0;
    virtual N getNamespace() const = 0;
};
