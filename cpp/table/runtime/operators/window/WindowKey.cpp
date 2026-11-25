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

#include "WindowKey.h"

WindowKey WindowKey::replace(long window_, std::shared_ptr<RowData> key_)
{
    this->window = window_;
    this->key = std::move(key_);
    return *this;
}

long WindowKey::getWindow() const
{
    return this->window;
}

std::shared_ptr<RowData> WindowKey::getKey() const
{
    return this->key;
}

long WindowKey::hash() const
{
    size_t h1 = std::hash<long>{}(window);
    size_t h2 = key->hashCode();

    long hashResult = h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    return hashResult;
}
