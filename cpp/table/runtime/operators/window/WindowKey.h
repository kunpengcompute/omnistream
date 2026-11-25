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

#ifndef FLINK_TNEL_WINDOWKEY_H
#define FLINK_TNEL_WINDOWKEY_H

#include <iostream>
#include <functional>
#include <memory>
#include <utility>
#include "table/data/binary/BinaryRowData.h"

class WindowKey {
public:
    WindowKey(long window, std::shared_ptr<RowData> key) : window(window), key(std::move(key))
    {
    }
    WindowKey replace(long window, std::shared_ptr<RowData> key);
    long getWindow() const;
    std::shared_ptr<RowData> getKey() const;
    long hash() const;
    // 重载相等比较运算符
    bool operator==(const WindowKey& other) const
    {
        return window == other.window && *key == *(other.getKey());
    }

private:
    long window;
    std::shared_ptr<RowData> key;
};

namespace std {
template <>
struct hash<WindowKey> {
    std::size_t operator()(const WindowKey& windowKey) const noexcept
    {
        return windowKey.hash();
    }
};
} // namespace std

#endif
