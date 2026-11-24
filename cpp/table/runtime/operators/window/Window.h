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

#include <cstdint>

class Window {
public:
    Window() = default;

    virtual ~Window() = default;

    virtual int64_t maxTimestamp() const = 0;

protected:
    // Copy constructor and assignment operator are protected to prevent object slicing
    Window(const Window&) = default;
    Window& operator=(const Window&) = default;
    Window(Window&&) = default;
    Window& operator=(Window&&) = default;
};
