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

#ifndef OMNISTREAM_VOID_H
#define OMNISTREAM_VOID_H

#include "Object.h"

class Void : public Object {
public:
    Void() = default;

    Void(const Void& other) = default;

    ~Void() override = default;

    Void& operator=(const Void& other)
    {
        return *this;
    }

    bool operator==(const Void& other) const
    {
        return true;
    }

    operator size_t() const
    {
        return 0;
    }

    operator int64_t() const
    {
        return 0;
    }

    int hashCode()
    {
        return 99;
    }
};

namespace std {
template <>
struct hash<Void> {
    std::size_t operator()(const Void& ns) const noexcept
    {
        return 99;
    }
};

template <>
struct equal_to<Void> {
    bool operator()(const Void& lhs, const Void& rhs) const noexcept
    {
        return true;
    }
};
} // namespace std

#endif // OMNISTREAM_VOID_H
