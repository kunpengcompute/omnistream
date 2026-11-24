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

#ifndef FLINK_TNEL_VOIDNAMESPACE_H
#define FLINK_TNEL_VOIDNAMESPACE_H

#include "basictypes/Object.h"

class VoidNamespace : public Object {
public:
    VoidNamespace() = default;

    VoidNamespace(const VoidNamespace& other) = default;

    ~VoidNamespace() override = default;

    VoidNamespace& operator=(const VoidNamespace& other)
    {
        return *this;
    }

    bool operator==(const VoidNamespace& other) const
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
struct hash<VoidNamespace> {
    std::size_t operator()(const VoidNamespace& ns) const noexcept
    {
        return 99;
    }
};

template <>
struct equal_to<VoidNamespace> {
    bool operator()(const VoidNamespace& lhs, const VoidNamespace& rhs) const noexcept
    {
        return true;
    }
};
} // namespace std

#endif // FLINK_TNEL_VOIDNAMESPACE_H
