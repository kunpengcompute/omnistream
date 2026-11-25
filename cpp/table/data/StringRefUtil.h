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

#ifndef OMNISTREAM_STRINGREFUTIL_H
#define OMNISTREAM_STRINGREFUTIL_H
#include "OmniOperatorJIT/core/src/type/string_ref.h"
namespace std {
template <>
struct hash<omniruntime::type::StringRef> {
    size_t operator()(const omniruntime::type::StringRef& stringRef) const
    {
        uint32_t hash = 42;
        for (size_t i = 0; i < stringRef.size; ++i) {
            hash ^= stringRef.data[i];
            hash *= 16777619u;
        }
        return hash;
    }
};

template <>
struct equal_to<omniruntime::type::StringRef> {
    bool operator()(const omniruntime::type::StringRef& lhs, const omniruntime::type::StringRef& rhs) const
    {
        return lhs == rhs;
    }
};
} // namespace std
#endif // OMNISTREAM_STRINGREFUTIL_H
