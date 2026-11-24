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

#ifndef FLINK_TNEL_UDF_HASH_H
#define FLINK_TNEL_UDF_HASH_H

// stateless function
using HashFunctionType = size_t(Object*);
using CmpFunctionType = bool(Object*, Object*);

struct UdfHash {
    HashFunctionType* hashFunc;
    UdfHash()
    {
    }
    explicit UdfHash(HashFunctionType* func) : hashFunc(func)
    {
    }

    size_t operator()(Object* str) const
    {
        return hashFunc(str);
    }
};

struct UdfCompare {
    CmpFunctionType* cmpFunc;
    UdfCompare()
    {
    }
    explicit UdfCompare(CmpFunctionType* func) : cmpFunc(func)
    {
    }

    bool operator()(Object* const& lhs, Object*& rhs) const
    {
        return cmpFunc(lhs, rhs);
    }

    bool operator()(Object* const& lhs, Object*& rhs)
    {
        return cmpFunc(lhs, rhs);
    }

    bool operator()(Object* const& lhs, Object* const& rhs) const
    {
        return cmpFunc(lhs, rhs);
    }

    bool operator()(Object* const& lhs, Object* const& rhs)
    {
        return cmpFunc(lhs, rhs);
    }
};
#endif
