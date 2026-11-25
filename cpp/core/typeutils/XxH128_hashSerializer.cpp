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

#include "XxH128_hashSerializer.h"

void* XxH128_hashSerializer::deserialize(DataInputView& source)
{
    int64_t low64 = source.readLong();
    int64_t high64 = source.readLong();

    auto* res = new XXH128_hash_t();
    res->low64 = static_cast<uint64_t>(low64);
    res->high64 = static_cast<uint64_t>(high64);

    return static_cast<void*>(res);
}

void XxH128_hashSerializer::serialize(void* record, DataOutputSerializer& target)
{
    auto* obj = static_cast<XXH128_hash_t*>(record);
    int64_t low64 = static_cast<int64_t>(obj->low64);
    int64_t high64 = static_cast<int64_t>(obj->high64);

    target.writeLong(low64);
    target.writeLong(high64);
}

XxH128_hashSerializer* XxH128_hashSerializer::INSTANCE = new XxH128_hashSerializer();
