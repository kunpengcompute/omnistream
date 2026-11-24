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

#include "JoinTupleSerializer.h"

void* JoinTupleSerializer::deserialize(DataInputView& source)
{
    int32_t f0 = source.readInt();
    int32_t f1 = source.readInt();

    auto* res = new std::tuple<int32_t, int32_t>(f0, f1);

    return static_cast<void*>(res);
}

void JoinTupleSerializer::serialize(void* record, DataOutputSerializer& target)
{
    auto* obj = reinterpret_cast<std::tuple<int32_t, int32_t>*>(record);
    int32_t f0 = std::get<0>(*obj);
    int32_t f1 = std::get<1>(*obj);

    target.writeInt(f0);
    target.writeInt(f1);
}

JoinTupleSerializer* JoinTupleSerializer::INSTANCE = new JoinTupleSerializer();
