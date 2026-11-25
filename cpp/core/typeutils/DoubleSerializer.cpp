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

#include "DoubleSerializer.h"

void DoubleSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    auto doubleValue = static_cast<Double*>(buffer);
    target.writeDouble(doubleValue->value);
}

void DoubleSerializer::deserialize(Object* buffer, DataInputView& source)
{
    auto doubleValue = static_cast<Double*>(buffer);
    doubleValue->value = source.readDouble();
}

DoubleSerializer* DoubleSerializer::INSTANCE = new DoubleSerializer();

Object* DoubleSerializer::GetBuffer()
{
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new Double();
}
