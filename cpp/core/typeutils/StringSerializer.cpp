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

#include <string>
#include "../type/StringValue.h"
#include "StringSerializer.h"

void* StringSerializer::deserialize(DataInputView& source)
{
    return StringValue::readString(source);
}

void StringSerializer::serialize(void* record, DataOutputSerializer& target)
{
    StringValue::writeString(static_cast<const std::u32string*>(record), target);
}

void StringSerializer::deserialize(Object* buffer, DataInputView& source)
{
    LOG("StringSerializer::deserialize change start ---");
    StringValue::readString(reinterpret_cast<String*>(buffer), source);
    LOG("StringSerializer::deserialize change end ---");
}

void StringSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    LOG("StringSerializer::serialize change start +++");
    StringValue::writeString(reinterpret_cast<String*>(buffer), target);
    LOG("StringSerializer::serialize change end +++");
}

StringSerializer* StringSerializer::INSTANCE = new StringSerializer();
StringSerializer::StringSerializerCleaner StringSerializer::cleaner;

Object* StringSerializer::GetBuffer()
{
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new String();
}
