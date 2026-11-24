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

#include "LongSerializer.h"
#include "basictypes/Long.h"

void* LongSerializer::deserialize(DataInputView& source)
{
    return reinterpret_cast<void*>(new long(source.readLong()));
}

void LongSerializer::serialize(void* record, DataOutputSerializer& target)
{
    target.writeLong(*(long*)record);
}

void LongSerializer::deserialize(Object* buffer, DataInputView& source)
{
    LOG("LongSerializer::deserialize change start ---");
    int64_t value = source.readLong();
    reinterpret_cast<Long*>(buffer)->setValue(value);
    LOG("LongSerializer::deserialize change end ---");
}

void LongSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    LOG("LongSerializer::serialize change start +++ value : ");
    const int64_t value = reinterpret_cast<Long*>(buffer)->getValue();
    target.writeLong(value);
    LOG("LongSerializer::serialize change end +++");
}

LongSerializer::LongSerializer()
{
    reuseBuffer = new Long();
}

LongSerializer* LongSerializer::INSTANCE = new LongSerializer();
LongSerializer::LongSerializerCleaner LongSerializer::cleaner;

void* IntSerializer::deserialize(DataInputView& source)
{
    return reinterpret_cast<void*>(new int(source.readInt()));
}

void IntSerializer::serialize(void* record, DataOutputSerializer& target)
{
    target.writeInt(*(int*)record);
}

void IntSerializer::deserialize(Object* buffer, DataInputView& source)
{
    NOT_IMPL_EXCEPTION;
}

void IntSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    NOT_IMPL_EXCEPTION;
}

IntSerializer::IntSerializer()
{
    reuseBuffer = nullptr;
}

IntSerializer* IntSerializer::INSTANCE = new IntSerializer();

IntSerializer::IntSerializerCleaner IntSerializer::cleaner;

Object* LongSerializer::GetBuffer()
{
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new Long();
}
