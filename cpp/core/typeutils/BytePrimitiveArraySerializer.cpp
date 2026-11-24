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

#include "BytePrimitiveArraySerializer.h"
#include "basictypes/JavaArray.h"

using ByteArray = JavaArray<uint8_t>;

void BytePrimitiveArraySerializer::deserialize(Object* buffer, DataInputView& source)
{
    auto array = static_cast<ByteArray*>(buffer);
    int size = source.readInt();

    if (size == 0) {
        return;
    }

    array->resize(size);
    source.readFully(array->data(), size, 0, size);
}

void BytePrimitiveArraySerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    auto array = static_cast<ByteArray*>(buffer);
    int size = array->size();
    target.writeInt(size);

    if (size > 0) {
        target.write(array->data(), size, 0, size);
    }
}

void BytePrimitiveArraySerializer::setSubBufferReusable(bool bufferReusable_)
{
    if (elementSerializer != nullptr) {
        elementSerializer->setSelfBufferReusable(bufferReusable_);
    }
}

Object* BytePrimitiveArraySerializer::GetBuffer()
{
    if (bufferReusable) {
        auto array = static_cast<ByteArray*>(reuseBuffer);
        if (array != nullptr) {
            array->resize(0);
        }
        if (reuseBuffer != nullptr) {
            reuseBuffer->getRefCount();
        }
        return reuseBuffer;
    }
    return new ByteArray();
}
