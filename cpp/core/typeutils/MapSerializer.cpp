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

#include "MapSerializer.h"
#include "basictypes/java_util_HashMap.h"

MapSerializer::MapSerializer(TypeSerializer* keySerializer, TypeSerializer* valueSerializer)
    : keySerializer(keySerializer),
      valueSerializer(valueSerializer)
{
    reuseBuffer = new HashMap();
    setSubBufferReusable(false);
}

void MapSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    auto map = static_cast<HashMap*>(buffer);
    int size = map->size();
    target.writeInt(size);

    for (const auto& it : *map->map_) {
        auto key = it.first;
        auto value = it.second;
        keySerializer->serialize(key, target);
        if (value == nullptr) {
            target.writeBoolean(true);
        } else {
            target.writeBoolean(false);
            valueSerializer->serialize(value, target);
        }
    }
}

void MapSerializer::deserialize(Object* buffer, DataInputView& source)
{
    auto size = source.readInt();
    auto map = static_cast<HashMap*>(buffer);
    for (int i = 0; i < size; ++i) {
        auto keyBuffer = keySerializer->GetBuffer();
        if (keyBuffer == nullptr) {
            continue;
        }
        keySerializer->deserialize(keyBuffer, source);
        auto isNull = source.readBoolean();
        Object* valueBuffer = nullptr;
        if (likely(!isNull)) {
            valueBuffer = valueSerializer->GetBuffer();
            valueSerializer->deserialize(valueBuffer, source);
        }
        map->put(keyBuffer, valueBuffer);
        keyBuffer->putRefCount();
        if (likely(!isNull) && valueBuffer != nullptr) {
            valueBuffer->putRefCount();
        }
    }
}

void MapSerializer::setSubBufferReusable(bool bufferReusable_)
{
    keySerializer->setSelfBufferReusable(bufferReusable_);
    valueSerializer->setSelfBufferReusable(bufferReusable_);
}

Object* MapSerializer::GetBuffer()
{
    if (bufferReusable) {
        static_cast<HashMap*>(reuseBuffer)->clear();
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new HashMap();
}
