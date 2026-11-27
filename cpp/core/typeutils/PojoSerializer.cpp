/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "PojoSerializer.h"

void PojoSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    if (classObj == nullptr) {
        classObj = ClassRegistry::instance().getClass(clazz);
    }
    int8_t flags = 0;
    if (buffer == nullptr) {
        flags |= IS_NULL;
        target.writeByte(flags);
        return;
    }

    int8_t subclassTag = -1;
    TypeSerializer* subclassSerializer = nullptr;
    auto actualClass = (std::string)(classObj->getName());
    if (clazz != actualClass) {
        subclassTag = registeredClasses[actualClass];
        if (subclassTag != 0) {
            flags |= IS_TAGGED_SUBCLASS;
            subclassSerializer = registeredSerializers[subclassTag];
        } else {
            flags |= IS_SUBCLASS;
            subclassSerializer = getSubclassSerializer(actualClass);
        }
    } else {
        flags |= NO_SUBCLASS;
    }

    target.writeByte(flags);

    // if its a registered subclass, write the class tag id, otherwise write the full classname
    if ((flags & IS_SUBCLASS) != 0) {
        target.writeUTF(actualClass);
    } else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
        target.writeByte(subclassTag);
    }

    // if its a subclass, use the corresponding subclass serializer,
    // otherwise serialize each field with our field serializers
    if ((flags & NO_SUBCLASS) != 0) {
        for (int i = 0; i < numFields; i++) {
            auto o = classObj->get(buffer, fields[i]);
            if (o == nullptr) {
                target.writeBoolean(true); // null field handling
            } else {
                target.writeBoolean(false);
                fieldSerializers[i]->serialize(o, target);
                o->putRefCount();
            }
        }
    } else {
        // subclass
        if (subclassSerializer != nullptr) {
            subclassSerializer->serialize(buffer, target);
        }
    }
}

TypeSerializer* PojoSerializer::getSubclassSerializer(const std::string& subclass)
{
    if (subclassSerializerCache.find(subclass) != subclassSerializerCache.end()) {
        return subclassSerializerCache[subclass];
    }
    auto result = createSubclassSerializer(subclass);
    subclassSerializerCache[subclass] = result;
    return result;
}

TypeSerializer* PojoSerializer::createSubclassSerializer(const std::string& subclass)
{
    // todo
    return nullptr;
}

void PojoSerializer::deserialize(Object* buffer, DataInputView& source)
{
    uint8_t flags = source.readByte();
    if ((flags & IS_NULL) != 0) {
        return;
    }

    TypeSerializer* subclassSerializer = getSubclassSerializer(flags, source);

    if ((flags & NO_SUBCLASS) != 0) {
        deserializeFields(buffer, source);
    } else if (subclassSerializer != nullptr) {
        subclassSerializer->deserialize(buffer, source);
    }
}

TypeSerializer* PojoSerializer::getSubclassSerializer(uint8_t flags, DataInputView& source)
{
    if ((flags & IS_SUBCLASS) != 0) {
        std::string subclassName = source.readUTF();
        return getSubclassSerializer(subclassName);
    }
    if ((flags & IS_TAGGED_SUBCLASS) != 0) {
        int subclassTag = source.readByte();
        return registeredSerializers[subclassTag];
    }
    return nullptr;
}

void PojoSerializer::deserializeFields(Object* buffer, DataInputView& source)
{
    for (int i = 0; i < numFields; i++) {
        bool isNull = source.readBoolean();
        Object* fieldBuffer = nullptr;

        if (fields[i] != "") {
            if (isNull) {
                classObj->set(buffer, fields[i], nullptr);
                continue;
            }
            fieldBuffer = fieldSerializers[i]->GetBuffer();
            fieldSerializers[i]->deserialize(fieldBuffer, source);
            classObj->set(buffer, fields[i], fieldBuffer);
        } else if (!isNull) {
            fieldBuffer = fieldSerializers[i]->GetBuffer();
            fieldSerializers[i]->deserialize(fieldBuffer, source);
        }

        if (fieldBuffer != nullptr) {
            fieldBuffer->putRefCount();
        }
    }
}

void PojoSerializer::setSubBufferReusable(bool bufferReusable_)
{
    for (auto& fieldSerializer : fieldSerializers) {
        fieldSerializer->setSelfBufferReusable(bufferReusable_);
    }
}

Object* PojoSerializer::GetBuffer()
{
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return classObj->newInstance();
}
