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

#ifndef OMNISTREAM_BYTEPRIMITIVEARRAYSERIALIZER_H
#define OMNISTREAM_BYTEPRIMITIVEARRAYSERIALIZER_H
#include "TypeSerializerSingleton.h"
#include "basictypes/JavaArray.h"
#include <vector>

class BytePrimitiveArraySerializer : public TypeSerializerSingleton {
public:
    explicit BytePrimitiveArraySerializer(TypeSerializer* elementSerializer) : elementSerializer(elementSerializer)
    {
        setSubBufferReusable(false);
        reuseBuffer = new JavaArray<uint8_t>();
    }

    ~BytePrimitiveArraySerializer() override
    {
        if (elementSerializer != nullptr) {
            delete elementSerializer;
        }
    }

    void* deserialize(DataInputView& source) override
    {
        int size = source.readInt();
        std::vector<uint8_t>* array = new std::vector<uint8_t>(size);
        source.readFully(array->data(), size, 0, size);
        return static_cast<void*>(array);
    }

    void serialize(void* record, DataOutputSerializer& target) override
    {
        std::vector<uint8_t>* array = static_cast<std::vector<uint8_t>*>(record);
        if (array) {
            target.writeInt(array->size());
            target.write(array->data(), array->size(), 0, array->size());
        } else {
            INFO_RELEASE("savepoint: BytePrimitiveArraySerializer serialize null array");
        }
    }

    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BYTE_ARRAY_BK;
    }

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::BYTE_PRIMITIVE_ARRAY, "", nullptr, elementSerializer};
        return typeJson.toJson();
    }

private:
    TypeSerializer* elementSerializer = nullptr;
};

#endif // OMNISTREAM_BYTEPRIMITIVEARRAYSERIALIZER_H
