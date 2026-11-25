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

#ifndef OMNISTREAM_LISTSERIALIZER_H
#define OMNISTREAM_LISTSERIALIZER_H
#include "TypeSerializerSingleton.h"
#include "SerializerJsonInfo.h"
#include "typeutils/RowDataSerializer.h"
#include "typeutils/TypeSerializer.h"

class ListSerializer : public TypeSerializerSingleton {
public:
    explicit ListSerializer(TypeSerializer* elementSerializer, bool ownsElementSerializer = true);

    ~ListSerializer() override
    {
        if (ownsElementSerializer && elementSerializer != nullptr) {
            delete elementSerializer;
            elementSerializer = nullptr;
        }
    }

    void* deserialize(DataInputView& source) override;
    void serialize(void* record, DataOutputSerializer& target) override;
    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;
    BackendDataType getBackendId() const override
    {
        // TODO:
        // 当前无法通过getBackendId区分elementSerializer是否为RowDataSerializer还是BinaryRowDataSerializer
        // ，因此采用dynamic_cast判断elementSerializer类型，
        // 待BinaryRowDataSerializer使用单独的BackendId后可以修改为使用getBackendId判断elementSerializer类型
        return dynamic_cast<RowDataSerializer*>(elementSerializer) == nullptr ? BackendDataType::OBJECT_BK
                                                                              : BackendDataType::ROW_LIST_BK;
    }

    TypeSerializer* getElementSerializer() const
    {
        return elementSerializer;
    }

    void setElementSerializer(TypeSerializer* newElementSerializer)
    {
        elementSerializer = newElementSerializer;
    }

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::LIST, "", nullptr, elementSerializer};
        return typeJson.toJson();
    }

private:
    TypeSerializer* elementSerializer = nullptr;
    bool ownsElementSerializer;
};

#endif // OMNISTREAM_LISTSERIALIZER_H
