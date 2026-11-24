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

#ifndef OMNISTREAM_EXTERNALSERIALIZER_H
#define OMNISTREAM_EXTERNALSERIALIZER_H

#include "TypeSerializerSingleton.h"

class ExternalSerializer : public TypeSerializerSingleton {
public:
    ExternalSerializer(LogicalType* dateType, TypeSerializer* internalSerializer, bool isInternalInput);
    ~ExternalSerializer() override
    {
        if (internalSerializer != nullptr) {
            delete internalSerializer;
            internalSerializer = nullptr;
        }
    }
    static ExternalSerializer* of(LogicalType* dateType);
    LogicalType* getDataType();
    void* deserialize(DataInputView& source) override;
    void serialize(void* record, DataOutputSerializer& target) override;
    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;
    BackendDataType getBackendId() const override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::EXTERNAL};
        typeJson.logicalType = dateType;
        typeJson.valueSerializer = internalSerializer;

        SerializerAttributes serializerAttributes;
        serializerAttributes.externalIsInternalInput = isInternalInput;
        typeJson.serializerAttributes = &serializerAttributes;

        return typeJson.toJson();
    }

private:
    bool isInternalInput;
    LogicalType* dateType;
    TypeSerializer* internalSerializer;
};

#endif // OMNISTREAM_EXTERNALSERIALIZER_H
