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

#ifndef OMNISTREAM_POJOSERIALIZER_H
#define OMNISTREAM_POJOSERIALIZER_H

#include <vector>
#include "TypeSerializerSingleton.h"
#include "basictypes/Class.h"
#include "basictypes/ClassRegistry.h"
#include "SerializerJsonInfo.h"

class PojoSerializer : public TypeSerializerSingleton {
public:
    PojoSerializer(std::string clazz, std::vector<TypeSerializer*> fieldSerializers, std::vector<std::string> fields)
        : fieldSerializers(fieldSerializers),
          clazz(clazz),
          fields(fields)
    {
        numFields = fieldSerializers.size();
        if (ClassRegistry::instance().hasRegistry(clazz)) {
            classObj = ClassRegistry::instance().getClass(clazz);
            reuseBuffer = classObj->newInstance();
        }
    }

    ~PojoSerializer() override
    {
        for (auto& serializer : fieldSerializers) {
            delete serializer;
        }
    }

    void* deserialize(DataInputView& source) override
    {
        NOT_IMPL_EXCEPTION;
    }
    void serialize(void* record, DataOutputSerializer& target) override
    {
        NOT_IMPL_EXCEPTION;
    }
    void serialize(Object* buffer, DataOutputSerializer& target) override;
    void deserialize(Object* buffer, DataInputView& source) override;
    const char* getName() const override
    {
        return "PojoSerializer";
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::POJO_BK;
    }

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {
            SerializerType::POJO, clazz, nullptr, nullptr, nullptr, registeredSerializers, fields};
        return typeJson.toJson();
    }

private:
    TypeSerializer* getSubclassSerializer(const std::string& subclass);
    TypeSerializer* getSubclassSerializer(uint8_t flags, DataInputView& source);
    void deserializeFields(Object* buffer, DataInputView& source);
    TypeSerializer* createSubclassSerializer(const std::string& subclass);
    static const int8_t IS_NULL = 1;
    static const int8_t NO_SUBCLASS = 2;
    static const int8_t IS_SUBCLASS = 4;
    static const int8_t IS_TAGGED_SUBCLASS = 8;
    std::vector<TypeSerializer*> fieldSerializers;
    int32_t numFields;
    std::string clazz;
    std::vector<std::string> fields;
    std::map<std::string, int8_t> registeredClasses;
    std::vector<TypeSerializer*> registeredSerializers;
    std::map<std::string, TypeSerializer*> subclassSerializerCache;
    Class* classObj = nullptr;
};
#endif // OMNISTREAM_POJOSERIALIZER_H
