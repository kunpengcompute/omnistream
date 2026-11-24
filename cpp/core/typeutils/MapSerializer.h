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

#ifndef FLINK_TNEL_MAPSERIALIZER_H
#define FLINK_TNEL_MAPSERIALIZER_H

#include <memory>

#include "TypeSerializerSingleton.h"
#include "core/type/MapValue.h"

class MapSerializer : public TypeSerializerSingleton {
public:
    MapSerializer(TypeSerializer* keySerializer, TypeSerializer* valueSerializer);

    ~MapSerializer() override
    {
        if (keySerializer != nullptr) {
            delete keySerializer;
        }
        if (valueSerializer != nullptr) {
            delete valueSerializer;
        }
    }
    void* deserialize(DataInputView& source) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void serialize(void* record, DataOutputSerializer& target) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void serialize(Object* buffer, DataOutputSerializer& target) override;
    void deserialize(Object* buffer, DataInputView& source) override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    };
    const char* getName() const override
    {
        return "MapSerializer";
    };

    TypeSerializer* getKeySerializer() const
    {
        return keySerializer;
    }
    TypeSerializer* getValueSerializer() const
    {
        return valueSerializer;
    }

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;
    virtual TypeSerializer* duplicate()
    {
        return new MapSerializer(keySerializer->duplicate(), valueSerializer->duplicate());
    };
    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    {
        // to-Do
        return nullptr;
    };

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::MAP, "", keySerializer, valueSerializer};
        return typeJson.toJson();
    }

private:
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;
};

#endif // FLINK_TNEL_MAPSERIALIZER_H
