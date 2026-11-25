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

#ifndef OMNISTREAM_XXH128_HASHSERIALIZER_H
#define OMNISTREAM_XXH128_HASHSERIALIZER_H

#include <xxhash.h>

#include "TypeSerializerSingleton.h"

#include "core/typeinfo/typeconstants.h"

class XxH128_hashSerializer : public TypeSerializerSingleton {
public:
    XxH128_hashSerializer()
    {
    }

    ~XxH128_hashSerializer() override = default;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    const char* getName() const override
    {
        return "XxH128_hashSerializer";
    }

    virtual TypeSerializer* duplicate()
    {
        return XxH128_hashSerializer::INSTANCE;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::XXHASH128_BK;
    };

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::POJO, TYPE_NAME_XXH128_HASH_CLASS};
        return typeJson.toJson();
    }

    void setSubBufferReusable(bool bufferReusable_) override
    {
    }

    static XxH128_hashSerializer* INSTANCE;
};

#endif // OMNISTREAM_XXH128_HASHSERIALIZER_H
