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

#ifndef OMNISTREAM_VECTORBATCHSERIALIZER_H
#define OMNISTREAM_VECTORBATCHSERIALIZER_H

#include "core/typeutils/TypeSerializerSingleton.h"
#include "core/typeinfo/typeconstants.h"

class VectorBatchSerializer : public TypeSerializerSingleton {
public:
    VectorBatchSerializer() = default;
    ~VectorBatchSerializer() override = default;

    void* deserialize(DataInputView& source) override
    {
    }

    void serialize(void* record, DataOutputSerializer& target) override
    {
    }

    const char* getName() const override
    {
        return "VectorBatchSerializer";
    }

    virtual TypeSerializer* duplicate()
    {
        return &getInstance();
    }

    static VectorBatchSerializer& getInstance()
    {
        static VectorBatchSerializer instance;
        return instance;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VECTOR_BATCH_BK;
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::POJO, TYPE_NAME_VECTOR_BATCH_CLASS};
        return typeJson.toJson();
    }

    void setSubBufferReusable(bool bufferReusable_) override
    {
    }
};

#endif // OMNISTREAM_VECTORBATCHSERIALIZER_H
