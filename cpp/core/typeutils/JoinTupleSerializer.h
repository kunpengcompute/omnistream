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

#ifndef OMNISTREAM_JOINTUPLESERIALIZER_H
#define OMNISTREAM_JOINTUPLESERIALIZER_H

#include "core/typeinfo/typeconstants.h"

#include "TypeSerializerSingleton.h"

class JoinTupleSerializer : public TypeSerializerSingleton {
public:
    JoinTupleSerializer()
    {
    }

    ~JoinTupleSerializer() override = default;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    const char* getName() const override
    {
        return "JoinTupleSerializer";
    }

    virtual TypeSerializer* duplicate()
    {
        return JoinTupleSerializer::INSTANCE;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::TUPLE_INT32_INT32;
    };

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::POJO, TYPE_NAME_JOIN_TUPLE_CLASS};
        return typeJson.toJson();
    }

    void setSubBufferReusable(bool bufferReusable_) override
    {
    }

    static JoinTupleSerializer* INSTANCE;
};

#endif // OMNISTREAM_JOINTUPLESERIALIZER_H
