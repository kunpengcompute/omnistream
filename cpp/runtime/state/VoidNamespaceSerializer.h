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

#ifndef FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
#define FLINK_TNEL_VOIDNAMESPACESERIALIZER_H

#include "core/typeutils/TypeSerializerSingleton.h"
#include "VoidNamespace.h"

class VoidNamespaceSerializer : public TypeSerializerSingleton {
public:
    VoidNamespaceSerializer()
    {
        reuseBuffer = new VoidNamespace();
    }

    ~VoidNamespaceSerializer() override = default;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override
    {
        target.write(0);
    }

    void deserialize(Object* buffer, DataInputView& source) override
    {
        source.readByte();
    }

    void serialize(Object* buffer, DataOutputSerializer& target) override
    {
        target.write(0);
    }

    const char* getName() const override
    {
        return "VoidNamespaceSerializer";
    }

    virtual TypeSerializer* duplicate()
    {
        return VoidNamespaceSerializer::INSTANCE;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VOID_NAMESPACE_BK;
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::VOID_NAMESPACE};
        return typeJson.toJson();
    }

    Object* GetBuffer() override;

    static VoidNamespaceSerializer* INSTANCE;
};

#endif // FLINK_TNEL_VOIDNAMESPACESERIALIZER_H
