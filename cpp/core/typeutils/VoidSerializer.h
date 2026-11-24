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

#ifndef VOIDSERIALIZER_H
#define VOIDSERIALIZER_H

#include "TypeSerializerSingleton.h"
#include "SerializerJsonInfo.h"
#include "basictypes/Void.h"

class VoidSerializer : public TypeSerializerSingleton {
public:
    VoidSerializer()
    {
        reuseBuffer = new Void();
    }

    ~VoidSerializer() override = default;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override
    {
        target.write(0);
    }

    void deserialize(Object* buffer, DataInputView& source) override
    {
    }

    void serialize(Object* buffer, DataOutputSerializer& target) override
    {
        target.write(0);
    }

    const char* getName() const override
    {
        return "VoidSerializer";
    }

    virtual TypeSerializer* duplicate()
    {
        return VoidSerializer::INSTANCE;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::VOID};
        return typeJson.toJson();
    }

    Object* GetBuffer() override;

    static VoidSerializer* INSTANCE;
};

#endif // VOIDSERIALIZER_H
