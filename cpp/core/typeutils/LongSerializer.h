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

#ifndef FLINK_TNEL_LONGSERIALIZER_H
#define FLINK_TNEL_LONGSERIALIZER_H

#include "TypeSerializerSingleton.h"
#include "SerializerJsonInfo.h"

class LongSerializer : public TypeSerializerSingleton {
public:
    LongSerializer();

    void* deserialize(DataInputView& source) override;
    void serialize(void* record, DataOutputSerializer& target) override;

    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;

    static LongSerializer* INSTANCE;

    class LongSerializerCleaner {
    public:
        ~LongSerializerCleaner()
        {
            if (INSTANCE) {
                delete INSTANCE;
                INSTANCE = nullptr;
            }
        }
    };

    const char* getName() const override
    {
        return "LongSerializer";
    }

    // to delete static instance object
    static LongSerializerCleaner cleaner;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BIGINT_BK;
    };

    Object* GetBuffer() override;
    virtual TypeSerializer* duplicate()
    {
        // Don't duplicate, use existing instance
        return LongSerializer::INSTANCE;
    };
    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    {
        // to-Do
        return nullptr;
    };

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::LONG};
        return typeJson.toJson();
    }
};

class IntSerializer : public TypeSerializerSingleton {
public:
    IntSerializer();

    void* deserialize(DataInputView& source) override;
    void serialize(void* record, DataOutputSerializer& target) override;

    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;

    static IntSerializer* INSTANCE;

    class IntSerializerCleaner {
    public:
        ~IntSerializerCleaner()
        {
            if (INSTANCE) {
                delete INSTANCE;
                INSTANCE = nullptr;
            }
        }
    };

    const char* getName() const override
    {
        return "IntSerializer";
    }

    // to delete static instance object
    static IntSerializerCleaner cleaner;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::INT_BK;
    };

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::INT};
        return typeJson.toJson();
    }
};

#endif
