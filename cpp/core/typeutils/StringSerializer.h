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

#ifndef FLINK_TNEL_STRINGSERIALIZER_H
#define FLINK_TNEL_STRINGSERIALIZER_H

#include <memory>

#include "TypeSerializerSingleton.h"

class StringSerializer : public TypeSerializerSingleton {
public:
    StringSerializer()
    {
        reuseBuffer = new String();
    };
    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    void deserialize(Object* buffer, DataInputView& source) override;

    void serialize(Object* buffer, DataOutputSerializer& target) override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VARCHAR_BK;
    }

    static StringSerializer* INSTANCE;

    class StringSerializerCleaner {
    public:
        ~StringSerializerCleaner()
        {
            if (INSTANCE) {
                delete INSTANCE;
                INSTANCE = nullptr;
            }
        }
    };

    const char* getName() const override
    {
        return "StringSerializer";
    }

    static StringSerializerCleaner cleaner;

    Object* GetBuffer() override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::STRING_T};
        return typeJson.toJson();
    }

private:
    StringSerializer* reuseSerializer = nullptr;
};
#endif
