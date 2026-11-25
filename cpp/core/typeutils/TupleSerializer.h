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

#ifndef OMNISTREAM_TUPLESERIALIZER_H
#define OMNISTREAM_TUPLESERIALIZER_H

#include <nlohmann/json.hpp>
#include "TypeSerializer.h"
#include "basictypes/Tuple2.h"
#include "typeinfo/TypeInformation.h"
#include "typeutils/LongSerializer.h"
#include "typeutils/StringSerializer.h"

class Tuple2Serializer : public TypeSerializerSingleton {
public:
    explicit Tuple2Serializer(const std::vector<TypeInformation*>& types);
    explicit Tuple2Serializer(nlohmann::json& type);
    Tuple2Serializer();

    ~Tuple2Serializer()
    {
        for (int i = 0; i < arity; ++i) {
            delete fieldSerializers[i];
        }
        fieldSerializers.clear();
    }

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    void deserialize(Object* buffer, DataInputView& source) override;

    void serialize(Object* buffer, DataOutputSerializer& target) override;

    const char* getName() const override
    {
        return "TupleSerializer";
    }

    BackendDataType getBackendId() const override;

    void createSerializer(nlohmann::json& type);

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

    // 元数据 round-trip 使用：输出 {type:13, element_type, fieldSerializers:[...]}，
    // 与 OmniAdaptor 的 OmniNativeSerializerJsonInfo / OmniParseFactory 对齐。
    std::string toJson() override;

private:
    std::vector<TypeSerializer*> fieldSerializers;
    uint16_t arity = 0;
};

#endif
