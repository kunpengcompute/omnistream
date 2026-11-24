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

#ifndef OMNISTREAM_SORTEDVECTORLONG_H
#define OMNISTREAM_SORTEDVECTORLONG_H

#include "core/typeutils/TypeSerializerSingleton.h"
#include "core/typeinfo/typeconstants.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"

using namespace omniruntime::type;

class SortedVectorLong : public TypeSerializerSingleton {
public:
    SortedVectorLong()
    {
    }

    ~SortedVectorLong() override = default;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    const char* getName() const override
    {
        return "SortedVectorLong";
    }

    virtual TypeSerializer* duplicate()
    {
        return SortedVectorLong::INSTANCE;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::SET_LONG;
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::POJO, TYPE_NAME_SORTED_VECTOR_LONG_CLASS};
        // Describe the Java POJO field explicitly instead of relying on generic type inference.
        ListSerializer listSerializer(new LongSerializer());
        typeJson.fieldNames = {"list"};
        typeJson.fieldSerializers = {&listSerializer};
        return typeJson.toJson();
    }

    void setSubBufferReusable(bool bufferReusable_) override
    {
    }

    static SortedVectorLong* INSTANCE;
};
#endif // OMNISTREAM_SORTEDVECTORLONG_H
