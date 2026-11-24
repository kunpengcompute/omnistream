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

#ifndef OMNISTREAM_VALUESTATEDESCRIPTORWRAPPER_H
#define OMNISTREAM_VALUESTATEDESCRIPTORWRAPPER_H
#include "StateDescriptor.h"
#include "ValueStateDescriptor.h"

class ValueStateDescriptorWrapper : public StateDescriptor, public Object {
public:
    ValueStateDescriptorWrapper(String* nameStr, Class* cl)
    {
        valueStateDescriptor = new ValueStateDescriptor<Object*>(nameStr->toString(), cl);
        auto serializer = valueStateDescriptor->getTypeInfo()->createTypeSerializer();
        // reuse buffer is not allowed here
        serializer->setSelfBufferReusable(false);
        valueStateDescriptor->SetStateSerializer(serializer);
    }

    // can not delete valueStateDescriptor, because it will be deleted when RocksdbKeyedStateBackend delete.
    ~ValueStateDescriptorWrapper() override = default;

    int hashCode() override
    {
        NOT_IMPL_EXCEPTION;
    }

    bool equals(Object* obj) override
    {
        NOT_IMPL_EXCEPTION;
    }

    std::string toString() override
    {
        NOT_IMPL_EXCEPTION;
    }

    Object* clone() override
    {
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() override
    {
        return valueStateDescriptor->getBackendId();
    }

    BackendDataType getValueDataId() override
    {
        return valueStateDescriptor->getValueDataId();
    }

    BackendDataType getKeyDataId() override
    {
        return valueStateDescriptor->getKeyDataId();
    }

    StateDescriptor::Type getType() override
    {
        return valueStateDescriptor->getType();
    }

    ValueStateDescriptor<Object*>* getValueStateDescriptor()
    {
        return valueStateDescriptor;
    }

private:
    ValueStateDescriptor<Object*>* valueStateDescriptor;
};
#endif // OMNISTREAM_VALUESTATEDESCRIPTORWRAPPER_H
