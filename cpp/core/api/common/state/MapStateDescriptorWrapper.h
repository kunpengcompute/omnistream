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

#ifndef OMNISTREAM_MAPSTATEDESCRIPTORWRAPPER_H
#define OMNISTREAM_MAPSTATEDESCRIPTORWRAPPER_H
#include "StateDescriptor.h"
#include "MapStateDescriptor.h"

class MapStateDescriptorWrapper : public StateDescriptor, public Object {
public:
    MapStateDescriptorWrapper(String* name, TypeInformation* keyTypeInfo, TypeInformation* valueTypeInfo)
    {
        mapStateDescriptor = new MapStateDescriptor<Object*, Object*>(name->toString(), keyTypeInfo, valueTypeInfo);
        // reuse buffer is not allowed here
        mapStateDescriptor->getStateSerializer()->setSelfBufferReusable(false);
    }

    // can not delete mapStateDescriptor, because it will be deleted when RocksdbKeyedStateBackend delete.
    ~MapStateDescriptorWrapper() override = default;

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
        return mapStateDescriptor->getBackendId();
    }

    BackendDataType getValueDataId() override
    {
        return mapStateDescriptor->getValueDataId();
    }

    BackendDataType getKeyDataId() override
    {
        return mapStateDescriptor->getKeyDataId();
    }

    StateDescriptor::Type getType() override
    {
        return mapStateDescriptor->getType();
    }

    MapStateDescriptor<Object*, Object*>* getMapStateDescriptor()
    {
        return mapStateDescriptor;
    }

private:
    MapStateDescriptor<Object*, Object*>* mapStateDescriptor;
};

#endif // OMNISTREAM_MAPSTATEDESCRIPTORWRAPPER_H
