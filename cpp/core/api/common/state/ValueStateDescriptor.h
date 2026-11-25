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

#ifndef FLINK_TNEL_VALUESTATEDESCRIPTOR_H
#define FLINK_TNEL_VALUESTATEDESCRIPTOR_H
#include "StateDescriptor.h"
#include "ValueState.h"

/**
 * T: The type of the values that the value state can hold, such as Object*
 * */
template <typename T>
class ValueStateDescriptor : public StateDescriptor {
public:
    ValueStateDescriptor() = default;

    ValueStateDescriptor(const std::string& name, TypeSerializer* serializer) : StateDescriptor(name, serializer)
    {
        if (serializer != nullptr) {
            backendDataId = serializer->getBackendId();
        }
    };
    Type getType() override
    {
        return StateDescriptor::Type::VALUE;
    };

    ValueStateDescriptor(const std::string& name, TypeInformation* typeInfo) : StateDescriptor(name, typeInfo)
    {
        if (typeInfo != nullptr) {
            backendDataId = typeInfo->getBackendId();
        }
    };

    ValueStateDescriptor(const std::string& name, Class* cl) : StateDescriptor(name, cl)
    {
        TypeInformation* typeInfo = getTypeInfo();
        if (typeInfo != nullptr) {
            backendDataId = typeInfo->getBackendId();
        }
    };

    ~ValueStateDescriptor() override = default;

    BackendDataType getBackendId() override
    {
        return backendDataId;
    }
    BackendDataType getKeyDataId() override
    {
        throw std::runtime_error("No keyDataId for a ValueStateDescriptor");
    }
    BackendDataType getValueDataId() override
    {
        throw std::runtime_error("No valueDataId for a ValueStateDescriptor");
    }

protected:
    BackendDataType backendDataId = BackendDataType::INVALID_BK;
};

using DataStreamValueStateDescriptor = ValueStateDescriptor<Object*>;

#endif // FLINK_TNEL_VALUESTATEDESCRIPTOR_H
