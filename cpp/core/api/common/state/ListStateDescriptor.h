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

#ifndef FLINK_TNEL_LISTSTATEDESCRIPTOR_H
#define FLINK_TNEL_LISTSTATEDESCRIPTOR_H
#include "StateDescriptor.h"
#include "ListState.h"
#include "core/typeutils/ListSerializer.h"

/**
 * T: The type of the values that can be added to the list state.
 * */
template <typename T>
class ListStateDescriptor : public StateDescriptor {
public:
    ListStateDescriptor(std::string& name, TypeSerializer* serializer) : StateDescriptor(name, serializer)
    {
        backendDataId = serializer->getBackendId();
        if (serializer->getBackendId() == BackendDataType::ROW_BK) {
            listSerializer_ = new ListSerializer(serializer, /*ownsElementSerializer=*/false);
        }
    };
    ~ListStateDescriptor()
    {
        delete listSerializer_;
    }
    TypeSerializer* getStateSerializer() override
    {
        return listSerializer_ != nullptr ? listSerializer_ : StateDescriptor::getStateSerializer();
    }
    void SetStateSerializer(TypeSerializer* newStateSerializer) override
    {
        StateDescriptor::SetStateSerializer(newStateSerializer);
        if (listSerializer_ != nullptr) {
            listSerializer_->setElementSerializer(newStateSerializer);
        }
    }
    Type getType() override
    {
        return StateDescriptor::Type::LIST;
    };

    ListStateDescriptor(std::string& name, TypeInformation* typeInfo) : StateDescriptor(name, typeInfo)
    {
        backendDataId = typeInfo->getBackendId();
    };

    BackendDataType getBackendId() override
    {
        return backendDataId;
    }
    BackendDataType getKeyDataId() override
    {
        throw std::runtime_error("No keyDataId for a ListStateDescriptor");
    }
    BackendDataType getValueDataId() override
    {
        throw std::runtime_error("No valueDataId for a ListStateDescriptor");
    }

protected:
    BackendDataType backendDataId;
    ListSerializer* listSerializer_ = nullptr;
};

using DataStreamListStateDescriptor = ListStateDescriptor<Object*>;

#endif // FLINK_TNEL_LISTSTATEDESCRIPTOR_H
