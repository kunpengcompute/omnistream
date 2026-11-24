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

#ifndef FLINK_TNEL_MAPSTATEDESCRIPTOR_H
#define FLINK_TNEL_MAPSTATEDESCRIPTOR_H
#include <string>
#include "core/typeutils/TypeSerializer.h"
#include "core/typeinfo/TypeInformation.h"
#include "StateDescriptor.h"
#include "MapState.h"
#include "core/typeinfo/MapTypeInfo.h"
#include <atomic>
#include <map>

/**
 * UK: The type of the keys that can be added to the map state.
 * */
template <typename UK, typename UV>
class MapStateDescriptor : public StateDescriptor {
public:
    MapStateDescriptor(std::string name, TypeSerializer* keySerializer, TypeSerializer* valueSerializer)
        : StateDescriptor(name, new MapSerializer(keySerializer, valueSerializer)),
          keyDataId(keySerializer->getBackendId()),
          valueDataId(valueSerializer->getBackendId()),
          userKeySerializer(keySerializer),
          valueSerializer(valueSerializer) {};

    MapStateDescriptor(std::string name, TypeInformation* keyTypeInfo, TypeInformation* valueTypeInfo)
        : StateDescriptor(name, new MapTypeInfo(keyTypeInfo, valueTypeInfo)),
          keyDataId(keyTypeInfo->getBackendId()),
          valueDataId(valueTypeInfo->getBackendId())
    {
        this->userKeySerializer = keyTypeInfo->createTypeSerializer();
        this->valueSerializer = valueTypeInfo->createTypeSerializer();
        stateSerializer = new MapSerializer(userKeySerializer, valueSerializer);
    }

    MapStateDescriptor(std::string name, TypeSerializer* serializer)
        : StateDescriptor(name, serializer),
          keyDataId(serializer->getBackendId()),
          valueDataId(serializer->getBackendId()) {};

    ~MapStateDescriptor() override
    {
        delete userKeySerializer;
        if (valueSerializer != userKeySerializer) {
            delete valueSerializer;
        }
        userKeySerializer = nullptr;
        valueSerializer = nullptr;
    }

    TypeSerializer* GetUserKeySerializer()
    {
        return this->userKeySerializer;
    }

    TypeSerializer* GetValueSerializer()
    {
        return this->valueSerializer;
    }

    Type getType() override
    {
        return StateDescriptor::Type::MAP;
    };
    void setKeyValueBackendTypeId(BackendDataType keyType, BackendDataType valueType)
    {
        keyDataId = keyType;
        valueDataId = valueType;
    }
    BackendDataType getBackendId() override
    {
        throw std::runtime_error("No dataId for a MapStateDescriptor");
    }
    BackendDataType getKeyDataId() override
    {
        return keyDataId;
    }
    BackendDataType getValueDataId() override
    {
        return valueDataId;
    }

protected:
    BackendDataType keyDataId = BackendDataType::INVALID_BK;
    BackendDataType valueDataId = BackendDataType::INVALID_BK;
    TypeSerializer* userKeySerializer;
    TypeSerializer* valueSerializer;
};

using DataStreamMapStateDescriptor = MapStateDescriptor<Object*, Object*>;

#endif // FLINK_TNEL_STATEDESCRIPTOR_H
