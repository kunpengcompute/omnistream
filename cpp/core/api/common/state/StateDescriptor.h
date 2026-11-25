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

#ifndef FLINK_TNEL_STATEDESCRIPTOR_H
#define FLINK_TNEL_STATEDESCRIPTOR_H
#include <string>
#include "core/typeutils/TypeSerializer.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/typeinfo/TypeExtractor.h"
#include "basictypes/Class.h"
#include <atomic>

/**
 * <S> The type of the State objects created from this
 * <T> The type of the value of the state object described by this state descriptor.
 * */
class StateDescriptor {
public:
    StateDescriptor() = default;

    StateDescriptor(std::string name, TypeSerializer* serializer /* , T defaultValue */)
        : name(name),
          stateSerializer(serializer)
    {
    }

    StateDescriptor(std::string name, TypeInformation* typeInfo) : name(name), typeInfo(typeInfo)
    {
    }

    StateDescriptor(std::string name, Class* cl) : name(name)
    {
        typeInfo = TypeExtractor::CreateTypeInfo(cl);
    }

    virtual ~StateDescriptor()
    {
    }

    enum class Type {
        /** @deprecated Enum for migrating from old checkpoints/savepoint versions. */
        UNKNOWN,
        VALUE,
        LIST,
        REDUCING,
        FOLDING,
        AGGREGATING,
        MAP
    };

    static Type StringToType(const std::string& type)
    {
        static std::unordered_map<std::string, Type> stringToType = {
            {"UNKNOWN", Type::UNKNOWN},
            {"VALUE", Type::VALUE},
            {"LIST", Type::LIST},
            {"REDUCING", Type::REDUCING},
            {"FOLDING", Type::FOLDING},
            {"AGGREGATING", Type::AGGREGATING},
            {"MAP", Type::MAP},
            // Numeric encoding (used by computeSnapshot() which stores std::to_string((int)stateType))
            {"0", Type::UNKNOWN},
            {"1", Type::VALUE},
            {"2", Type::LIST},
            {"3", Type::REDUCING},
            {"4", Type::FOLDING},
            {"5", Type::AGGREGATING},
            {"6", Type::MAP}};
        if (stringToType.find(type) != stringToType.end()) {
            return stringToType[type];
        } else {
            return Type::UNKNOWN;
        }
    }
    std::string getName() const
    {
        return name;
    };
    virtual TypeSerializer* getStateSerializer()
    {
        return stateSerializer;
    }
    TypeInformation* getTypeInfo()
    {
        return typeInfo;
    }
    virtual void SetStateSerializer(TypeSerializer* newStateSerializer)
    {
        this->stateSerializer = newStateSerializer;
    }
    virtual Type getType() = 0;

    virtual BackendDataType getKeyDataId() = 0;
    virtual BackendDataType getValueDataId() = 0;
    virtual BackendDataType getBackendId() = 0;

protected:
    std::string name;
    TypeSerializer* stateSerializer = nullptr;
    // T defaultValue;
    StateDescriptor(std::string name, Class type) : name(name)
    {
    }

private:
    // todo: here should be atomic_ref (since c++20). My system is too old and does not have it.
    std::atomic<TypeSerializer*> serializerAtomicReference{};
    TypeInformation* typeInfo = nullptr;
};
#endif // FLINK_TNEL_STATEDESCRIPTOR_H
