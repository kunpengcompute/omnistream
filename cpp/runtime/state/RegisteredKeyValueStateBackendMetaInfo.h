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

#pragma once

#include <string>
#include "RegisteredStateMetaInfoBase.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

class RegisteredKeyValueStateBackendMetaInfo : public RegisteredStateMetaInfoBase {
public:
    RegisteredKeyValueStateBackendMetaInfo(
        StateDescriptor::Type stateType,     // @Nonnull
        const std::string& name,             // @Nonnull
        TypeSerializer* namespaceSerializer, // @Nonnull
        TypeSerializer* stateSerializer)     // @Nonnull
        : RegisteredStateMetaInfoBase(name),
          stateType(stateType),
          namespaceSerializer(namespaceSerializer),
          stateSerializer(stateSerializer)
    {
    }

    RegisteredKeyValueStateBackendMetaInfo(
        const std::string& name,             // @Nonnull
        TypeSerializer* namespaceSerializer, // @Nonnull
        TypeSerializer* stateSerializer)     // @Nonnull
        : RegisteredKeyValueStateBackendMetaInfo(
              StateDescriptor::Type::UNKNOWN, name, namespaceSerializer, stateSerializer) {};

    explicit RegisteredKeyValueStateBackendMetaInfo(const StateMetaInfoSnapshot& snapshot);
    // @Nonnull
    StateDescriptor::Type getStateType() const
    {
        return stateType;
    }

    // @Nonnull
    TypeSerializer* getNamespaceSerializer() const
    {
        return namespaceSerializer;
    }

    // @Nonnull
    TypeSerializer* getStateSerializer() const
    {
        return stateSerializer;
    }

    // @Nonnull
    std::shared_ptr<StateMetaInfoSnapshot> snapshot() override
    {
        return computeSnapshot();
    }

    void setNamespaceSerializer(TypeSerializer* nmspaceSerializer)
    {
        namespaceSerializer = nmspaceSerializer;
    }
    void setStateSerializer(TypeSerializer* sttSerializer)
    {
        stateSerializer = sttSerializer;
    }

private:
    // Instead of using serializer provider, straight up use serializer
    // Might change later if needed
    StateDescriptor::Type stateType;
    TypeSerializer* namespaceSerializer = nullptr;
    TypeSerializer* stateSerializer = nullptr;

    static StateDescriptor::Type parseStateType(const std::string& typeStr)
    {
        if (typeStr == "VALUE") return StateDescriptor::Type::VALUE;
        if (typeStr == "LIST") return StateDescriptor::Type::LIST;
        if (typeStr == "MAP") return StateDescriptor::Type::MAP;
        if (typeStr == "REDUCING") return StateDescriptor::Type::REDUCING;
        if (typeStr == "AGGREGATING") return StateDescriptor::Type::AGGREGATING;
        if (typeStr == "FOLDING") return StateDescriptor::Type::FOLDING;
        return StateDescriptor::Type::UNKNOWN;
    }
    // @Nonnull
    std::shared_ptr<StateMetaInfoSnapshot> computeSnapshot();
};
