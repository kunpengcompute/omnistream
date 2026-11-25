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

#ifndef FLINK_TNEL_RESTORESTATEDESCRIPTOR_H
#define FLINK_TNEL_RESTORESTATEDESCRIPTOR_H

#include "StateDescriptor.h"

/**
 * A non-template StateDescriptor subclass used during restore.
 * Reconstructed from StateMetaInfoSnapshot metadata (state type + BackendDataType info),
 * allowing createOrUpdateInternalState() to dispatch to the correct typed state table.
 */
class RestoreStateDescriptor : public StateDescriptor {
public:
    RestoreStateDescriptor(
        const std::string& name,
        Type type,
        TypeSerializer* stateSerializer,
        BackendDataType backendId,
        BackendDataType keyDataId = BackendDataType::VOID_NAMESPACE_BK,
        BackendDataType valueDataId = BackendDataType::VOID_NAMESPACE_BK)
        : StateDescriptor(name, stateSerializer),
          type_(type),
          backendId_(backendId),
          keyDataId_(keyDataId),
          valueDataId_(valueDataId)
    {
    }

    ~RestoreStateDescriptor() override = default;

    Type getType() override
    {
        return type_;
    }
    BackendDataType getBackendId() override
    {
        return backendId_;
    }
    BackendDataType getKeyDataId() override
    {
        return keyDataId_;
    }
    BackendDataType getValueDataId() override
    {
        return valueDataId_;
    }

private:
    Type type_;
    BackendDataType backendId_;
    BackendDataType keyDataId_;
    BackendDataType valueDataId_;
};

#endif // FLINK_TNEL_RESTORESTATEDESCRIPTOR_H
