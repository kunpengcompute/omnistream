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

#ifndef OMNISTREAM_REGISTEREDOPERATORSTATEBACKENDMETAINFO_H
#define OMNISTREAM_REGISTEREDOPERATORSTATEBACKENDMETAINFO_H

#include <string>
#include <unordered_map>
#include <memory>

#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/TypeSerializerSnapshot.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

#include "OperatorStateHandle.h"
#include "RegisteredStateMetaInfoBase.h"

class RegisteredOperatorStateBackendMetaInfo : public RegisteredStateMetaInfoBase {
public:
    RegisteredOperatorStateBackendMetaInfo(
        const std::string& name, OperatorStateHandle::Mode assignmentMode, TypeSerializer* stateSerializer);

    explicit RegisteredOperatorStateBackendMetaInfo(const StateMetaInfoSnapshot& snapshot);

    std::shared_ptr<StateMetaInfoSnapshot> snapshot() override
    {
        return computeSnapshot();
    }

    OperatorStateHandle::Mode getAssignmentMode()
    {
        return assignmentMode_;
    }

    void updateAssignmentMode(OperatorStateHandle::Mode assignmentMode)
    {
        assignmentMode_ = assignmentMode;
    }

    TypeSerializer* getStateSerializer()
    {
        return stateSerializer_;
    }

    void updateStateSerializer(TypeSerializer* stateSerializer)
    {
        stateSerializer_ = stateSerializer;
    }

private:
    OperatorStateHandle::Mode assignmentMode_;
    TypeSerializer* stateSerializer_;

    std::shared_ptr<StateMetaInfoSnapshot> computeSnapshot();
};

#endif // OMNISTREAM_REGISTEREDOPERATORSTATEBACKENDMETAINFO_H
