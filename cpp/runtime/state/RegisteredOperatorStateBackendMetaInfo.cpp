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

#include "RegisteredOperatorStateBackendMetaInfo.h"

RegisteredOperatorStateBackendMetaInfo::RegisteredOperatorStateBackendMetaInfo(
    const std::string& name, OperatorStateHandle::Mode assignmentMode, TypeSerializer* stateSerializer)
    : RegisteredStateMetaInfoBase(name),
      assignmentMode_(assignmentMode),
      stateSerializer_(stateSerializer)
{
}

RegisteredOperatorStateBackendMetaInfo::RegisteredOperatorStateBackendMetaInfo(const StateMetaInfoSnapshot& snapshot)
    : RegisteredStateMetaInfoBase(snapshot.getName())
{
    auto assignmentModeStr =
        snapshot.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::OPERATOR_STATE_DISTRIBUTION_MODE);
    assignmentMode_ = OperatorStateHandle::StrToMode(assignmentModeStr);

    stateSerializer_ = snapshot.getValueSerializer();
}

std::shared_ptr<StateMetaInfoSnapshot> RegisteredOperatorStateBackendMetaInfo::computeSnapshot()
{
    std::unordered_map<std::string, std::string> optionsMap;
    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;

    std::string optionKey = StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::OPERATOR_STATE_DISTRIBUTION_MODE);
    optionsMap.emplace(optionKey, std::to_string((int)getAssignmentMode()));

    serializerMap.emplace(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, getStateSerializer());

    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::OPERATOR,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}
