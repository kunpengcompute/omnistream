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

#include "RegisteredKeyValueStateBackendMetaInfo.h"

std::shared_ptr<StateMetaInfoSnapshot> RegisteredKeyValueStateBackendMetaInfo::computeSnapshot()
{
    std::unordered_map<std::string, std::string> optionsMap;
    optionsMap[StateMetaInfoSnapshot::commonOptionsKeyToString(
        StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE)] = std::to_string((int)stateType);

    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    serializerMap.emplace(StateMetaInfoSnapshot::VALUE_SERIALIZER_KEY, getStateSerializer());
    serializerMap.emplace(StateMetaInfoSnapshot::NAMESPACE_SERIALIZER_KEY, getNamespaceSerializer());

    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}

RegisteredKeyValueStateBackendMetaInfo::RegisteredKeyValueStateBackendMetaInfo(const StateMetaInfoSnapshot& snapshot)
    : RegisteredStateMetaInfoBase(snapshot.getName())
{
    auto stateTypeString = snapshot.getOption(StateMetaInfoSnapshot::CommonOptionsKeys::KEYED_STATE_TYPE);
    stateType = StateDescriptor::StringToType(stateTypeString);
    namespaceSerializer = snapshot.getNamespaceSerializer();
    stateSerializer = snapshot.getValueSerializer();
}
