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

#include "KeyGroupsSavepointStateHandle.h"
#include "state/KeyGroupsStateHandle.h"
#include <sstream>
#include <memory>
KeyGroupsSavepointStateHandle::KeyGroupsSavepointStateHandle(
    const KeyGroupRangeOffsets& groupRangeOffsets, const std::shared_ptr<StreamStateHandle>& streamStateHandle)
    : KeyGroupsStateHandle(groupRangeOffsets, streamStateHandle)
{
}

KeyGroupsSavepointStateHandle::KeyGroupsSavepointStateHandle(const nlohmann::json& description)
    : KeyGroupsStateHandle(description)
{
}

std::shared_ptr<KeyedStateHandle> KeyGroupsSavepointStateHandle::GetIntersection(
    const KeyGroupRange& keyGroupRange) const
{
    auto offsets = getGroupRangeOffsets().getIntersection(keyGroupRange);
    if (offsets.getKeyGroupRange().getNumberOfKeyGroups() <= 0) {
        return nullptr;
    }
    return std::make_shared<KeyGroupsSavepointStateHandle>(offsets, getDelegateStateHandle());
}
std::string KeyGroupsSavepointStateHandle::ToString() const
{
    nlohmann::json json;
    json["stateHandleName"] = "KeyGroupsSavepointStateHandle";
    json["stateHandleId"] = nlohmann::json::parse(GetStateHandleId().ToString());
    json["groupRangeOffsets"] = nlohmann::json::parse(getGroupRangeOffsets().ToString());
    if (getDelegateStateHandle() != nullptr) {
        nlohmann::json streamStateHandle = nlohmann::json::parse(getDelegateStateHandle()->ToString());
        json["streamStateHandle"] = streamStateHandle;
        json["stateHandle"] = streamStateHandle;
    } else {
        json["streamStateHandle"] = nullptr;
        json["stateHandle"] = nullptr;
    }
    return json.dump();
}
