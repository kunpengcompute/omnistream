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

#include "TaskBackendStateMetricGroup.h"

namespace omnistream {
TaskBackendStateMetricGroup::TaskBackendStateMetricGroup(AbstractMetricGroup* parent) : AbstractMetricGroup(parent)
{
}

std::shared_ptr<OperatorStateMetricGroup> TaskBackendStateMetricGroup::GetOrCreateOperatorGroup(
    const std::string& operatorName)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = operatorGroups_.find(operatorName);
    if (it != operatorGroups_.end()) {
        return it->second;
    }
    auto group = std::make_shared<OperatorStateMetricGroup>(this);
    operatorGroups_[operatorName] = group;
    addGroup(operatorName, group);
    return group;
}

std::vector<std::string> TaskBackendStateMetricGroup::GetOperatorGroupNames() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> names;
    names.reserve(operatorGroups_.size());
    for (const auto& pair : operatorGroups_) {
        names.push_back(pair.first);
    }
    return names;
}
} // namespace omnistream
