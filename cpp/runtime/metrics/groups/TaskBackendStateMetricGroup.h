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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/metrics/groups/AbstractMetricGroup.h"
#include "runtime/metrics/groups/OperatorStateMetricGroup.h"

namespace omnistream {
// Holds one OperatorStateMetricGroup per operator (keyed by operator name). In OmniStream the
// per-operator InternalOperatorMetricGroups are not registered into the native TaskMetricGroup,
// so this group is our own home for per-operator keyed-state metrics.
class TaskBackendStateMetricGroup : public AbstractMetricGroup {
public:
    explicit TaskBackendStateMetricGroup(AbstractMetricGroup* parent = nullptr);

    // Idempotent: returns the existing group for the operator, or creates and registers one.
    std::shared_ptr<OperatorStateMetricGroup> GetOrCreateOperatorGroup(const std::string& operatorName);

    std::vector<std::string> GetOperatorGroupNames() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<OperatorStateMetricGroup>> operatorGroups_;
};
} // namespace omnistream
