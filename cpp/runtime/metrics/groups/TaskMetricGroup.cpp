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

#include "TaskMetricGroup.h"

namespace omnistream {
// Constructor and Destructor
TaskMetricGroup::~TaskMetricGroup()
{
    CleanMetrics(); // Clear all metrics when the object is destroyed
}

// Add methods to manage task metrics
void TaskMetricGroup::AddTaskIOMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
{
    // taskIOMetricGroup[metricName] = metric;
    taskIOMetricGroup_.AddMetric(metricName, metric);
}

void TaskMetricGroup::AddInternalOperatorIOMetric(
    const std::string& operatorName, const std::string& metricName, std::shared_ptr<Metric> metric)
{
    internalOperatorIOMetricGroup[operatorName][metricName] = metric;
    operatorNames.insert(operatorName); // Add operator name to the set
}

std::shared_ptr<Metric> TaskMetricGroup::GetTaskIOMetric(const std::string& metricName) const
{
    return taskIOMetricGroup_.GetMetric(metricName);
}

std::shared_ptr<Metric> TaskMetricGroup::GetInternalOperatorIOMetric(
    const std::string& operatorName, const std::string& metricName) const
{
    std::string subOperatorName = operatorName.length() > 80 ? operatorName.substr(0, 80) : operatorName;
    auto it = internalOperatorIOMetricGroup.find(subOperatorName);
    if (it != internalOperatorIOMetricGroup.end()) {
        auto metricIt = it->second.find(metricName);
        if (metricIt != it->second.end()) {
            return metricIt->second;
        }
    }
    return nullptr;
}

std::unordered_set<std::string> TaskMetricGroup::GetOperatorNames() const
{
    return operatorNames;
}

void TaskMetricGroup::CleanMetrics()
{
    internalOperatorIOMetricGroup.clear();
    operatorNames.clear();
}

TaskIOMetricGroup* TaskMetricGroup::GetTaskIOMetricGroup()
{
    return &taskIOMetricGroup_;
}

std::shared_ptr<TaskBackendStateMetricGroup> TaskMetricGroup::GetTaskBackendStateMetricGroup()
{
    std::lock_guard<std::mutex> lock(backendStateMutex_);
    if (taskBackendStateMetricGroup_ == nullptr) {
        taskBackendStateMetricGroup_ = std::make_shared<TaskBackendStateMetricGroup>(this);
        addGroup("BackendState", taskBackendStateMetricGroup_);
    }
    return taskBackendStateMetricGroup_;
}
}; // namespace omnistream
