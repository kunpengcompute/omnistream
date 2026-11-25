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
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "TaskIOMetricGroup.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"
#include "runtime/metrics/groups/TaskBackendStateMetricGroup.h"
#include "runtime/metrics/Metric.h"

namespace omnistream {
class TaskMetricGroup : public AbstractMetricGroup {
public:
    TaskMetricGroup() = default;
    // Clear all metrics
    ~TaskMetricGroup();
    void CleanMetrics();
    // Add methods to manage task metrics
    void AddTaskIOMetric(const std::string& metricName, std::shared_ptr<Metric> metric);
    void AddInternalOperatorIOMetric(
        const std::string& scope, const std::string& metricName, std::shared_ptr<Metric> metric);
    std::shared_ptr<Metric> GetTaskIOMetric(const std::string& metricName) const;
    std::shared_ptr<Metric> GetInternalOperatorIOMetric(
        const std::string& operatorName, const std::string& metricName) const;
    // get all operator names
    std::unordered_set<std::string> GetOperatorNames() const;

    TaskIOMetricGroup* GetTaskIOMetricGroup();

    // Single entry point for the per-operator keyed-state metrics. Lazily creates and
    // registers the child group on first access.
    std::shared_ptr<TaskBackendStateMetricGroup> GetTaskBackendStateMetricGroup();

private:
    // task io metric group
    // std::unordered_map<std::string, std::shared_ptr<Metric>> taskIOMetricGroup;
    // internal operator io metric group
    std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<Metric>>>
        internalOperatorIOMetricGroup;
    // store all operatorNames in a set structure
    std::unordered_set<std::string> operatorNames;
    TaskIOMetricGroup taskIOMetricGroup_;
    std::shared_ptr<TaskBackendStateMetricGroup> taskBackendStateMetricGroup_;
    std::mutex backendStateMutex_;
};
} // namespace omnistream
