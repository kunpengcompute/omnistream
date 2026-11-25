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

#include "TaskLocalNettyBufferMetricGroup.h"

#include <stdexcept>
#include <utility>

namespace omnistream {
TaskLocalNettyBufferMetricGroup::TaskLocalNettyBufferMetricGroup(
    AbstractMetricGroup* parent, SizeSupplierFactory sizeSupplierFactory)
    : AbstractMetricGroup(parent),
      sizeSupplierFactory(std::move(sizeSupplierFactory))
{
}

void TaskLocalNettyBufferMetricGroup::AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
{
    metrics[metricName] = metric;
    auto sizeGauge = std::dynamic_pointer_cast<SizeGauge>(metric);
    if (sizeGauge != nullptr) {
        if (sizeSupplierFactory == nullptr) {
            throw std::runtime_error("LocalNettyBufferPool size supplier factory is null for metric: " + metricName);
        }
        sizeGauge->RegisterSupplier(sizeSupplierFactory(metricName));
    }
}

std::shared_ptr<Metric> TaskLocalNettyBufferMetricGroup::GetMetric(const std::string& metricName) const
{
    auto it = metrics.find(metricName);
    if (it != metrics.end()) {
        return it->second;
    }
    return nullptr;
}

void TaskLocalNettyBufferMetricGroup::CleanMetrics()
{
    metrics.clear();
}
} // namespace omnistream
