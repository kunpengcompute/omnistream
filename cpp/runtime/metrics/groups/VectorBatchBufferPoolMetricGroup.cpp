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

#include "VectorBatchBufferPoolMetricGroup.h"

#include <utility>

namespace omnistream {
VectorBatchBufferPoolMetricGroup::VectorBatchBufferPoolMetricGroup(
    AbstractMetricGroup* parent, SizeSupplierFactory sizeSupplierFactory)
    : AbstractMetricGroup(parent),
      sizeSupplierFactory(std::move(sizeSupplierFactory))
{
}

void VectorBatchBufferPoolMetricGroup::SetSizeSupplierFactory(SizeSupplierFactory sizeSupplierFactory_)
{
    sizeSupplierFactory = std::move(sizeSupplierFactory_);
    if (sizeSupplierFactory == nullptr) {
        return;
    }

    for (const auto& metricEntry : metrics) {
        auto sizeGauge = std::dynamic_pointer_cast<SizeGauge>(metricEntry.second);
        if (sizeGauge != nullptr) {
            sizeGauge->RegisterSupplier(sizeSupplierFactory(metricEntry.first));
        }
    }
}

void VectorBatchBufferPoolMetricGroup::AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
{
    metrics[metricName] = metric;
    auto sizeGauge = std::dynamic_pointer_cast<SizeGauge>(metric);
    if (sizeGauge != nullptr && sizeSupplierFactory != nullptr) {
        sizeGauge->RegisterSupplier(sizeSupplierFactory(metricName));
    }
}

std::shared_ptr<Metric> VectorBatchBufferPoolMetricGroup::GetMetric(const std::string& metricName) const
{
    auto it = metrics.find(metricName);
    if (it != metrics.end()) {
        return it->second;
    }
    return nullptr;
}

void VectorBatchBufferPoolMetricGroup::CleanMetrics()
{
    metrics.clear();
}
} // namespace omnistream
