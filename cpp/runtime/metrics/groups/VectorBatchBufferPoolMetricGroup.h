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

#include <functional>
#include <memory>
#include <string>

#include "runtime/metrics/Metric.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
class VectorBatchBufferPoolMetricGroup : public AbstractMetricGroup {
public:
    using SizeSupplierFactory = std::function<SizeGauge::SizeSupplier(const std::string&)>;

    explicit VectorBatchBufferPoolMetricGroup(
        AbstractMetricGroup* parent = nullptr, SizeSupplierFactory sizeSupplierFactory = nullptr);

    void SetSizeSupplierFactory(SizeSupplierFactory sizeSupplierFactory);
    void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric) override;
    std::shared_ptr<Metric> GetMetric(const std::string& metricName) const override;
    void CleanMetrics() override;

private:
    SizeSupplierFactory sizeSupplierFactory;

    std::string OBJECT_SEGMENT_SIZE = "objectSegmentSize";
    std::string REQUIRED_MEMORY = "requiredMemory";
    std::string CURRENT_POOL_MEMORY_BUDGET = "currentPoolMemoryBudget";
    std::string MAX_ALLOWED_MEMORY = "maxAllowedMemory";
    std::string USED_MEMORY = "usedMemory";
    std::string AVAILABLE_MEMORY = "availableMemory";
    std::string MAX_MEMORY_PER_CHANNEL = "maxMemoryPerChannel";
    std::string REQUEST_SEGMENT_NUMBER = "requestSegmentNumber";
    std::string RECYCLE_SEGMENT_NUMBER = "recycleSegmentNumber";
};
} // namespace omnistream
