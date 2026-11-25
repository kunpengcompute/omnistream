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
#include <unordered_map>

#include "runtime/metrics/Metric.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
class TaskLocalNettyBufferMetricGroup : public AbstractMetricGroup {
public:
    using SizeSupplierFactory = std::function<SizeGauge::SizeSupplier(const std::string&)>;

    explicit TaskLocalNettyBufferMetricGroup(
        AbstractMetricGroup* parent = nullptr, SizeSupplierFactory sizeSupplierFactory = nullptr);

    void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric) override;
    std::shared_ptr<Metric> GetMetric(const std::string& metricName) const override;
    void CleanMetrics() override;

private:
    SizeSupplierFactory sizeSupplierFactory;

    std::string REQUIRED_BUFFERS = "requiredBuffers";
    std::string MAX_BUFFERS = "maxBuffers";
    std::string CURRENT_POOL_SIZE = "currentPoolSize";
    std::string AVAILABLE_BUFFERS = "availableBuffers";
    std::string REQUESTED_BUFFERS = "requestedBuffers";
    std::string REQUEST_REGULAR_BUFFER_COUNT = "requestRegularBufferCount";
    std::string REQUEST_BIG_BUFFER_COUNT = "requestBigBufferCount";
    std::string RECYCLE_REGULAR_BUFFER_COUNT = "recycleRegularBufferCount";
    std::string RECYCLE_BIG_BUFFER_COUNT = "recycleBigBufferCount";
    std::string BIG_TOTAL_MEMORY_SIZE = "bigTotalMemorySize";
    std::string AVAILABLE_BIG_MEMORY_SIZE = "availableBigMemorySize";
};
} // namespace omnistream
