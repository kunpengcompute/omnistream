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
class GlobalVectorBatchBufferMetricGroup : public AbstractMetricGroup {
public:
    using SizeSupplierFactory = std::function<SizeGauge::SizeSupplier(const std::string&)>;

    explicit GlobalVectorBatchBufferMetricGroup(
        AbstractMetricGroup* parent = nullptr, SizeSupplierFactory sizeSupplierFactory = nullptr);

    void SetSizeSupplierFactory(SizeSupplierFactory sizeSupplierFactory);
    void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric) override;

private:
    SizeSupplierFactory sizeSupplierFactory;

    std::string OBJECT_SEGMENT_SIZE = "objectSegmentSize";
    std::string TOTAL_NUMBER_OF_OBJECT_SEGMENTS = "totalNumberOfObjectSegments";
    std::string TOTAL_MEMORY = "totalMemory";
    std::string AVAILABLE_OBJECT_SEGMENTS = "availableObjectSegments";
    std::string AVAILABLE_MEMORY = "availableMemory";
    std::string USED_OBJECT_SEGMENTS = "usedObjectSegments";
    std::string USED_MEMORY = "usedMemory";
    std::string REGISTERED_BUFFER_POOLS = "registeredBufferPools";
    std::string BUFFER_COUNT = "bufferCount";
};
} // namespace omnistream
