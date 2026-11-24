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

#include "OperatorStateMetricGroup.h"

#include <stdexcept>

namespace omnistream {
constexpr char OperatorStateMetricGroup::VALUE_STATE_COUNT[];
constexpr char OperatorStateMetricGroup::MAP_STATE_COUNT[];
constexpr char OperatorStateMetricGroup::LIST_STATE_COUNT[];
constexpr char OperatorStateMetricGroup::VALUE_STATE_DATA_SIZE[];
constexpr char OperatorStateMetricGroup::MAP_STATE_DATA_SIZE[];
constexpr char OperatorStateMetricGroup::LIST_STATE_DATA_SIZE[];
constexpr char OperatorStateMetricGroup::VECTOR_BATCH_STATE_DATA_SIZE[];
constexpr char OperatorStateMetricGroup::VECTOR_BATCH_SIZE[];
constexpr char OperatorStateMetricGroup::TOTAL_BACKEND_STATE_DATA_SIZE[];

OperatorStateMetricGroup::OperatorStateMetricGroup(AbstractMetricGroup* parent) : AbstractMetricGroup(parent)
{
}

void OperatorStateMetricGroup::IncValueStateCount()
{
    valueStateCount_.fetch_add(1, std::memory_order_relaxed);
}

void OperatorStateMetricGroup::IncMapStateCount()
{
    mapStateCount_.fetch_add(1, std::memory_order_relaxed);
}

void OperatorStateMetricGroup::IncListStateCount()
{
    listStateCount_.fetch_add(1, std::memory_order_relaxed);
}

void OperatorStateMetricGroup::SetValueStateDataSize(int64_t size)
{
    valueStateDataSize_.store(size, std::memory_order_relaxed);
}

void OperatorStateMetricGroup::SetMapStateDataSize(int64_t size)
{
    mapStateDataSize_.store(size, std::memory_order_relaxed);
}

void OperatorStateMetricGroup::SetListStateDataSize(int64_t size)
{
    listStateDataSize_.store(size, std::memory_order_relaxed);
}

void OperatorStateMetricGroup::SetDataSizeSuppliers(DataSizeSuppliers suppliers)
{
    std::lock_guard<std::mutex> lock(dataSizeMutex_);
    valueDataSizeFn_ = std::move(suppliers.value);
    mapDataSizeFn_ = std::move(suppliers.map);
    listDataSizeFn_ = std::move(suppliers.list);
    vectorBatchDataSizeFn_ = std::move(suppliers.vectorBatchBytes);
    vectorBatchCountFn_ = std::move(suppliers.vectorBatchCount);
    totalDataSizeFn_ = std::move(suppliers.total);
}

void OperatorStateMetricGroup::ClearDataSizeSuppliers()
{
    std::lock_guard<std::mutex> lock(dataSizeMutex_);
    valueDataSizeFn_ = nullptr;
    mapDataSizeFn_ = nullptr;
    listDataSizeFn_ = nullptr;
    vectorBatchDataSizeFn_ = nullptr;
    vectorBatchCountFn_ = nullptr;
    totalDataSizeFn_ = nullptr;
}

// int-valued metrics (the three counts + the VectorBatch COUNT). Byte-valued
// metrics are sized in int64 by CreateLongSupplier (LongSizeGauge) to avoid INT_MAX truncation.
SizeGauge::SizeSupplier OperatorStateMetricGroup::CreateSupplier(const std::string& metricName)
{
    if (metricName == VALUE_STATE_COUNT) {
        return [this]() { return valueStateCount_.load(std::memory_order_relaxed); };
    }
    if (metricName == MAP_STATE_COUNT) {
        return [this]() { return mapStateCount_.load(std::memory_order_relaxed); };
    }
    if (metricName == LIST_STATE_COUNT) {
        return [this]() { return listStateCount_.load(std::memory_order_relaxed); };
    }
    // VectorBatch COUNT (number of held batches) stays int -- it cannot realistically
    // exceed INT_MAX. Pulls from the backend supplier under dataSizeMutex_, else falls back to the
    // 0 atomic (RocksDB backend).
    if (metricName == VECTOR_BATCH_SIZE) {
        return [this]() {
            std::lock_guard<std::mutex> lock(dataSizeMutex_);
            if (vectorBatchCountFn_) {
                return static_cast<int>(vectorBatchCountFn_());
            }
            return static_cast<int>(vectorBatchSize_.load(std::memory_order_relaxed));
        };
    }
    throw std::runtime_error("OperatorStateMetricGroup: unknown int metric name: " + metricName);
}

// byte-valued data-size metrics in int64 (LongSizeGauge). Each PULLS on demand from
// the backend-registered supplier (under dataSizeMutex_); when no supplier is registered (backend
// gone, or RocksDB) it falls back to the 0 atomic. No int truncation.
LongSizeGauge::LongSizeSupplier OperatorStateMetricGroup::CreateLongSupplier(const std::string& metricName)
{
    if (metricName == VALUE_STATE_DATA_SIZE) {
        return [this]() -> int64_t {
            std::lock_guard<std::mutex> lock(dataSizeMutex_);
            if (valueDataSizeFn_) {
                return valueDataSizeFn_();
            }
            return valueStateDataSize_.load(std::memory_order_relaxed);
        };
    }
    if (metricName == MAP_STATE_DATA_SIZE) {
        return [this]() -> int64_t {
            std::lock_guard<std::mutex> lock(dataSizeMutex_);
            if (mapDataSizeFn_) {
                return mapDataSizeFn_();
            }
            return mapStateDataSize_.load(std::memory_order_relaxed);
        };
    }
    if (metricName == LIST_STATE_DATA_SIZE) {
        return [this]() -> int64_t {
            std::lock_guard<std::mutex> lock(dataSizeMutex_);
            if (listDataSizeFn_) {
                return listDataSizeFn_();
            }
            return listStateDataSize_.load(std::memory_order_relaxed);
        };
    }
    if (metricName == VECTOR_BATCH_STATE_DATA_SIZE) {
        return [this]() -> int64_t {
            std::lock_guard<std::mutex> lock(dataSizeMutex_);
            if (vectorBatchDataSizeFn_) {
                return vectorBatchDataSizeFn_();
            }
            return vectorBatchStateDataSize_.load(std::memory_order_relaxed);
        };
    }
    if (metricName == TOTAL_BACKEND_STATE_DATA_SIZE) {
        return [this]() -> int64_t {
            std::lock_guard<std::mutex> lock(dataSizeMutex_);
            if (totalDataSizeFn_) {
                return totalDataSizeFn_();
            }
            return totalBackendStateDataSize_.load(std::memory_order_relaxed);
        };
    }
    throw std::runtime_error("OperatorStateMetricGroup: unknown long metric name: " + metricName);
}

void OperatorStateMetricGroup::AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
{
    metrics[metricName] = metric;
    // byte metrics arrive as LongSizeGauge (int64), counts as SizeGauge (int).
    if (auto longGauge = std::dynamic_pointer_cast<LongSizeGauge>(metric)) {
        longGauge->RegisterSupplier(CreateLongSupplier(metricName));
        return;
    }
    if (auto sizeGauge = std::dynamic_pointer_cast<SizeGauge>(metric)) {
        sizeGauge->RegisterSupplier(CreateSupplier(metricName));
    }
}
} // namespace omnistream
