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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "runtime/metrics/Metric.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/LongSizeGauge.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
// Per-operator keyed-state metrics: counts of created Value/Map/List states and their
// data sizes. Counts are wired live; data sizes are scaffolded (always 0 until sourced).
class OperatorStateMetricGroup : public AbstractMetricGroup {
public:
    static constexpr char VALUE_STATE_COUNT[] = "valueStateCount";
    static constexpr char MAP_STATE_COUNT[] = "mapStateCount";
    static constexpr char LIST_STATE_COUNT[] = "listStateCount";
    static constexpr char VALUE_STATE_DATA_SIZE[] = "valueStateDataSize";
    static constexpr char MAP_STATE_DATA_SIZE[] = "mapStateDataSize";
    static constexpr char LIST_STATE_DATA_SIZE[] = "listStateDataSize";
    // VectorBatch buffers held in keyed state (join/dedup/topN) + the per-operator
    // grand total. VectorBatch metrics are heap-backend only (RocksDB registers no supplier -> 0).
    static constexpr char VECTOR_BATCH_STATE_DATA_SIZE[] = "vectorBatchStateDataSize";
    static constexpr char VECTOR_BATCH_SIZE[] = "vectorBatchSize";
    static constexpr char TOTAL_BACKEND_STATE_DATA_SIZE[] = "totalBackendStateDataSize";

    explicit OperatorStateMetricGroup(AbstractMetricGroup* parent = nullptr);

    void IncValueStateCount();
    void IncMapStateCount();
    void IncListStateCount();

    void SetValueStateDataSize(int64_t size);
    void SetMapStateDataSize(int64_t size);
    void SetListStateDataSize(int64_t size);

    // on-demand data-size sources, registered by the keyed-state backend. The
    // SizeGauge supplier (metric-reporter thread) invokes these to PULL the current sizes, instead
    // of reading a value pushed at checkpoint. Each fn reads only reporter-safe atomics/size(),
    // never a live CopyOnWriteStateMap entry. Guarded by dataSizeMutex_ so the backend can clear
    // them in its destructor (blocking any in-flight reporter call) to avoid use-after-free.
    using DataSizeSupplier = std::function<int64_t()>;
    struct DataSizeSuppliers {
        DataSizeSupplier value;
        DataSizeSupplier map;
        DataSizeSupplier list;
        DataSizeSupplier vectorBatchBytes;
        DataSizeSupplier vectorBatchCount;
        DataSizeSupplier total;
    };
    void SetDataSizeSuppliers(DataSizeSuppliers suppliers);
    void ClearDataSizeSuppliers();

    // Wires a SizeGauge metric to read live from this group's fields.
    void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric) override;

private:
    // count metrics (int) use SizeGauge; byte metrics (int64) use LongSizeGauge.
    SizeGauge::SizeSupplier CreateSupplier(const std::string& metricName);
    LongSizeGauge::LongSizeSupplier CreateLongSupplier(const std::string& metricName);

    std::atomic<int> valueStateCount_{0};
    std::atomic<int> mapStateCount_{0};
    std::atomic<int> listStateCount_{0};
    std::atomic<int64_t> valueStateDataSize_{0};
    std::atomic<int64_t> mapStateDataSize_{0};
    std::atomic<int64_t> listStateDataSize_{0};
    // fallback atomics (default 0) read when no supplier is registered -- e.g. the
    // RocksDB backend, which registers none, so its VectorBatch/total gauges read 0.
    std::atomic<int64_t> vectorBatchStateDataSize_{0};
    std::atomic<int64_t> vectorBatchSize_{0};
    std::atomic<int64_t> totalBackendStateDataSize_{0};

    // protects the on-demand suppliers below.
    std::mutex dataSizeMutex_;
    DataSizeSupplier valueDataSizeFn_;
    DataSizeSupplier mapDataSizeFn_;
    DataSizeSupplier listDataSizeFn_;
    DataSizeSupplier vectorBatchDataSizeFn_;
    DataSizeSupplier vectorBatchCountFn_;
    DataSizeSupplier totalDataSizeFn_;
};
} // namespace omnistream
