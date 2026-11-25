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

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include "RocksDBSharedResources.h"

// Handles identify Omni-owned resources, never FRocksDB pointers. Slot-level deduplication
// and managed-memory reservation belong to Flink's MemoryManager on the Java side.
class NativeRocksDBResourceRegistry {
public:
    static NativeRocksDBResourceRegistry& instance();
    int64_t create(uint64_t bytes, double writeBufferRatio, double highPriorityRatio, bool partitionedFilters);
    std::shared_ptr<RocksDBSharedResources> acquire(int64_t handle);
    void release(int64_t handle);

private:
    std::mutex mutex_;
    int64_t nextHandle_ = 1;
    std::unordered_map<int64_t, std::shared_ptr<RocksDBSharedResources>> resources_;
};
