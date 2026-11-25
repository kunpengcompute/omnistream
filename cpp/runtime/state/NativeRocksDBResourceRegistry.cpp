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

#include "NativeRocksDBResourceRegistry.h"

#include <cmath>
#include <limits>
#include <stdexcept>
#include "common.h"
#include "RocksDBMemoryControllerUtils.h"

NativeRocksDBResourceRegistry& NativeRocksDBResourceRegistry::instance()
{
    static NativeRocksDBResourceRegistry registry;
    return registry;
}

int64_t NativeRocksDBResourceRegistry::create(
    uint64_t bytes, double writeBufferRatio, double highPriorityRatio, bool partitionedFilters)
{
    if (bytes == 0 || bytes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        !std::isfinite(writeBufferRatio) || writeBufferRatio <= 0 || writeBufferRatio >= 1 ||
        !std::isfinite(highPriorityRatio) || highPriorityRatio < 0 || highPriorityRatio >= 1 ||
        2 * writeBufferRatio / (3 - writeBufferRatio) + highPriorityRatio >= 1) {
        throw std::invalid_argument("Invalid Omni RocksDB shared-memory configuration");
    }
    auto resource = RocksDBMemoryControllerUtils::allocateRocksDBSharedResources(
        bytes, writeBufferRatio, highPriorityRatio, partitionedFilters);
    if (resource->getCache()->GetCapacity() == 0 || resource->getWriteBufferManagerCapacity() == 0) {
        throw std::invalid_argument("Omni RocksDB shared-memory budget is too small");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (nextHandle_ == std::numeric_limits<int64_t>::max()) {
        throw std::overflow_error("Omni RocksDB resource handle space exhausted");
    }
    const auto handle = nextHandle_++;
    INFO_RELEASE(
        "Omni RocksDB shared resource created: handle=" << handle << ", budget=" << bytes
                                                        << ", cache=" << resource->getCache().get()
                                                        << ", wbm=" << resource->getWriteBufferManager().get());
    resources_.emplace(handle, std::move(resource));
    return handle;
}

std::shared_ptr<RocksDBSharedResources> NativeRocksDBResourceRegistry::acquire(int64_t handle)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = resources_.find(handle);
    if (it == resources_.end()) {
        throw std::invalid_argument("Unknown or closed Omni RocksDB shared-resource handle");
    }
    return it->second;
}

void NativeRocksDBResourceRegistry::release(int64_t handle)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = resources_.find(handle);
    if (it == resources_.end()) {
        return;
    }
    // A remaining backend reference means Java is about to return its budget too early.
    if (it->second.use_count() != 1) {
        throw std::logic_error("Omni RocksDB shared resource still has native users during lease release");
    }
    resources_.erase(it);
    INFO_RELEASE("Omni RocksDB shared resource released: handle=" << handle);
}
