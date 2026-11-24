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

#include "RocksDBSharedResources.h"
#include <cstdint>

class RocksDBMemoryControllerUtils {
public:
    RocksDBMemoryControllerUtils() = delete;
    ~RocksDBMemoryControllerUtils() = delete;
    RocksDBMemoryControllerUtils(const RocksDBMemoryControllerUtils&) = delete;
    RocksDBMemoryControllerUtils& operator=(const RocksDBMemoryControllerUtils&) = delete;

    static std::shared_ptr<RocksDBSharedResources> allocateRocksDBSharedResources(
        uint64_t totalMemorySize,
        double writeBufferRatio,
        double highPriorityPoolRatio,
        bool usingPartitionedIndexFilters)
    {
        uint64_t calculateCacheCapacity = calculateActualCacheCapacity(totalMemorySize, writeBufferRatio);
        auto cache = createCache(calculateCacheCapacity, highPriorityPoolRatio);
        uint64_t writeBufferManagerCapacity = calculateWriteBufferManagerCapacity(totalMemorySize, writeBufferRatio);
        auto writeBufferManager = createWriteBufferManager(writeBufferManagerCapacity, cache);
        INFO_RELEASE(
            "Create RocksDBSharedResources in C++. totalMemorySize: "
            << totalMemorySize << ", writeBufferRatio: " << writeBufferRatio << ", highPriorityPoolRatio: "
            << highPriorityPoolRatio << ", usingPartitionedIndexFilters: " << usingPartitionedIndexFilters
            << ", calculateCacheCapacity: " << calculateCacheCapacity
            << ", writeBufferManagerCapacity: " << writeBufferManagerCapacity);
        return std::make_shared<RocksDBSharedResources>(
            cache, writeBufferManager, writeBufferManagerCapacity, usingPartitionedIndexFilters);
    }

    static uint64_t calculateActualCacheCapacity(uint64_t totalMemorySize, double writeBufferRatio)
    {
        return static_cast<int64_t>((3 - writeBufferRatio) * totalMemorySize / 3);
    }

    static int64_t calculateWriteBufferManagerCapacity(int64_t totalMemorySize, double writeBufferRatio)
    {
        return static_cast<int64_t>(2 * totalMemorySize * writeBufferRatio / 3);
    }

    static std::shared_ptr<rocksdb::Cache> createCache(int64_t cacheCapacity, double highPriorityPoolRatio)
    {
        return rocksdb::NewLRUCache(cacheCapacity, -1, false, highPriorityPoolRatio);
    }

    static std::shared_ptr<rocksdb::WriteBufferManager> createWriteBufferManager(
        int64_t writeBufferManagerCapacity, const std::shared_ptr<rocksdb::Cache> cache)
    {
        return std::make_shared<rocksdb::WriteBufferManager>(writeBufferManagerCapacity, cache);
    }

    static int64_t calculateRocksDBDefaultArenaBlockSize(int64_t writeBufferSize)
    {
        int64_t arenaBlockSize = writeBufferSize / 8;

        constexpr int64_t align = 4 * 1024;
        return ((arenaBlockSize + align - 1) / align) * align;
    }

    static int64_t calculateRocksDBMutableLimit(int64_t bufferSize)
    {
        return bufferSize * 7 / 8;
    }

    static bool validateArenaBlockSize(int64_t arenaBlockSize, int64_t mutableLimit)
    {
        return arenaBlockSize <= mutableLimit;
    }
};
