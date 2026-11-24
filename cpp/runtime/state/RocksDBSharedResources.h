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
#include <memory>
#include "rocksdb/cache.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/write_buffer_manager.h"

class RocksDBSharedResources final {
public:
    RocksDBSharedResources(
        std::shared_ptr<ROCKSDB_NAMESPACE::Cache> cache,
        std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> writeBufferManager,
        int64_t writeBufferManagerCapacity,
        bool usingPartitionedIndexFilters)
        : cache_(cache),
          writeBufferManager_(writeBufferManager),
          writeBufferManagerCapacity_(writeBufferManagerCapacity),
          usingPartitionedIndexFilters_(usingPartitionedIndexFilters)
    {
    }

    std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> getWriteBufferManager()
    {
        return writeBufferManager_;
    }

    std::shared_ptr<ROCKSDB_NAMESPACE::Cache> getCache()
    {
        return cache_;
    }

    int64_t getWriteBufferManagerCapacity()
    {
        return writeBufferManagerCapacity_;
    }

    bool isUsingPartitionedIndexFilters() const
    {
        return usingPartitionedIndexFilters_;
    }

private:
    std::shared_ptr<ROCKSDB_NAMESPACE::Cache> cache_;
    std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> writeBufferManager_;
    int64_t writeBufferManagerCapacity_;
    bool usingPartitionedIndexFilters_;
};
