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

class RocksDBResourceContainer {
public:
    RocksDBResourceContainer(
        std::shared_ptr<RocksDBSharedResources> sharedResources,
        const fs::path& instanceBasePath,
        bool enableStatistics)
        : sharedResources_(sharedResources),
          enableStatistics_(enableStatistics)
    {
        if (!instanceBasePath.empty()) {
            instanceRocksDBPath_ = instanceBasePath / "db";
        }
    }

    ~RocksDBResourceContainer() = default;

    std::shared_ptr<rocksdb::DBOptions> getDbOptions()
    {
        auto options = createBaseCommonDBOptions();

        // set the configurable options
        DefaultConfigurableOptionsFactory::createDBOptions(*options);

        if (sharedResources_ != nullptr) {
            options->write_buffer_manager = sharedResources_->getWriteBufferManager();
        } else {
            THROW_LOGIC_EXCEPTION("RocksDBResourceContainer::getDbOptions, RocksDBSharedResources should not be null.");
        }

        return options;
    }

    std::shared_ptr<rocksdb::ColumnFamilyOptions> getColumnOptions()
    {
        auto columnFamilyOptions = createBaseCommonColumnOptions();
        ROCKSDB_NAMESPACE::BlockBasedTableOptions blockBasedTableOptions;
        if (!sharedResources_) {
            throw std::logic_error("Missing Omni RocksDB shared resources");
        }
        blockBasedTableOptions.block_cache = sharedResources_->getCache();
        blockBasedTableOptions.cache_index_and_filter_blocks = true;
        blockBasedTableOptions.cache_index_and_filter_blocks_with_high_priority = true;
        blockBasedTableOptions.pin_l0_filter_and_index_blocks_in_cache = true;
        INFO_RELEASE(
            "RocksDB shared block cache options: cacheIndexAndFilterBlocks="
            << blockBasedTableOptions.cache_index_and_filter_blocks << ", cacheIndexAndFilterBlocksWithHighPriority="
            << blockBasedTableOptions.cache_index_and_filter_blocks_with_high_priority
            << ", pinL0FilterAndIndexBlocksInCache=" << blockBasedTableOptions.pin_l0_filter_and_index_blocks_in_cache
            << ", blockCache=" << blockBasedTableOptions.block_cache.get());
        DefaultConfigurableOptionsFactory::createColumnOptions(*columnFamilyOptions, blockBasedTableOptions);
        if (sharedResources_->isUsingPartitionedIndexFilters() && blockBasedTableOptions.filter_policy) {
            blockBasedTableOptions.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
            blockBasedTableOptions.partition_filters = true;
            blockBasedTableOptions.pin_top_level_index_and_filter = true;
            columnFamilyOptions->table_factory.reset(rocksdb::NewBlockBasedTableFactory(blockBasedTableOptions));
        }
        return columnFamilyOptions;
    }

    std::shared_ptr<rocksdb::ReadOptions> getReadOptions()
    {
        auto readOptions = std::make_shared<rocksdb::ReadOptions>();
        return readOptions;
    }

    std::shared_ptr<rocksdb::WriteOptions> getWriteOptions()
    {
        auto writeOptions = std::make_shared<rocksdb::WriteOptions>();
        writeOptions->disableWAL = true;
        return writeOptions;
    }

    int64_t getWriteBufferManagerCapacity()
    {
        if (sharedResources_ == nullptr) {
            return -1;
        }
        return sharedResources_->getWriteBufferManagerCapacity();
    }

private:
    std::shared_ptr<RocksDBSharedResources> sharedResources_;
    std::optional<fs::path> instanceRocksDBPath_;
    bool enableStatistics_;

    std::shared_ptr<rocksdb::DBOptions> createBaseCommonDBOptions()
    {
        auto options = std::make_shared<rocksdb::DBOptions>();
        options->use_fsync = false;
        options->stats_dump_period_sec = 0;
        options->create_if_missing = true;
        return options;
    }

    std::shared_ptr<rocksdb::ColumnFamilyOptions> createBaseCommonColumnOptions()
    {
        auto opt = std::make_shared<rocksdb::ColumnFamilyOptions>();
        return opt;
    }
};
