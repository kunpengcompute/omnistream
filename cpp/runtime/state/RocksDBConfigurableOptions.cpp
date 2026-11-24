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

#include "RocksDBConfigurableOptions.h"
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::MAX_BACKGROUND_THREADS =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.thread.num"), new Integer(2));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::MAX_OPEN_FILES =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.files.open"), new Integer(-1));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::LOG_MAX_FILE_SIZE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.log.max-file-size"), new String("25mb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::LOG_FILE_NUM =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.log.file-num"), new Integer(4));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::LOG_DIR =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.log.dir"), new String(), false);
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::LOG_LEVEL = std::make_unique<ConfigOption>(
    new String("state.backend.rocksdb.log.level"), new Integer(static_cast<int>(InfoLogLevel::INFO_LEVEL)));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::COMPACTION_STYLE = std::make_unique<ConfigOption>(
    new String("state.backend.rocksdb.compaction.style"), new Integer(static_cast<int>(CompactionStyle::LEVEL)));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_DYNAMIC_LEVEL_SIZE = std::make_unique<ConfigOption>(
    new String("state.backend.rocksdb.compaction.level.use-dynamic-size"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::TARGET_FILE_SIZE_BASE = std::make_unique<ConfigOption>(
    new String("state.backend.rocksdb.compaction.level.target-file-size-base"), new String("64mb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::MAX_SIZE_LEVEL_BASE = std::make_unique<ConfigOption>(
    new String("state.backend.rocksdb.compaction.level.max-size-level-base"), new String("256mb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::WRITE_BUFFER_SIZE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.writebuffer.size"), new String("64mb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::MAX_WRITE_BUFFER_NUMBER =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.writebuffer.count"), new Integer(2));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::MIN_WRITE_BUFFER_NUMBER_TO_MERGE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.writebuffer.number-to-merge"), new Integer(1));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::BLOCK_SIZE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.block.blocksize"), new String("4kb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::METADATA_BLOCK_SIZE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.block.metadata-blocksize"), new String("4kb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::BLOCK_CACHE_SIZE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.block.cache-size"), new String("8mb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::WRITE_BATCH_SIZE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.write-batch-size"), new String("2mb"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_BLOOM_FILTER =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.use-bloom-filter"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::BLOOM_FILTER_BITS_PER_KEY =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.bloom-filter.bits-per-key"), new Double(10.0));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::BLOOM_FILTER_BLOCK_BASED_MODE =
    std::make_unique<ConfigOption>(
        new String("state.backend.rocksdb.bloom-filter.block-based-mode"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::RESTORE_OVERLAP_FRACTION_THRESHOLD =
    std::make_unique<ConfigOption>(
        new String("state.backend.rocksdb.restore-overlap-fraction-threshold"), new Double(0.0));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::CHECKSUM_TYPE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.checksum-type"), new String("kxxHash64"));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_MANAGED_MEMORY =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.memory.managed"), new Boolean(true));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::HIGH_PRIORITY_POOL_RATIO =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.memory.high-prio-pool-ratio"), new Double(0.1));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::WRITE_BUFFER_RATIO =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.memory.write-buffer-ratio"), new Double(0.5));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_PARTITIONED_INDEX_FILTERS =
    std::make_unique<ConfigOption>(
        new String("state.backend.rocksdb.memory.partitioned-index-filters"), new Boolean(false));
// FALCON PARAMETERS
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_PARTITION_FILTER =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.falcon.use-partition-filter"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_HASH_MEMTABLE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.falcon.use-hash-memtable"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_RANGE_FILTER =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.falcon.use-range-filter"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::USE_STATE_CACHE =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.falcon.use-state-cache"), new Boolean(false));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::STATE_CACHE_SIZE_LIMIT =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.falcon.state-cache-sizeLimit"), new Integer(3000));
std::unique_ptr<ConfigOption> RocksDBConfigurableOptions::PREFIX_EXTRACTOR_LENGTH =
    std::make_unique<ConfigOption>(new String("state.backend.rocksdb.falcon.prefix-extractor.length"), new Integer(13));
