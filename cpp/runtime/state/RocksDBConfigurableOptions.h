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

#ifndef OMNISTREAM_ROCKSDBCONFIGURABLEOPTIONS_H
#define OMNISTREAM_ROCKSDBCONFIGURABLEOPTIONS_H
#include "core/include/common.h"
#include "core/configuration/ConfigOption.h"
#include "basictypes/Integer.h"
#include "basictypes/String.h"
#include "basictypes/Double.h"
#include "basictypes/java_lang_Boolean.h"
#include "rocksdb/InfoLogLevel.h"
#include "rocksdb/CompactionStyle.h"

class RocksDBConfigurableOptions {
public:
    static std::unique_ptr<ConfigOption> MAX_BACKGROUND_THREADS;
    static std::unique_ptr<ConfigOption> MAX_OPEN_FILES;
    static std::unique_ptr<ConfigOption> LOG_MAX_FILE_SIZE;
    static std::unique_ptr<ConfigOption> LOG_FILE_NUM;
    static std::unique_ptr<ConfigOption> LOG_DIR;
    static std::unique_ptr<ConfigOption> LOG_LEVEL;
    static std::unique_ptr<ConfigOption> COMPACTION_STYLE;
    static std::unique_ptr<ConfigOption> USE_DYNAMIC_LEVEL_SIZE;
    static std::unique_ptr<ConfigOption> TARGET_FILE_SIZE_BASE;
    static std::unique_ptr<ConfigOption> MAX_SIZE_LEVEL_BASE;
    static std::unique_ptr<ConfigOption> WRITE_BUFFER_SIZE;
    static std::unique_ptr<ConfigOption> MAX_WRITE_BUFFER_NUMBER;
    static std::unique_ptr<ConfigOption> MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
    static std::unique_ptr<ConfigOption> BLOCK_SIZE;
    static std::unique_ptr<ConfigOption> METADATA_BLOCK_SIZE;
    static std::unique_ptr<ConfigOption> BLOCK_CACHE_SIZE;
    static std::unique_ptr<ConfigOption> WRITE_BATCH_SIZE;
    static std::unique_ptr<ConfigOption> USE_BLOOM_FILTER;
    static std::unique_ptr<ConfigOption> BLOOM_FILTER_BITS_PER_KEY;
    static std::unique_ptr<ConfigOption> BLOOM_FILTER_BLOCK_BASED_MODE;
    static std::unique_ptr<ConfigOption> RESTORE_OVERLAP_FRACTION_THRESHOLD;
    static std::unique_ptr<ConfigOption> CHECKSUM_TYPE;
    static std::unique_ptr<ConfigOption> USE_MANAGED_MEMORY;
    static std::unique_ptr<ConfigOption> HIGH_PRIORITY_POOL_RATIO;
    static std::unique_ptr<ConfigOption> WRITE_BUFFER_RATIO;
    static std::unique_ptr<ConfigOption> USE_PARTITIONED_INDEX_FILTERS;
    // FALCON PARAMETERS
    static std::unique_ptr<ConfigOption> USE_PARTITION_FILTER;
    static std::unique_ptr<ConfigOption> USE_HASH_MEMTABLE;
    static std::unique_ptr<ConfigOption> USE_RANGE_FILTER;
    static std::unique_ptr<ConfigOption> USE_STATE_CACHE;
    static std::unique_ptr<ConfigOption> STATE_CACHE_SIZE_LIMIT;
    static std::unique_ptr<ConfigOption> PREFIX_EXTRACTOR_LENGTH;
};

#endif // OMNISTREAM_ROCKSDBCONFIGURABLEOPTIONS_H
