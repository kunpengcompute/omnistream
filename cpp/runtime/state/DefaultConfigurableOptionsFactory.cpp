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

#include "DefaultConfigurableOptionsFactory.h"
void DefaultConfigurableOptionsFactory::createColumnOptions(
    ROCKSDB_NAMESPACE::ColumnFamilyOptions& currentOptions,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions& blockBasedTableOptions)
{
    auto compactionStyle =
        reinterpret_cast<Integer*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::COMPACTION_STYLE));
    if (compactionStyle != nullptr) {
        currentOptions.compaction_style = static_cast<ROCKSDB_NAMESPACE::CompactionStyle>(compactionStyle->getValue());
        compactionStyle->putRefCount();
    }

    auto levelCompactionDynamicLevelBytes = reinterpret_cast<Boolean*>(
        Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_DYNAMIC_LEVEL_SIZE));
    if (levelCompactionDynamicLevelBytes != nullptr) {
        currentOptions.level_compaction_dynamic_level_bytes = levelCompactionDynamicLevelBytes->value;
        levelCompactionDynamicLevelBytes->putRefCount();
    }

    auto targetFileSizeBase = reinterpret_cast<String*>(
        Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::TARGET_FILE_SIZE_BASE));
    if (targetFileSizeBase != nullptr) {
        currentOptions.target_file_size_base = MemorySize::parseBytes(targetFileSizeBase->getData());
        targetFileSizeBase->putRefCount();
    }

    auto writeBufferSize =
        reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::WRITE_BUFFER_SIZE));
    if (writeBufferSize != nullptr) {
        currentOptions.write_buffer_size = MemorySize::parseBytes(writeBufferSize->getData());
        writeBufferSize->putRefCount();
    }

    auto maxWriteBufferNumber = reinterpret_cast<Integer*>(
        Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::MAX_WRITE_BUFFER_NUMBER));
    if (maxWriteBufferNumber != nullptr) {
        currentOptions.max_write_buffer_number = maxWriteBufferNumber->value;
        maxWriteBufferNumber->putRefCount();
    }

    auto minWriteBufferNumberToMerge = reinterpret_cast<Integer*>(
        Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::MIN_WRITE_BUFFER_NUMBER_TO_MERGE));
    if (minWriteBufferNumberToMerge != nullptr) {
        currentOptions.min_write_buffer_number_to_merge = minWriteBufferNumberToMerge->value;
        minWriteBufferNumberToMerge->putRefCount();
    }

    auto blockSize =
        reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::BLOCK_SIZE));
    if (blockSize != nullptr) {
        blockBasedTableOptions.block_size = MemorySize::parseBytes(blockSize->getData());
        blockSize->putRefCount();
    }

    auto metadataBlockSize =
        reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::METADATA_BLOCK_SIZE));
    if (metadataBlockSize != nullptr) {
        blockBasedTableOptions.metadata_block_size = MemorySize::parseBytes(metadataBlockSize->getData());
        metadataBlockSize->putRefCount();
    }

    auto checksumType =
        reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::CHECKSUM_TYPE));
    if (checksumType != nullptr) {
        ROCKSDB_NAMESPACE::ChecksumType checksum = ROCKSDB_NAMESPACE::ChecksumType::kxxHash64;
        std::string checksumName = checksumType->toString();
        if (checksumName == "kNoChecksum") {
            checksum = ROCKSDB_NAMESPACE::ChecksumType::kNoChecksum;
        } else if (checksumName == "kCRC32c") {
            checksum = ROCKSDB_NAMESPACE::ChecksumType::kCRC32c;
        } else if (checksumName == "kxxHash") {
            checksum = ROCKSDB_NAMESPACE::ChecksumType::kxxHash;
        } else if (checksumName == "kxxHash64") {
            checksum = ROCKSDB_NAMESPACE::ChecksumType::kxxHash64;
        } else if (checksumName == "kXXH3") {
            checksum = ROCKSDB_NAMESPACE::ChecksumType::kXXH3;
        } else {
            GErrorLog("Invalid checksum type : " + checksumName + ", use default value : kxxHash64.");
        }
        blockBasedTableOptions.checksum = checksum;
        checksumType->putRefCount();
    }

    auto useBloomFilter =
        reinterpret_cast<Boolean*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_BLOOM_FILTER));
    if (useBloomFilter != nullptr && useBloomFilter->value) {
        auto bitsPerKey = reinterpret_cast<Double*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::BLOOM_FILTER_BITS_PER_KEY));
        if (bitsPerKey == nullptr) {
            bitsPerKey = reinterpret_cast<Double*>(
                RocksDBConfigurableOptions::BLOOM_FILTER_BITS_PER_KEY->GetDefaultValue()->clone());
        }

        auto blockBasedMode = reinterpret_cast<Boolean*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::BLOOM_FILTER_BLOCK_BASED_MODE));
        if (blockBasedMode == nullptr) {
            blockBasedMode = reinterpret_cast<Boolean*>(
                RocksDBConfigurableOptions::BLOOM_FILTER_BLOCK_BASED_MODE->GetDefaultValue()->clone());
        }
        auto filterPolicy = ROCKSDB_NAMESPACE::NewBloomFilterPolicy(bitsPerKey->value, blockBasedMode->value);
        blockBasedTableOptions.filter_policy.reset(filterPolicy);
        useBloomFilter->putRefCount();
        bitsPerKey->putRefCount();
        blockBasedMode->putRefCount();

        // [FALCON] enable filter parameters
        auto usePartitionFilter = reinterpret_cast<Boolean*>(
            Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_PARTITION_FILTER));

        if (usePartitionFilter != nullptr && usePartitionFilter->value) {
            blockBasedTableOptions.partition_filters = true;
            blockBasedTableOptions.index_type = ROCKSDB_NAMESPACE::BlockBasedTableOptions::kTwoLevelIndexSearch;
            INFO_RELEASE("[FALCON] enable partition filter.");
        }

        if (usePartitionFilter != nullptr) {
            usePartitionFilter->putRefCount();
        }
    }
    currentOptions.table_factory.reset(NewBlockBasedTableFactory(blockBasedTableOptions));
}

void DefaultConfigurableOptionsFactory::createDBOptions(rocksdb::DBOptions& currentOptions)
{
    auto maxThreadNum = reinterpret_cast<Integer*>(
        Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::MAX_BACKGROUND_THREADS));
    if (maxThreadNum != nullptr) {
        currentOptions.max_background_jobs = maxThreadNum->value;
        maxThreadNum->putRefCount();
    }

    auto mapOpenFiles =
        reinterpret_cast<Integer*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::MAX_OPEN_FILES));
    if (mapOpenFiles != nullptr) {
        currentOptions.max_open_files = mapOpenFiles->value;
        mapOpenFiles->putRefCount();
    }

    auto logLevel =
        reinterpret_cast<Integer*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::LOG_LEVEL));
    if (logLevel != nullptr) {
        currentOptions.info_log_level = static_cast<ROCKSDB_NAMESPACE::InfoLogLevel>(logLevel->value);
        logLevel->putRefCount();
    }
    auto logDir = reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::LOG_DIR));
    if (logDir != nullptr) {
        currentOptions.db_log_dir = logDir->toString();
        logDir->putRefCount();
    }

    auto logMaxFileSize =
        reinterpret_cast<String*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::LOG_MAX_FILE_SIZE));
    if (logMaxFileSize != nullptr) {
        currentOptions.max_log_file_size = MemorySize::parseBytes(logMaxFileSize->getData());
        logMaxFileSize->putRefCount();
    }

    auto logFileNum =
        reinterpret_cast<Integer*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::LOG_FILE_NUM));
    if (logFileNum != nullptr) {
        currentOptions.keep_log_file_num = logFileNum->value;
        logFileNum->putRefCount();
    }

    // [FALCON] set allow_concurrent_memtable_write as false for hash memTable
    auto useHashMemTable =
        reinterpret_cast<Boolean*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_HASH_MEMTABLE));

    if (useHashMemTable != nullptr && useHashMemTable->value) {
        currentOptions.allow_concurrent_memtable_write = false;
        INFO_RELEASE("[FALCON] successfully set allow_concurrent_memtable_write as false for hash memTable.");
    }

    if (useHashMemTable != nullptr) {
        useHashMemTable->putRefCount();
    }
}
