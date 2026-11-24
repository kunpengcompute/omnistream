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

#include <memory>
#include <utility>
#include <stdexcept>
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/options.h>
#include <atomic>

class RocksDBWriteBatchWrapper {
public:
    RocksDBWriteBatchWrapper(rocksdb::DB* db, long write_batch_size = DEFAULT_BATCH_SIZE)
        : RocksDBWriteBatchWrapper(db, nullptr, DEFAULT_CAPACITY, write_batch_size)
    {
    }

    RocksDBWriteBatchWrapper(rocksdb::DB* db, std::shared_ptr<rocksdb::WriteOptions> options)
        : RocksDBWriteBatchWrapper(db, std::move(options), DEFAULT_CAPACITY, DEFAULT_BATCH_SIZE)
    {
    }

    RocksDBWriteBatchWrapper(rocksdb::DB* db, std::shared_ptr<rocksdb::WriteOptions> options, long batch_size)
        : RocksDBWriteBatchWrapper(db, std::move(options), DEFAULT_CAPACITY, batch_size)
    {
    }

    RocksDBWriteBatchWrapper(
        rocksdb::DB* db, std::shared_ptr<rocksdb::WriteOptions> options, int capacity, long batch_size)
        : db_(db),
          options_(std::move(options)),
          capacity_(capacity),
          batch_size_(batch_size)
    {
        if (capacity < MIN_CAPACITY || capacity > MAX_CAPACITY) {
            throw std::invalid_argument(
                "Capacity should be between " + std::to_string(MIN_CAPACITY) + " and " + std::to_string(MAX_CAPACITY));
        }

        if (batch_size < 0) {
            throw std::invalid_argument("Max batch size have to be non-negative.");
        }

        if (batch_size_ > 0) {
            batch_ = std::make_unique<rocksdb::WriteBatch>(
                std::min(static_cast<long>(batch_size_), static_cast<long>(capacity_) * PER_RECORD_BYTES));
        } else {
            batch_ = std::make_unique<rocksdb::WriteBatch>(capacity_ * PER_RECORD_BYTES);
        }
    }

    void Put(rocksdb::ColumnFamilyHandle* handle, const rocksdb::Slice& key, const rocksdb::Slice& value)
    {
        batch_->Put(handle, key, value);
        FlushIfNeeded();
    }

    void Delete(rocksdb::ColumnFamilyHandle* handle, const rocksdb::Slice& key)
    {
        batch_->Delete(handle, key);
        FlushIfNeeded();
    }

    // disableWAL is set upstream on the shared WriteOptions
    // (RocksDBResourceContainer::getWriteOptions) and on instance-level
    // writeOptions members (RocksdbStateTable / RocksdbMapStateTable ctors).
    // All three call sites use the options-bearing constructor, so
    // Flush() itself does not need to touch disableWAL.
    void Flush()
    {
        rocksdb::Status status;
        if (options_) {
            status = db_->Write(*options_, batch_.get());
        } else {
            rocksdb::WriteOptions write_options;
            status = db_->Write(write_options, batch_.get());
        }

        if (!status.ok()) {
            throw std::runtime_error("RocksDB write failed: " + status.ToString());
        }

        batch_->Clear();
    }

    uint32_t Discard()
    {
        if (closed_.exchange(true)) {
            return 0;
        }
        uint32_t pendingEntries = batch_->Count();
        batch_->Clear();
        return pendingEntries;
    }

    std::shared_ptr<rocksdb::WriteOptions> GetOptions() const
    {
        return options_;
    }

    void close()
    {
        if (!closed_.exchange(true)) {
            if (batch_->Count() != 0) {
                Flush();
            }
            batch_->Clear();
        }
    }

    ~RocksDBWriteBatchWrapper()
    {
        try {
            close();
        } catch (...) {
            // do nothing
        }
    }

private:
    static constexpr int MIN_CAPACITY = 100;
    static constexpr int MAX_CAPACITY = 1000;
    static constexpr int PER_RECORD_BYTES = 100;
    static constexpr long DEFAULT_BATCH_SIZE = 0;
    static constexpr int DEFAULT_CAPACITY = 500;
    rocksdb::DB* db_;
    std::shared_ptr<rocksdb::WriteOptions> options_;
    std::unique_ptr<rocksdb::WriteBatch> batch_;
    uint32_t capacity_;
    long batch_size_;
    std::atomic<bool> closed_ = false;

    void FlushIfNeeded()
    {
        bool need_flush = batch_->Count() == capacity_ || (batch_size_ > 0 && GetDataSize() >= batch_size_);
        if (need_flush) {
            Flush();
        }
    }

    long GetDataSize() const
    {
        return batch_->GetDataSize();
    }
};
