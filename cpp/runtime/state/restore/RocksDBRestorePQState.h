/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this
 * software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:

 * *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES
 * OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT
 * FOR A PARTICULAR PURPOSE.
 */

#pragma once

#include <memory>
#include <vector>

#include <rocksdb/slice.h>

#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"

namespace omnistream {

// ============================================================================
// RocksDBRestorePQState — 将 PQ entry 写入 RocksDB column family
// ============================================================================

class RocksDBRestorePQState : public RestorePQState {
public:
    RocksDBRestorePQState(RocksDbHandle* rocksDbHandle, rocksdb::ColumnFamilyHandle* cfHandle, long writeBatchSize)
        : rocksDbHandle_(rocksDbHandle),
          cfHandle_(cfHandle),
          writeBatchSize_(writeBatchSize)
    {
    }

    void writeEntry(const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& valueBytes) override
    {
        if (pqWriter_ == nullptr) {
            pqWriter_ = std::make_unique<RocksDBWriteBatchWrapper>(rocksDbHandle_->getDb(), writeBatchSize_);
        }
        rocksdb::Slice keySlice(reinterpret_cast<const char*>(keyBytes.data()), keyBytes.size());
        rocksdb::Slice valueSlice(reinterpret_cast<const char*>(valueBytes.data()), valueBytes.size());
        pqWriter_->Put(cfHandle_, keySlice, valueSlice);
        entryCount_++;
        INFO_RELEASE(
            "RocksDBRestorePQState: writeEntry #" << entryCount_ << ", keySize=" << keyBytes.size()
                                                  << ", valueSize=" << valueBytes.size());
    }

    void flush() override
    {
        if (pqWriter_ != nullptr) {
            pqWriter_->Flush();
        }
        INFO_RELEASE("RocksDBRestorePQState: flush totalEntries=" << entryCount_);
    }

    void discard() override
    {
        if (pqWriter_ != nullptr) {
            uint32_t pendingEntries = pqWriter_->Discard();
            if (pendingEntries > 0) {
                INFO_RELEASE("RocksDBRestorePQState: discard pendingEntries=" << pendingEntries);
            }
        }
        pqWriter_.reset();
    }

private:
    RocksDbHandle* rocksDbHandle_;
    rocksdb::ColumnFamilyHandle* cfHandle_;
    long writeBatchSize_;
    std::unique_ptr<RocksDBWriteBatchWrapper> pqWriter_;
    int64_t entryCount_ = 0;
};

} // namespace omnistream
