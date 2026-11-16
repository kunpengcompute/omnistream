/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/slice.h>

#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"

namespace omnistream {

// RocksDB writer 运行时上下文：仅包含 writer 所需的裸值，不依赖 delegate/operation
struct RocksDBWriterContext {
    rocksdb::DB* db = nullptr;
    long writeBatchSize = 0;
    int keyGroupPrefixBytes = 0;
    int startKeyGroup = 0;
    int keyGroupId = 0;
    int64_t* mainEntryCount = nullptr;
};

template <typename K>
class RocksDBRestoreKVState : public RestoreKVState {
public:
    RocksDBRestoreKVState(RocksDBWriterContext& ctx, rocksdb::ColumnFamilyHandle* mainCF, int kvStateId)
        : ctx_(ctx),
          mainCF_(mainCF),
          kvStateId_(kvStateId)
    {
    }

    void flush() override
    {
        if (mainWriter_) {
            mainWriter_->Flush();
        }
        INFO_RELEASE("RocksDBRestoreKVState: flush kvStateId=" << kvStateId_ << ", totalEntries=" << entryCount_);
    }

    void setKeyGroupId(int keyGroupId) override
    {
        ctx_.keyGroupId = keyGroupId;
    }

    void discard() override
    {
        if (mainWriter_ != nullptr) {
            uint32_t pendingEntries = mainWriter_->Discard();
            if (pendingEntries > 0) {
                INFO_RELEASE(
                    "RocksDBRestoreKVState: discard kvStateId=" << kvStateId_ << ", pendingEntries=" << pendingEntries);
            }
        }
        mainWriter_.reset();
    }

protected:
    void writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value) override;

    RocksDBWriterContext& ctx_;
    rocksdb::ColumnFamilyHandle* mainCF_;
    int kvStateId_;
    std::unique_ptr<RocksDBWriteBatchWrapper> mainWriter_;
    int64_t entryCount_ = 0;
};

template <typename K>
void RocksDBRestoreKVState<K>::writeBytesEntry(const std::vector<int8_t>& keyBytes, ByteView value)
{
    rocksdb::Slice keySlice(reinterpret_cast<const char*>(keyBytes.data()), keyBytes.size());
    rocksdb::Slice valueSlice(reinterpret_cast<const char*>(value.data()), value.size());

    if (mainWriter_ == nullptr) {
        mainWriter_ = std::make_unique<RocksDBWriteBatchWrapper>(ctx_.db, ctx_.writeBatchSize);
    }
    mainWriter_->Put(mainCF_, keySlice, valueSlice);
    entryCount_++;
    if (ctx_.mainEntryCount) (*ctx_.mainEntryCount)++;
}
} // namespace omnistream
