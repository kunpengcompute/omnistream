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
#include <string>
#include <vector>

#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

namespace omnistream {

// ============================================================================
// RocksDBHeapRestorePQState — 将 PQ entry 恢复到 Heap 中（而非 RocksDB）
// ============================================================================
//
// 当 RocksDB 状态后端配置了 Heap PQ（如 timer service），PQ state 的恢复路径
// 与 KV state 不同：KV 写入 RocksDB column family，PQ 通过
// HeapPriorityQueueSnapshotRestoreWrapperBase 暂存，后续由
// HeapPriorityQueuesManager drain 到类型化的 heap queue。
//
// 这类比 Flink 的 RocksDBHeapTimersFullRestoreOperation 中 PQ 的处理方式：
//   - getOrCreatePendingPriorityQueueState() 创建 RestoredHeap...Wrapper
//   - restoreKVStateData() 将 PQ entry 路由到 restoredPQ
//     → restoreSerializedElement(key, keyGroupPrefixBytes)

class RocksDBHeapRestorePQState : public RestorePQState {
public:
    RocksDBHeapRestorePQState(
        std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> pqWrapper, int keyGroupPrefixBytes)
        : pqWrapper_(std::move(pqWrapper)),
          keyGroupPrefixBytes_(keyGroupPrefixBytes)
    {
    }

    void writeEntry(const std::vector<int8_t>& keyBytes, const std::vector<int8_t>& /*valueBytes*/) override
    {
        pqWrapper_->restoreSerializedElement(keyBytes, keyGroupPrefixBytes_);
        entryCount_++;
        INFO_RELEASE("RocksDBHeapRestorePQState: writeEntry #" << entryCount_ << ", keySize=" << keyBytes.size());
    }

    void flush() override
    {
        INFO_RELEASE("RocksDBHeapRestorePQState: flush totalEntries=" << entryCount_);
    }

    void discard() override
    {
    }

private:
    std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> pqWrapper_;
    int keyGroupPrefixBytes_;
    int64_t entryCount_ = 0;
};

} // namespace omnistream
