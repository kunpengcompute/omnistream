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
#include <unordered_map>
#include <vector>

#include <rocksdb/slice.h>

#include "runtime/state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/heap/HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/RocksDBRestoreKVState.h"
#include "runtime/state/restore/RocksDBRestorePQState.h"
#include "runtime/state/restore/RocksDBHeapRestorePQState.h"
#include "runtime/state/rocksdb/RocksDbHandle.h"

namespace omnistream {

// ============================================================================
// RocksDBRestoreBackendDelegate — compatible restore 的 RocksDB writer 工厂
// ============================================================================

template <typename K>
class RocksDBRestoreBackendDelegate : public RestoreBackendDelegate {
public:
    using RegisteredPQStatesMap =
        std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>;

    RocksDBRestoreBackendDelegate(
        RocksDbHandle* rocksDbHandle,
        int startKeyGroup,
        int keyGroupPrefixBytes,
        long writeBatchSize,
        std::shared_ptr<RegisteredPQStatesMap> registeredPQStates = nullptr);

    std::unique_ptr<RestoreKVState> createKVState(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo) override;

    std::unique_ptr<RestorePQState> createPQState(int kvStateId, const StateMetaInfoSnapshot& metaInfo) override;

    void logStatistics() const;

private:
    struct KvStateInfo {
        std::string stateName;
        rocksdb::ColumnFamilyHandle* mainCF = nullptr;
        std::shared_ptr<StateMetaInfoSnapshot> mainMetaInfo;
    };

    KvStateInfo& ensureStateRegistered(int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo);

    RocksDBWriterContext& makeWriterContext()
    {
        return writerCtx_;
    }

    std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> getOrCreatePendingPQ(
        const StateMetaInfoSnapshot& metaInfo);

    RocksDbHandle* rocksDbHandle_;
    int startKeyGroup_;
    int keyGroupPrefixBytes_;
    long writeBatchSize_;
    RocksDBWriterContext writerCtx_;
    std::shared_ptr<RegisteredPQStatesMap> registeredPQStates_;
    std::unordered_map<int, KvStateInfo> kvStateInfo_;
    int64_t mainEntryCount_ = 0;
};

// ============================================================================
// 实现
// ============================================================================

template <typename K>
RocksDBRestoreBackendDelegate<K>::RocksDBRestoreBackendDelegate(
    RocksDbHandle* rocksDbHandle,
    int startKeyGroup,
    int keyGroupPrefixBytes,
    long writeBatchSize,
    std::shared_ptr<RegisteredPQStatesMap> registeredPQStates)
    : rocksDbHandle_(rocksDbHandle),
      startKeyGroup_(startKeyGroup),
      keyGroupPrefixBytes_(keyGroupPrefixBytes),
      writeBatchSize_(writeBatchSize),
      writerCtx_{
          rocksDbHandle->getDb(),
          writeBatchSize,
          keyGroupPrefixBytes,
          startKeyGroup,
          -1 /* keyGroupId: 哨兵值，必须由 setKeyGroupId() 设置后才可写入 */,
          &mainEntryCount_},
      registeredPQStates_(std::move(registeredPQStates))
{
}

template <typename K>
typename RocksDBRestoreBackendDelegate<K>::KvStateInfo& RocksDBRestoreBackendDelegate<K>::ensureStateRegistered(
    int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo)
{
    auto it = kvStateInfo_.find(kvStateId);
    if (it != kvStateInfo_.end()) {
        return it->second;
    }

    KvStateInfo info;
    info.stateName = mainMetaInfo.getName();
    info.mainMetaInfo = std::make_shared<StateMetaInfoSnapshot>(mainMetaInfo);

    auto mainStateInfo = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, mainMetaInfo);
    info.mainCF = mainStateInfo->columnFamilyHandle_;
    INFO_RELEASE("RocksDBRestoreBackendDelegate: state '" << mainMetaInfo.getName() << "' main CF registered");

    auto [insertedIt, _] = kvStateInfo_.emplace(kvStateId, std::move(info));
    return insertedIt->second;
}

template <typename K>
std::unique_ptr<RestoreKVState> RocksDBRestoreBackendDelegate<K>::createKVState(
    int kvStateId, const StateMetaInfoSnapshot& mainMetaInfo)
{
    auto& info = ensureStateRegistered(kvStateId, mainMetaInfo);
    return std::make_unique<RocksDBRestoreKVState<K>>(makeWriterContext(), info.mainCF, kvStateId);
}

// --- PQ State ---

template <typename K>
std::unique_ptr<RestorePQState> RocksDBRestoreBackendDelegate<K>::createPQState(
    int kvStateId, const StateMetaInfoSnapshot& metaInfo)
{
    if (registeredPQStates_ != nullptr) {
        auto pqWrapper = getOrCreatePendingPQ(metaInfo);
        if (pqWrapper != nullptr) {
            INFO_RELEASE(
                "RocksDBRestoreBackendDelegate: createPQState kvStateId="
                << kvStateId << ", name=" << metaInfo.getName() << " -> RocksDBHeapRestorePQState");
            return std::make_unique<RocksDBHeapRestorePQState>(pqWrapper, keyGroupPrefixBytes_);
        }
    }
    // 回退：纯 RocksDB PQ
    auto stateInfo = rocksDbHandle_->getOrRegisterStateColumnFamilyHandle(nullptr, metaInfo);
    INFO_RELEASE(
        "RocksDBRestoreBackendDelegate: createPQState kvStateId=" << kvStateId << ", name=" << metaInfo.getName());
    return std::make_unique<RocksDBRestorePQState>(rocksDbHandle_, stateInfo->columnFamilyHandle_, writeBatchSize_);
}

template <typename K>
std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase> RocksDBRestoreBackendDelegate<K>::getOrCreatePendingPQ(
    const StateMetaInfoSnapshot& metaInfo)
{
    const std::string& stateName = metaInfo.getName();
    auto existing = registeredPQStates_->find(stateName);
    if (existing != registeredPQStates_->end()) {
        return existing->second;
    }

    auto pqMetaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(metaInfo);
    auto pendingWrapper = std::make_shared<RestoredHeapPriorityQueueSnapshotRestoreWrapper>(pqMetaInfo);
    registeredPQStates_->emplace(stateName, pendingWrapper);
    INFO_RELEASE("RocksDBRestoreBackendDelegate: registered pending heap PQ state '" << stateName << "'");
    return pendingWrapper;
}

template <typename K>
void RocksDBRestoreBackendDelegate<K>::logStatistics() const
{
    INFO_RELEASE("RocksDBRestoreBackendDelegate: ======== restore statistics begin ========");
    INFO_RELEASE("RocksDBRestoreBackendDelegate: [stat] TOTAL: mainEntries=" << mainEntryCount_);
    INFO_RELEASE("RocksDBRestoreBackendDelegate: ======== restore statistics end ========");
}

} // namespace omnistream
