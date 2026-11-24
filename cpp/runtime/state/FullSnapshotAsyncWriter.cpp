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

#include <cstring>
#include <map>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include "FullSnapshotAsyncWriter.h"
#include "CheckpointStateOutputStreamProxy.h"
#include "SavepointKvStreamWriter.h"
#include "bridge/OmniTaskBridge.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include "KeyGroupsStateHandle.h"
#include "TimerConsistencyCheckControl.h"
#include "state/heap/HeapPriorityQueueDataDigest.h"
#include "common.h"
FullSnapshotAsyncWriter::FullSnapshotAsyncWriter(
    std::shared_ptr<SnapshotType> snapshotType,
    CheckpointOptions* checkpointOptions,
    long checkpointId,
    const std::shared_ptr<FullSnapshotResources>& snapshotResources,
    std::string keySerializer)
    : snapshotResources_(snapshotResources),
      checkpointOptions_(checkpointOptions),
      checkpointId_(checkpointId),
      snapshotType_(std::move(snapshotType)),
      keySerializer_(keySerializer)
{
}
std::shared_ptr<SnapshotResult<KeyedStateHandle>> FullSnapshotAsyncWriter::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    std::shared_ptr<KeyValueStateIterator> mergeIterator = nullptr;
    try {
        auto* kgRange = snapshotResources_->getKeyGroupRange();
        auto keyGroupRangeOffsets = std::make_shared<KeyGroupRangeOffsets>(*kgRange);
        CheckpointStateOutputStreamProxy stream(bridge, checkpointId_, checkpointOptions_);
        const auto& metaInfoSnapshots = snapshotResources_->getMetaInfoSnapshots();
        stream.writeMetadata(metaInfoSnapshots, keySerializer_);

        std::map<int, HeapPriorityQueueDataDigest::Summary> heapPqDigests;
        const bool shouldDigestHeapPq = snapshotType_ != nullptr && snapshotType_->IsSavepoint() &&
                                        TimerConsistencyCheckControl::timerConsistencyCheckEnabled;
        if (shouldDigestHeapPq) {
            for (size_t i = 0; i < metaInfoSnapshots.size(); i++) {
                const int kvStateId = static_cast<int>(i);
                if (!snapshotResources_->isHeapPriorityQueueStateId(kvStateId)) {
                    continue;
                }
                const std::string stateName = metaInfoSnapshots[i] == nullptr ? "" : metaInfoSnapshots[i]->getName();
                heapPqDigests.emplace(
                    kvStateId,
                    HeapPriorityQueueDataDigest::createSummary(
                        "snapshot",
                        checkpointId_,
                        stateName,
                        kgRange == nullptr ? 0 : kgRange->getNumberOfKeyGroups()));
            }
        }

        const int keyGroupPrefixBytes = snapshotResources_->getKeyGroupPrefixBytes();
        auto recordHeapPqEntry = [&](int kvStateId, ByteView key, ByteView value) {
            auto digest = heapPqDigests.find(kvStateId);
            if (digest == heapPqDigests.end()) {
                return;
            }
            HeapPriorityQueueDataDigest::addSerializedEntry(digest->second, key, value, keyGroupPrefixBytes);
        };

        mergeIterator = snapshotResources_->createKVStateIterator();
        SavepointKvStreamWriter kvStreamWriter(stream, *keyGroupRangeOffsets);
        if (mergeIterator->isValid()) {
            const auto& entry = mergeIterator->current();
            if (shouldDigestHeapPq) {
                recordHeapPqEntry(entry.kvStateId, entry.key, entry.value);
            }
            kvStreamWriter.writeFirst(entry.key, entry.value, entry.kvStateId, entry.keyGroup);
            mergeIterator->next();
        }
        while (mergeIterator->isValid()) {
            const auto& entry = mergeIterator->current();
            if (shouldDigestHeapPq) {
                recordHeapPqEntry(entry.kvStateId, entry.key, entry.value);
            }
            kvStreamWriter.writeNext(
                entry.key, entry.value, entry.kvStateId, entry.keyGroup, entry.newKeyGroup, entry.newKeyValueState);
            mergeIterator->next();
        }
        kvStreamWriter.finish();
        mergeIterator->close();
        auto handle = stream.close();
        if (handle) {
            auto jobManagerOwnedSnapshot = handle->GetJobManagerOwnedSnapshot();
            std::shared_ptr<KeyedStateHandle> jmKeyedState;
            const bool isSavepoint = snapshotType_ && snapshotType_->IsSavepoint();
            if (isSavepoint) {
                jmKeyedState = std::make_shared<KeyGroupsSavepointStateHandle>(
                    *keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
            } else {
                jmKeyedState =
                    std::make_shared<KeyGroupsStateHandle>(*keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
            }
            for (const auto& digest : heapPqDigests) {
                HeapPriorityQueueDataDigest::logSummary(digest.second);
            }
            return SnapshotResult<KeyedStateHandle>::Of(jmKeyedState);
        }
        for (const auto& digest : heapPqDigests) {
            HeapPriorityQueueDataDigest::logSummary(digest.second);
        }
        return SnapshotResult<KeyedStateHandle>::Empty();
    } catch (std::exception& e) {
        if (mergeIterator) {
            mergeIterator->close();
        }
        INFO_RELEASE("Error:FullSnapshotAsyncWriter::get cp=" << checkpointId_ << " exception: " << e.what());
        throw;
    }
}
