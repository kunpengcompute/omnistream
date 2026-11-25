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

#include "CompatibleFullSnapshotAsyncWriter.h"

#include <stdexcept>
#include <utility>

#include "CheckpointStateOutputStreamProxy.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include "common.h"

CompatibleFullSnapshotAsyncWriter::CompatibleFullSnapshotAsyncWriter(
    long checkpointId,
    CheckpointOptions* checkpointOptions,
    std::shared_ptr<CompatibleSavepointSnapshotResources> snapshotResources,
    std::string keySerializer,
    std::unique_ptr<omnistream::OperatorSavepointAdaptor> adaptor)
    : checkpointId_(checkpointId),
      checkpointOptions_(checkpointOptions),
      snapshotResources_(std::move(snapshotResources)),
      keySerializer_(std::move(keySerializer)),
      adaptor_(std::move(adaptor))
{
    if (checkpointOptions_ == nullptr) {
        INFO_RELEASE(
            "Error:CompatibleFullSnapshotAsyncWriter ctor cp=" << checkpointId_ << " checkpoint options is null");
        throw std::invalid_argument("Compatible snapshot checkpoint options must not be null");
    }
    if (snapshotResources_ == nullptr) {
        INFO_RELEASE(
            "Error:CompatibleFullSnapshotAsyncWriter ctor cp=" << checkpointId_ << " snapshot resources are null");
        throw std::invalid_argument("Compatible snapshot resources must not be null");
    }
    if (adaptor_ == nullptr) {
        INFO_RELEASE("Error:CompatibleFullSnapshotAsyncWriter ctor cp=" << checkpointId_ << " adaptor is null");
        throw std::invalid_argument("Compatible snapshot adaptor must not be null");
    }
}

std::shared_ptr<SnapshotResult<KeyedStateHandle>> CompatibleFullSnapshotAsyncWriter::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    auto sourceResources = snapshotResources_->sourceResources();
    const auto& metaInfoSnapshots = sourceResources->getMetaInfoSnapshots();
    adaptor_->validateForSave(metaInfoSnapshots);
    if (metaInfoSnapshots.empty()) {
        return SnapshotResult<KeyedStateHandle>::Empty();
    }
    if (bridge == nullptr) {
        INFO_RELEASE("Error:CompatibleFullSnapshotAsyncWriter::get cp=" << checkpointId_ << " bridge is null");
        throw std::invalid_argument("Compatible snapshot bridge must not be null");
    }

    auto* keyGroupRange = sourceResources->getKeyGroupRange();
    if (keyGroupRange == nullptr) {
        INFO_RELEASE("Error:CompatibleFullSnapshotAsyncWriter::get cp=" << checkpointId_ << " key group range is null");
        throw std::runtime_error("Compatible snapshot key group range must not be null");
    }
    auto keyGroupRangeOffsets = std::make_shared<KeyGroupRangeOffsets>(*keyGroupRange);
    CheckpointStateOutputStreamProxy stream(bridge, checkpointId_, checkpointOptions_);

    std::shared_ptr<SnapshotResult<StreamStateHandle>> streamResult;
    try {
        adaptor_->save(stream, *keyGroupRangeOffsets, *sourceResources, keySerializer_);

        streamResult = stream.close();
        if (streamResult == nullptr || streamResult->GetJobManagerOwnedSnapshot() == nullptr) {
            INFO_RELEASE(
                "Error:CompatibleFullSnapshotAsyncWriter::get cp=" << checkpointId_
                                                                   << " close returned empty stream state handle");
            throw std::runtime_error("Compatible snapshot close returned empty stream state handle");
        }
    } catch (...) {
        stream.abortNoThrow();
        throw;
    }

    std::shared_ptr<KeyedStateHandle> keyedStateHandle = std::make_shared<KeyGroupsSavepointStateHandle>(
        *keyGroupRangeOffsets, streamResult->GetJobManagerOwnedSnapshot());
    return SnapshotResult<KeyedStateHandle>::Of(keyedStateHandle);
}
