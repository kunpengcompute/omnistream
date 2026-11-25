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

#include "IncrementalRemoteKeyedStateHandle.h"

IncrementalRemoteKeyedStateHandle::IncrementalRemoteKeyedStateHandle(
    UUID backendIdentifier,
    KeyGroupRange keyGroupRange,
    int64_t checkpointId,
    std::vector<HandleAndLocalPath> sharedState,
    std::vector<HandleAndLocalPath> privateState,
    std::shared_ptr<StreamStateHandle> metaStateHandle,
    long persistedSizeOfThisCheckpoint,
    StateHandleID stateHandleId)
    : backendIdentifier_(backendIdentifier),
      keyGroupRange_(keyGroupRange),
      checkpointId_(checkpointId),
      sharedState_(sharedState),
      privateState_(privateState),
      metaStateHandle_(metaStateHandle),
      persistedSizeOfThisCheckpoint_(persistedSizeOfThisCheckpoint),
      stateHandleId_(stateHandleId)

{
    this->persistedSizeOfThisCheckpoint_ =
        persistedSizeOfThisCheckpoint == unkownCheckpointSize ? GetStateSize() : persistedSizeOfThisCheckpoint;
}

IncrementalRemoteKeyedStateHandle* IncrementalRemoteKeyedStateHandle::Restore(
    UUID backendIdentifier,
    KeyGroupRange keyGroupRange,
    int64_t checkpointId,
    std::vector<HandleAndLocalPath> sharedState,
    std::vector<HandleAndLocalPath> privateState,
    std::shared_ptr<StreamStateHandle> metaStateHandle,
    long persistedSizeOfThisCheckpoint,
    StateHandleID stateHandleId)
{
    return new IncrementalRemoteKeyedStateHandle(
        backendIdentifier,
        keyGroupRange,
        checkpointId,
        sharedState,
        privateState,
        metaStateHandle,
        persistedSizeOfThisCheckpoint,
        stateHandleId);
}

std::shared_ptr<CheckpointBoundKeyedStateHandle> IncrementalRemoteKeyedStateHandle::rebound(
    int64_t newCheckpointId) const
{
    return std::make_shared<IncrementalRemoteKeyedStateHandle>(
        backendIdentifier_,
        keyGroupRange_,
        newCheckpointId,
        sharedState_,
        privateState_,
        metaStateHandle_,
        persistedSizeOfThisCheckpoint_,
        stateHandleId_);
}

std::string IncrementalRemoteKeyedStateHandle::ToString() const
{
    std::string sharedStateStr = "[";
    for (size_t i = 0; i < sharedState_.size(); ++i) {
        sharedStateStr += sharedState_[i].ToString();
        if (i != sharedState_.size() - 1) {
            sharedStateStr += ", ";
        }
    }
    sharedStateStr += "]";
    std::string privateStateStr = "[";
    for (size_t i = 0; i < privateState_.size(); ++i) {
        privateStateStr += privateState_[i].ToString();
        if (i != privateState_.size() - 1) {
            privateStateStr += ", ";
        }
    }
    privateStateStr += "]";

    std::ostringstream oss;
    oss << "{"
        << "\"backendIdentifier\": \"" << backendIdentifier_.ToString()
        << "\", \"stateHandleId\": " << stateHandleId_.ToString()
        << ", \"keyGroupRange\": " << keyGroupRange_.ToString() << ", \"checkpointId\": " << checkpointId_
        << ", \"sharedState\": " << sharedStateStr << ", \"privateState\": " << privateStateStr
        << ", \"metaStateHandle\": " << (metaStateHandle_ ? metaStateHandle_->ToString() : "null")
        << ", \"persistedSizeOfThisCheckpoint\": " << persistedSizeOfThisCheckpoint_
        << ", \"checkpointedSize\": " << persistedSizeOfThisCheckpoint_
        << ", \"stateHandleName\": \"IncrementalRemoteKeyedStateHandle\"}";
    return oss.str();
}

void IncrementalRemoteKeyedStateHandle::DiscardState()
{
    try {
        metaStateHandle_->DiscardState();
    } catch (...) {
        // Log error
    }

    try {
        for (auto path : privateState_) {
            path.getHandle()->DiscardState();
        }
    } catch (...) {
        // Log error
    }
}

void IncrementalRemoteKeyedStateHandle::RegisterSharedStates(SharedStateRegistry& stateRegistry, int64_t checkpointId)
{
    // Do nothing for now
}

long IncrementalRemoteKeyedStateHandle::GetCheckpointedSize()
{
    return persistedSizeOfThisCheckpoint_;
}

StateHandleID IncrementalRemoteKeyedStateHandle::GetStateHandleId() const
{
    return stateHandleId_;
}

std::shared_ptr<KeyedStateHandle> IncrementalRemoteKeyedStateHandle::GetIntersection(
    const KeyGroupRange& keyGroupRange) const
{
    auto range = keyGroupRange_.getIntersection(keyGroupRange);
    if (range == nullptr || range->getNumberOfKeyGroups() == 0) {
        delete range;
        return nullptr;
    }
    auto shared = std::make_shared<IncrementalRemoteKeyedStateHandle>(
        backendIdentifier_,
        *range,
        checkpointId_,
        sharedState_,
        privateState_,
        metaStateHandle_,
        persistedSizeOfThisCheckpoint_,
        stateHandleId_);
    return shared;
}

const UUID& IncrementalRemoteKeyedStateHandle::GetBackendIdentifier() const
{
    return backendIdentifier_;
}

std::shared_ptr<StreamStateHandle> IncrementalRemoteKeyedStateHandle::GetMetaDataStateHandle() const
{
    return metaStateHandle_;
}

const std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>&
IncrementalRemoteKeyedStateHandle::GetSharedStateHandles() const
{
    return GetSharedState();
}

bool IncrementalRemoteKeyedStateHandle::operator==(const IncrementalRemoteKeyedStateHandle& other) const
{
    if (this == &other) {
        return true;
    }
    if (GetStateHandleId() != other.GetStateHandleId()) {
        return false;
    }
    if (GetCheckpointId() != other.GetCheckpointId()) {
        return false;
    }
    if (GetBackendIdentifier() != other.GetBackendIdentifier()) {
        return false;
    }
    if (!(GetKeyGroupRange() == other.GetKeyGroupRange())) {
        return false;
    }
    if (!(GetSharedState() == other.GetSharedState())) {
        return false;
    }
    if (!(GetPrivateState() == other.GetPrivateState())) {
        return false;
    }
    auto meta1 = GetMetaDataStateHandle();
    auto meta2 = other.GetMetaDataStateHandle();
    if (meta1 == nullptr && meta2 == nullptr) {
        return true;
    }
    if ((meta1 == nullptr) != (meta2 == nullptr)) {
        return false;
    }
    return *meta1 == *meta2;
}

long IncrementalRemoteKeyedStateHandle::GetStateSize() const
{
    long size = metaStateHandle_ ? metaStateHandle_->GetStateSize() : 0;

    for (HandleAndLocalPath path : sharedState_) {
        size += path.GetStateSize();
    }

    for (HandleAndLocalPath path : privateState_) {
        size += path.GetStateSize();
    }

    return size;
}

KeyGroupRange IncrementalRemoteKeyedStateHandle::GetKeyGroupRange() const
{
    return keyGroupRange_;
}

int64_t IncrementalRemoteKeyedStateHandle::GetCheckpointId() const
{
    return checkpointId_;
}

const std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>& IncrementalRemoteKeyedStateHandle::GetSharedState()
    const
{
    return sharedState_;
}

std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> IncrementalRemoteKeyedStateHandle::GetPrivateState() const
{
    return privateState_;
}
