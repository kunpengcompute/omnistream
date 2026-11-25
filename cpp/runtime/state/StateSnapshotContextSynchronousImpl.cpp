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

#include "StateSnapshotContextSynchronousImpl.h"
#include "core/include/common.h"

StateSnapshotContextSynchronousImpl::StateSnapshotContextSynchronousImpl(
    long checkpointId,
    long checkpointTimestamp,
    std::shared_ptr<CheckpointStreamFactory> streamFactory,
    KeyGroupRange* keyGroupRange)
    : StateSnapshotContextSynchronousImpl(
          checkpointId, checkpointTimestamp, streamFactory, keyGroupRange, nullptr, nullptr)
{
}

StateSnapshotContextSynchronousImpl::StateSnapshotContextSynchronousImpl(
    long checkpointId,
    long checkpointTimestamp,
    std::shared_ptr<CheckpointStreamFactory> streamFactory,
    KeyGroupRange* keyGroupRange,
    std::shared_ptr<omnistream::OmniTaskBridge> bridge,
    CheckpointOptions* checkpointOptions)
    : checkpointId_(checkpointId),
      checkpointTimestamp_(checkpointTimestamp),
      streamFactory_(streamFactory),
      keyGroupRange_(keyGroupRange),
      bridge_(std::move(bridge)),
      checkpointOptions_(checkpointOptions)
{
}

KeyedStateCheckpointOutputStream* StateSnapshotContextSynchronousImpl::getRawKeyedOperatorStateOutput()
{
    if (keyedStateCheckpointOutputStream_ == nullptr) {
        if (keyGroupRange_ == nullptr || keyGroupRange_->getNumberOfKeyGroups() <= 0) {
            THROW_LOGIC_EXCEPTION("Cannot create raw keyed state stream without a valid key-group range.");
        }
        if (bridge_ == nullptr || checkpointOptions_ == nullptr) {
            THROW_LOGIC_EXCEPTION("Cannot create raw keyed state stream without OmniTaskBridge and CheckpointOptions.");
        }
        keyedStateCheckpointOutputStream_ = std::make_shared<KeyedStateCheckpointOutputStream>(
            bridge_, checkpointId_, checkpointOptions_, keyGroupRange_);
    }
    return keyedStateCheckpointOutputStream_.get();
}

long StateSnapshotContextSynchronousImpl::getCheckpointId()
{
    return checkpointId_;
}

long StateSnapshotContextSynchronousImpl::getCheckpointTimestamp()
{
    return checkpointTimestamp_;
}

std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>
StateSnapshotContextSynchronousImpl::getKeyedStateStreamFuture()
{
    if (keyedStateCheckpointClosingFuture == nullptr) {
        auto stream = keyedStateCheckpointOutputStream_;
        keyedStateCheckpointClosingFuture =
            std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
                [stream]() -> std::shared_ptr<SnapshotResult<KeyedStateHandle>> {
                    if (stream == nullptr) {
                        return SnapshotResult<KeyedStateHandle>::Empty();
                    }
                    return stream->closeAndGetHandle();
                });
    }
    return keyedStateCheckpointClosingFuture;
}

std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
StateSnapshotContextSynchronousImpl::getOperatorStateStreamFuture()
{
    if (operatorStateCheckpointClosingFuture == nullptr) {
        //        operatorStateCheckpointClosingFuture =
        //            std::make_shared<std::packaged_task<SnapshotResult<OperatorStateHandle>>>(
        //                []() -> SnapshotResult<OperatorStateHandle> {
        //                    return SnapshotResult<OperatorStateHandle>(nullptr, nullptr);
        //                });
    }
    return operatorStateCheckpointClosingFuture;
}

void StateSnapshotContextSynchronousImpl::closeExceptionally()
{
    if (keyedStateCheckpointOutputStream_ != nullptr) {
        keyedStateCheckpointOutputStream_->closeExceptionally();
    }
}
