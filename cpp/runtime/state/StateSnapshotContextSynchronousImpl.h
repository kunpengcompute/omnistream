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

#ifndef OMNISTREAM_STATESNAPSHOTCONTEXTSYNCHRONOUSIMPL
#define OMNISTREAM_STATESNAPSHOTCONTEXTSYNCHRONOUSIMPL

#include <future>
#include <memory>
#include <utility>

#include "KeyGroupRange.h"
#include "CheckpointStreamFactory.h"
#include "SnapshotResult.h"
#include "KeyedStateHandle.h"
#include "OperatorStateHandle.h"
#include "KeyedStateCheckpointOutputStream.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/state/bridge/OmniTaskBridge.h"

class StateSnapshotContextSynchronousImpl {
public:
    StateSnapshotContextSynchronousImpl(
        long checkpointId,
        long checkpointTimestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        KeyGroupRange* keyGroupRange);

    StateSnapshotContextSynchronousImpl(
        long checkpointId,
        long checkpointTimestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        KeyGroupRange* keyGroupRange,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge,
        CheckpointOptions* checkpointOptions);

    KeyedStateCheckpointOutputStream* getRawKeyedOperatorStateOutput();

    long getCheckpointId();
    long getCheckpointTimestamp();

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>
    getKeyedStateStreamFuture();
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
    getOperatorStateStreamFuture();
    void closeExceptionally();

protected:
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>
        keyedStateCheckpointClosingFuture;
    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>>
        operatorStateCheckpointClosingFuture;

private:
    long checkpointId_;
    long checkpointTimestamp_;
    std::shared_ptr<CheckpointStreamFactory> streamFactory_;
    KeyGroupRange* keyGroupRange_;
    std::shared_ptr<omnistream::OmniTaskBridge> bridge_;
    CheckpointOptions* checkpointOptions_ = nullptr;
    std::shared_ptr<KeyedStateCheckpointOutputStream> keyedStateCheckpointOutputStream_;
};

#endif // OMNISTREAM_STATESNAPSHOTCONTEXTSYNCHRONOUSIMPL
