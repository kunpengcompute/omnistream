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

#ifndef OMNISTREAM_OPERATORSNAPSHOTFINALIZER_H
#define OMNISTREAM_OPERATORSNAPSHOTFINALIZER_H
#include "OperatorSnapshotFutures.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/checkpoint/OperatorSubtaskState.h"
#include "core/utils/threads/FutureUtils.h"
#include "runtime/checkpoint/StateObjectCollection.h"

// Placeholder class
class OperatorSnapshotFinalizer {
public:
    explicit OperatorSnapshotFinalizer(OperatorSnapshotFutures* snapshotFutures)
    {
        LOG(">>>>>>> start OperatorSnapshotFinalizer");
        auto keyedStateManaged = FutureUtils::runIfNotDoneAndGet(snapshotFutures->getKeyedStateManagedFuture());
        auto KeyedStateRaw = FutureUtils::runIfNotDoneAndGet(snapshotFutures->getKeyedStateRawFuture());

        auto operatorStateManaged = FutureUtils::runIfNotDoneAndGet(snapshotFutures->getOperatorStateManagedFuture());
        auto operatorStateRaw = FutureUtils::runIfNotDoneAndGet(snapshotFutures->getOperatorStateRawFuture());

        std::shared_ptr<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>> inputChannelState =
            FutureUtils::runIfNotDoneAndGet(snapshotFutures->getInputChannelStateFuture());
        std::shared_ptr<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>> resultPartitionState =
            FutureUtils::runIfNotDoneAndGet(snapshotFutures->getResultSubpartitionStateFuture());

        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> jobManagerOwnedManaged;
        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> taskLocalManaged;
        if (keyedStateManaged) {
            jobManagerOwnedManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(
                keyedStateManaged->GetJobManagerOwnedSnapshot());
            taskLocalManaged =
                StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(keyedStateManaged->GetTaskLocalSnapshot());
        } else {
            jobManagerOwnedManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
            taskLocalManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
        }
        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> jobManagerOwnedRaw;
        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> taskLocalRaw;
        if (KeyedStateRaw) {
            jobManagerOwnedRaw =
                StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(KeyedStateRaw->GetJobManagerOwnedSnapshot());
            taskLocalRaw =
                StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(KeyedStateRaw->GetTaskLocalSnapshot());
        } else {
            jobManagerOwnedRaw = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
            taskLocalRaw = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
        }

        std::shared_ptr<StateObjectCollection<OperatorStateHandle>> jobManagerOwnedOperatorManaged;
        std::shared_ptr<StateObjectCollection<OperatorStateHandle>> taskLocalOperatorManaged;
        if (operatorStateManaged) {
            jobManagerOwnedOperatorManaged = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(
                operatorStateManaged->GetJobManagerOwnedSnapshot());
            taskLocalOperatorManaged = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(
                operatorStateManaged->GetTaskLocalSnapshot());
        } else {
            jobManagerOwnedOperatorManaged = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr);
            taskLocalOperatorManaged = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr);
        }

        std::shared_ptr<StateObjectCollection<OperatorStateHandle>> jobManagerOwnedOperatorRaw;
        std::shared_ptr<StateObjectCollection<OperatorStateHandle>> taskLocalOperatorRaw;
        if (operatorStateRaw) {
            jobManagerOwnedOperatorRaw = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(
                operatorStateRaw->GetJobManagerOwnedSnapshot());
            taskLocalOperatorRaw =
                StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(operatorStateRaw->GetTaskLocalSnapshot());
        } else {
            jobManagerOwnedOperatorRaw = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr);
            taskLocalOperatorRaw = StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr);
        }

        JobManagerOwnedState = std::make_shared<OperatorSubtaskState>(
            *jobManagerOwnedOperatorManaged,
            *jobManagerOwnedOperatorRaw,
            *jobManagerOwnedManaged,
            *jobManagerOwnedRaw,
            inputChannelState == nullptr ? *StateObjectCollection<InputChannelStateHandle>::Empty()
                                         : *inputChannelState->GetJobManagerOwnedSnapshot(),
            resultPartitionState == nullptr ? *StateObjectCollection<ResultSubpartitionStateHandle>::Empty()
                                            : *resultPartitionState->GetJobManagerOwnedSnapshot());
        taskLocalState = std::make_shared<OperatorSubtaskState>(
            *taskLocalOperatorManaged,
            *taskLocalOperatorRaw,
            *taskLocalManaged,
            *taskLocalRaw,
            inputChannelState == nullptr ? *StateObjectCollection<InputChannelStateHandle>::Empty()
                                         : *inputChannelState->GetJobManagerOwnedSnapshot(),
            resultPartitionState == nullptr ? *StateObjectCollection<ResultSubpartitionStateHandle>::Empty()
                                            : *resultPartitionState->GetJobManagerOwnedSnapshot());
        LOG(">>>>>>> end OperatorSnapshotFinalizer");
    };

    [[nodiscard]] std::shared_ptr<OperatorSubtaskState> getTaskLocalState() const
    {
        return taskLocalState;
    }
    [[nodiscard]] std::shared_ptr<OperatorSubtaskState> getJobManagerOwnedState() const
    {
        return JobManagerOwnedState;
    }

private:
    std::shared_ptr<OperatorSubtaskState> JobManagerOwnedState;
    std::shared_ptr<OperatorSubtaskState> taskLocalState;
};

#endif // OMNISTREAM_OPERATORSNAPSHOTFINALIZER_H
