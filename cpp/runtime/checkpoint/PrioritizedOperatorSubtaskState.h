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

#ifndef OMNISTREAM_PRIORITIZEDOPERATORSUBTASKSTATE_H
#define OMNISTREAM_PRIORITIZEDOPERATORSUBTASKSTATE_H

#include <vector>
#include <list>
#include <memory>
#include <optional>
#include <functional>
#include <iterator>
#include <algorithm>
#include <stdexcept>

#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"
#include "runtime/state/StateObject.h"
#include "OperatorSubtaskState.h"
#include "StateObjectCollection.h"

namespace omnistream {

class PrioritizedOperatorSubtaskState {
public:
    // Forward declaration for nested Builder class
    class Builder;
    PrioritizedOperatorSubtaskState() = default;
    explicit PrioritizedOperatorSubtaskState(const std::optional<long>& restoredCheckpointId)
        : restoredCheckpointId(restoredCheckpointId) {};
    // StateObjectCollection contains a vector of shared_ptr<T>, therefore it is not copying the entire state.
    PrioritizedOperatorSubtaskState(
        const std::vector<StateObjectCollection<KeyedStateHandle>>& prioritizedManagedKeyedState,
        const std::vector<StateObjectCollection<KeyedStateHandle>>& prioritizedRawKeyedState,
        const std::vector<StateObjectCollection<OperatorStateHandle>>& prioritizedManagedOperatorState,
        const std::vector<StateObjectCollection<OperatorStateHandle>>& prioritizedRawOperatorState,
        const std::vector<StateObjectCollection<InputChannelStateHandle>>& prioritizedInputChannelState,
        const std::vector<StateObjectCollection<ResultSubpartitionStateHandle>>& prioritizedResultSubpartitionState,
        const std::optional<long>& restoredCheckpointId)
        : prioritizedManagedOperatorState(prioritizedManagedOperatorState),
          prioritizedRawOperatorState(prioritizedRawOperatorState),
          prioritizedManagedKeyedState(prioritizedManagedKeyedState),
          prioritizedRawKeyedState(prioritizedRawKeyedState),
          prioritizedInputChannelState(prioritizedInputChannelState),
          prioritizedResultSubpartitionState(prioritizedResultSubpartitionState),
          restoredCheckpointId(restoredCheckpointId)
    {
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns an immutable list with all alternative snapshots to restore the managed operator
     * state, in the order in which we should attempt to restore.
     */
    const std::vector<StateObjectCollection<OperatorStateHandle>>& getPrioritizedManagedOperatorState() const
    {
        return prioritizedManagedOperatorState;
    }

    /**
     * Returns an immutable list with all alternative snapshots to restore the raw operator state,
     * in the order in which we should attempt to restore.
     */
    const std::vector<StateObjectCollection<OperatorStateHandle>>& getPrioritizedRawOperatorState() const
    {
        return prioritizedRawOperatorState;
    }

    /**
     * Returns an immutable list with all alternative snapshots to restore the managed keyed state,
     * in the order in which we should attempt to restore.
     */
    const std::vector<StateObjectCollection<KeyedStateHandle>>& getPrioritizedManagedKeyedState() const
    {
        return prioritizedManagedKeyedState;
    }

    /**
     * Returns an immutable list with all alternative snapshots to restore the raw keyed state, in
     * the order in which we should attempt to restore.
     */
    const std::vector<StateObjectCollection<KeyedStateHandle>>& getPrioritizedRawKeyedState() const
    {
        return prioritizedRawKeyedState;
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns the managed operator state from the job manager, which represents the ground truth
     * about what this state should represent. This is the alternative with lowest priority.
     */
    StateObjectCollection<OperatorStateHandle> getJobManagerManagedOperatorState() const
    {
        return prioritizedManagedOperatorState.back();
    }

    /**
     * Returns the raw operator state from the job manager, which represents the ground truth about
     * what this state should represent. This is the alternative with lowest priority.
     */
    StateObjectCollection<OperatorStateHandle> getJobManagerRawOperatorState() const
    {
        return prioritizedRawOperatorState.back();
    }

    /**
     * Returns the managed keyed state from the job manager, which represents the ground truth about
     * what this state should represent. This is the alternative with lowest priority.
     */
    StateObjectCollection<KeyedStateHandle> getJobManagerManagedKeyedState() const
    {
        return prioritizedManagedKeyedState.back();
    }

    /**
     * Returns the raw keyed state from the job manager, which represents the ground truth about
     * what this state should represent. This is the alternative with lowest priority.
     */
    StateObjectCollection<KeyedStateHandle> getJobManagerRawKeyedState() const
    {
        return prioritizedRawKeyedState.back();
    }

    StateObjectCollection<InputChannelStateHandle> getPrioritizedInputChannelState() const
    {
        return prioritizedInputChannelState.back();
    }

    StateObjectCollection<ResultSubpartitionStateHandle> getPrioritizedResultSubpartitionState() const
    {
        return prioritizedResultSubpartitionState.back();
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns true if this was created for a restored operator, false otherwise. Restored operators
     * are operators that participated in a previous checkpoint, even if they did not emit any state
     * snapshots.
     */
    bool isRestored() const
    {
        return restoredCheckpointId.has_value();
    }

    /**
     * Returns the checkpoint id if this was created for a restored operator, null otherwise.
     * Restored operators are operators that participated in a previous checkpoint, even if they did
     * not emit any state snapshots.
     */
    std::optional<long> getRestoredCheckpointId() const
    {
        return restoredCheckpointId;
    }

public:
    /** A builder for PrioritizedOperatorSubtaskState. */
    class Builder {
    public:
        Builder(
            const OperatorSubtaskState& jobManagerState,
            const std::vector<OperatorSubtaskState>& alternativesByPriority)
            : jobManagerState(jobManagerState),
              alternativesByPriority(alternativesByPriority),
              restoredCheckpointId(-1)
        {
        }

        Builder(
            const OperatorSubtaskState& jobManagerState,
            const std::vector<OperatorSubtaskState>& alternativesByPriority,
            long restoredCheckpointId)
            : jobManagerState(jobManagerState),
              alternativesByPriority(alternativesByPriority),
              restoredCheckpointId(restoredCheckpointId)
        {
        }

        PrioritizedOperatorSubtaskState build();

    protected:
        /**
         * This helper method resolves the dependencies between the ground truth of the operator
         * state obtained from the job manager and potential alternatives for recovery, e.g. from a
         * task-local source.
         */
        template <typename T>
        std::vector<StateObjectCollection<T>> resolvePrioritizedAlternatives(
            const StateObjectCollection<T>& jobManagerState,
            const std::vector<StateObjectCollection<T>>& alternativesByPriority,
            const std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)>& approveFun);

    private:
        /** Ground truth of state, provided by job manager. */
        OperatorSubtaskState jobManagerState;

        /** (Local) alternatives to the job manager state. */
        std::vector<OperatorSubtaskState> alternativesByPriority;

        /** Checkpoint id of the restored checkpoint or null if not restored. */
        long restoredCheckpointId = -1;

    private:
        // T: type of the class to compare, E: type of class "identity", such as id
        // Given two class of type T, return whether they are equal based on their extracted identity <E>
        template <typename T, typename E>
        std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)> eqStateApprover(
            const std::function<const E(std::shared_ptr<T>)>& identityExtractor)
        {
            return [identityExtractor](std::shared_ptr<T> ref, std::shared_ptr<T> alt) -> bool {
                return identityExtractor(ref) == identityExtractor(alt);
            };
        }
    };

private:
    /** List of prioritized snapshot alternatives for managed operator state. */
    std::vector<StateObjectCollection<OperatorStateHandle>> prioritizedManagedOperatorState;

    /** List of prioritized snapshot alternatives for raw operator state. */
    std::vector<StateObjectCollection<OperatorStateHandle>> prioritizedRawOperatorState;

    /** List of prioritized snapshot alternatives for managed keyed state. */
    std::vector<StateObjectCollection<KeyedStateHandle>> prioritizedManagedKeyedState;

    /** List of prioritized snapshot alternatives for raw keyed state. */
    std::vector<StateObjectCollection<KeyedStateHandle>> prioritizedRawKeyedState;

    std::vector<StateObjectCollection<InputChannelStateHandle>> prioritizedInputChannelState;

    std::vector<StateObjectCollection<ResultSubpartitionStateHandle>> prioritizedResultSubpartitionState;

    /** Checkpoint id for a restored operator or null if not restored. */
    std::optional<long> restoredCheckpointId;
};
} // namespace omnistream

#endif
