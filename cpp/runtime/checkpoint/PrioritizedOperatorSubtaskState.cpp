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

#include "PrioritizedOperatorSubtaskState.h"
namespace omnistream {

PrioritizedOperatorSubtaskState PrioritizedOperatorSubtaskState::Builder::build()
{
    int size = static_cast<int>(alternativesByPriority.size());
    std::vector<StateObjectCollection<OperatorStateHandle>> managedOperatorAlternatives;
    managedOperatorAlternatives.reserve(size);
    std::vector<StateObjectCollection<KeyedStateHandle>> managedKeyedAlternatives;
    managedKeyedAlternatives.reserve(size);
    std::vector<StateObjectCollection<OperatorStateHandle>> rawOperatorAlternatives;
    rawOperatorAlternatives.reserve(size);
    std::vector<StateObjectCollection<KeyedStateHandle>> rawKeyedAlternatives;
    rawKeyedAlternatives.reserve(size);
    std::vector<StateObjectCollection<InputChannelStateHandle>> inputChannelStateAlternatives;
    inputChannelStateAlternatives.reserve(size);
    std::vector<StateObjectCollection<ResultSubpartitionStateHandle>> resultSubpartitionStateAlternatives;
    resultSubpartitionStateAlternatives.reserve(size);

    for (const OperatorSubtaskState& subtaskState : alternativesByPriority) {
        managedKeyedAlternatives.push_back(subtaskState.getManagedKeyedState());
        rawKeyedAlternatives.push_back(subtaskState.getRawKeyedState());
        managedOperatorAlternatives.push_back(subtaskState.getManagedOperatorState());
        rawOperatorAlternatives.push_back(subtaskState.getRawOperatorState());
        inputChannelStateAlternatives.push_back(subtaskState.getInputChannelState());
        resultSubpartitionStateAlternatives.push_back(subtaskState.getResultSubpartitionState());
    }
    using OPStateHandleIdentity = std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo>;
    return PrioritizedOperatorSubtaskState(
        resolvePrioritizedAlternatives(
            jobManagerState.getManagedKeyedState(),
            managedKeyedAlternatives,
            eqStateApprover<KeyedStateHandle, KeyGroupRange>(
                [](std::shared_ptr<KeyedStateHandle> handle) { return handle->GetKeyGroupRange(); })),
        resolvePrioritizedAlternatives(
            jobManagerState.getRawKeyedState(),
            rawKeyedAlternatives,
            eqStateApprover<KeyedStateHandle, KeyGroupRange>(
                [](std::shared_ptr<KeyedStateHandle> handle) { return handle->GetKeyGroupRange(); })),
        resolvePrioritizedAlternatives(
            jobManagerState.getManagedOperatorState(),
            managedOperatorAlternatives,
            eqStateApprover<OperatorStateHandle, OPStateHandleIdentity>(
                [](std::shared_ptr<OperatorStateHandle> handle) { return handle->getStateNameToPartitionOffsets(); })),
        resolvePrioritizedAlternatives(
            jobManagerState.getRawOperatorState(),
            rawOperatorAlternatives,
            eqStateApprover<OperatorStateHandle, OPStateHandleIdentity>(
                [](std::shared_ptr<OperatorStateHandle> handle) { return handle->getStateNameToPartitionOffsets(); })),
        resolvePrioritizedAlternatives(
            jobManagerState.getInputChannelState(),
            inputChannelStateAlternatives,
            eqStateApprover<InputChannelStateHandle, InputChannelInfo>(
                [](std::shared_ptr<InputChannelStateHandle> handle) { return handle->GetInfo(); })),
        resolvePrioritizedAlternatives(
            jobManagerState.getResultSubpartitionState(),
            resultSubpartitionStateAlternatives,
            eqStateApprover<ResultSubpartitionStateHandle, ResultSubpartitionInfoPOD>(
                [](std::shared_ptr<ResultSubpartitionStateHandle> handle) { return handle->GetInfo(); })),
        restoredCheckpointId);
}

template <typename T>
std::vector<StateObjectCollection<T>> PrioritizedOperatorSubtaskState::Builder::resolvePrioritizedAlternatives(
    const StateObjectCollection<T>& jobManagerState,
    const std::vector<StateObjectCollection<T>>& alternativesByPriority,
    const std::function<bool(std::shared_ptr<T>, std::shared_ptr<T>)>& approveFun)
{
    // Nothing to resolve if there are no alternatives, or the ground truth has already no
    // state, or if we can
    // assume that a rescaling happened because we find more than one handle in the JM state
    // (this is more a sanity
    // check).
    if (alternativesByPriority.empty() || !jobManagerState.HasState() || jobManagerState.Size() != 1) {
        std::vector<StateObjectCollection<T>> result;
        result.push_back(jobManagerState);
        return result;
    }

    // As we know size is == 1
    std::shared_ptr<T> reference = *jobManagerState.begin();

    // This will contain the end result, we initialize it with the potential max. size.
    std::vector<StateObjectCollection<T>> approved;
    approved.reserve(1 + alternativesByPriority.size());

    for (const StateObjectCollection<T>& alternative : alternativesByPriority) {
        // We found an alternative to the JM state if it has state, we have a 1:1
        // relationship, and the
        // approve-function signaled true.
        if (alternative.HasState() && alternative.Size() == 1 && approveFun(reference, *alternative.begin())) {
            approved.push_back(alternative);
        }
    }
    // Of course we include the ground truth as last alternative.
    approved.push_back(jobManagerState);
    return approved;
}
} // namespace omnistream
