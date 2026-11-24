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

#ifndef OMNISTREAM_OPERATORSUBTASKSTATE_H
#define OMNISTREAM_OPERATORSUBTASKSTATE_H
#include <nlohmann/json.hpp>
#include "runtime/state/CompositeStateHandle.h"
#include "runtime/checkpoint/InflightDataRescalingDescriptor.h"
#include "runtime/checkpoint/StateObjectCollection.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"
namespace omnistream {
class OperatorSubtaskState : public CompositeStateHandle {
public:
    OperatorSubtaskState(
        StateObjectCollection<OperatorStateHandle>& managedOperatorState,
        StateObjectCollection<OperatorStateHandle>& rawOperatorState,
        StateObjectCollection<KeyedStateHandle>& managedKeyedState,
        StateObjectCollection<KeyedStateHandle>& rawKeyedState,
        StateObjectCollection<InputChannelStateHandle>& inputChannelState,
        StateObjectCollection<ResultSubpartitionStateHandle>& resultSubpartitionState,
        std::shared_ptr<InflightDataRescalingDescriptor> inputRescalingDescriptor,
        std::shared_ptr<InflightDataRescalingDescriptor> outputRescalingDescriptor)
        : managedOperatorState(managedOperatorState),
          rawOperatorState(rawOperatorState),
          managedKeyedState(managedKeyedState),
          rawKeyedState(rawKeyedState),
          inputChannelState(inputChannelState),
          resultSubpartitionState(resultSubpartitionState),
          inputRescalingDescriptor(inputRescalingDescriptor),
          outputRescalingDescriptor(outputRescalingDescriptor),
          stateSize(
              this->managedOperatorState.GetStateSize() + this->rawOperatorState.GetStateSize() +
              this->managedKeyedState.GetStateSize() + this->rawKeyedState.GetStateSize() +
              this->inputChannelState.GetStateSize() + this->resultSubpartitionState.GetStateSize()),
          checkpointedSize(
              this->managedOperatorState.GetCheckpointedSize() + this->rawOperatorState.GetCheckpointedSize() +
              this->managedKeyedState.GetCheckpointedSize() + this->rawKeyedState.GetCheckpointedSize() +
              this->inputChannelState.GetCheckpointedSize() + this->resultSubpartitionState.GetCheckpointedSize())
    {
    }

    OperatorSubtaskState()
        : inputRescalingDescriptor(InflightDataRescalingDescriptor::noRescale),
          outputRescalingDescriptor(InflightDataRescalingDescriptor::noRescale)
    {
    }

    OperatorSubtaskState(
        StateObjectCollection<OperatorStateHandle>& managedOperatorState,
        StateObjectCollection<OperatorStateHandle>& rawOperatorState,
        StateObjectCollection<KeyedStateHandle>& managedKeyedState,
        StateObjectCollection<KeyedStateHandle>& rawKeyedState,
        StateObjectCollection<InputChannelStateHandle>& inputChannelState,
        StateObjectCollection<ResultSubpartitionStateHandle>& resultSubpartitionState)
        : managedOperatorState(managedOperatorState),
          rawOperatorState(rawOperatorState),
          managedKeyedState(managedKeyedState),
          rawKeyedState(rawKeyedState),
          inputChannelState(inputChannelState),
          resultSubpartitionState(resultSubpartitionState),
          inputRescalingDescriptor(InflightDataRescalingDescriptor::noRescale),
          outputRescalingDescriptor(InflightDataRescalingDescriptor::noRescale),
          stateSize(
              this->managedOperatorState.GetStateSize() + this->rawOperatorState.GetStateSize() +
              this->managedKeyedState.GetStateSize() + this->rawKeyedState.GetStateSize() +
              this->inputChannelState.GetStateSize() + this->resultSubpartitionState.GetStateSize()),
          checkpointedSize(
              this->managedOperatorState.GetCheckpointedSize() + this->rawOperatorState.GetCheckpointedSize() +
              this->managedKeyedState.GetCheckpointedSize() + this->rawKeyedState.GetCheckpointedSize() +
              this->inputChannelState.GetCheckpointedSize() + this->resultSubpartitionState.GetCheckpointedSize())
    {
    }

    bool HasState()
    {
        return managedOperatorState.HasState() || rawOperatorState.HasState() || managedKeyedState.HasState() ||
               rawKeyedState.HasState() || inputChannelState.HasState() || resultSubpartitionState.HasState();
    }

    long GetStateSize() const override
    {
        return stateSize;
    }

    void DiscardState() override
    {
        try {
            std::unordered_set<const void*> discardedHandles;

            DiscardStateHandles(managedOperatorState, discardedHandles, false);
            DiscardStateHandles(rawOperatorState, discardedHandles, false);
            DiscardStateHandles(managedKeyedState, discardedHandles, false);
            DiscardStateHandles(rawKeyedState, discardedHandles, false);

            DiscardStateHandles(inputChannelState, discardedHandles, true);
            DiscardStateHandles(resultSubpartitionState, discardedHandles, true);
        } catch (const std::exception& ex) {
            LOG("Exception while discarding states: " + std::string(ex.what()));
        }
    }

    void RegisterSharedStates(SharedStateRegistry& stateRegistry, long checkpointID) override
    {
    }

    [[nodiscard]] StateObjectCollection<KeyedStateHandle> GetManagedKeyedState() const
    {
        return managedKeyedState;
    }

    std::string ToString() const override
    {
        nlohmann::json j;

        j["stateHandleName"] = "OperatorSubtaskState";
        j["managedOperatorState"] = nlohmann::json::parse(managedOperatorState.ToString());
        j["rawOperatorState"] = nlohmann::json::parse(rawOperatorState.ToString());
        j["managedKeyedState"] = nlohmann::json::parse(managedKeyedState.ToString());
        j["rawKeyedState"] = nlohmann::json::parse(rawKeyedState.ToString());
        j["inputChannelState"] = nlohmann::json::parse(inputChannelState.ToString());
        j["resultSubpartitionState"] = nlohmann::json::parse(resultSubpartitionState.ToString());
        j["stateSize"] = stateSize;
        j["checkpointedSize"] = checkpointedSize;
        return j.dump();
    }

    long GetCheckpointedSize() override
    {
        return checkpointedSize;
    }

    void SetManagedOperatorState(std::shared_ptr<OperatorStateHandle> handle)
    {
        managedOperatorState.Add(handle);
        stateSize += handle->GetStateSize();
    }

    [[nodiscard]] auto getResultSubpartitionState() const
    {
        return resultSubpartitionState;
    }

    [[nodiscard]] const auto& getInputChannelState() const
    {
        return inputChannelState;
    }
    [[nodiscard]] const auto& getRawKeyedState() const
    {
        return rawKeyedState;
    }

    [[nodiscard]] const auto& getRawOperatorState() const
    {
        return rawOperatorState;
    }

    [[nodiscard]] const auto& getManagedOperatorState() const
    {
        return managedOperatorState;
    }

    [[nodiscard]] const auto& getManagedKeyedState() const
    {
        return managedKeyedState;
    }
    const auto getInputRescalingDescriptor() const
    {
        return inputRescalingDescriptor;
    }
    const auto getOutputRescalingDescriptor() const
    {
        return outputRescalingDescriptor;
    }
    bool IsFinished()
    {
        return false;
    }

private:
    /** Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}. */
    StateObjectCollection<OperatorStateHandle> managedOperatorState;
    /**
     * Snapshot written using {@link
     * org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
     */
    StateObjectCollection<OperatorStateHandle> rawOperatorState;

    /** Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}. */
    StateObjectCollection<KeyedStateHandle> managedKeyedState;

    /**
     * Snapshot written using {@link
     * org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
     */
    StateObjectCollection<KeyedStateHandle> rawKeyedState;

    StateObjectCollection<InputChannelStateHandle> inputChannelState;

    StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState;

    std::shared_ptr<InflightDataRescalingDescriptor> inputRescalingDescriptor;

    std::shared_ptr<InflightDataRescalingDescriptor> outputRescalingDescriptor;

    /**
     * The state size. This is also part of the deserialized state handle. We store it here in order
     * to not deserialize the state handle when gathering stats.
     */
    long stateSize = 0;

    long checkpointedSize = 0;

    template <typename T>
    void DiscardStateHandles(
        const StateObjectCollection<T>& stateCollection,
        std::unordered_set<const void*>& discardedHandlesPtrs,
        bool deduplicate)
    {
        for (const auto& handle : stateCollection.AsList()) {
            if (!handle) {
                continue;
            }

            const void* handlePtr = static_cast<const void*>(handle.get());
            // skip if we have already discarded the underlying delegate
            if (deduplicate && !discardedHandlesPtrs.insert(handlePtr).second) {
                continue;
            }

            try {
                handle->DiscardState();
            } catch (const std::exception& e) {
                LOG("Exception while discarding state handle: " + std::string(e.what()));
            }
        }
    }
};
} // namespace omnistream
#endif // OMNISTREAM_OPERATORSUBTASKSTATE_H
