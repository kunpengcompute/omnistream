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

#ifndef OMNISTREAM_SHAREDSTATEREGISTRY_H
#define OMNISTREAM_SHAREDSTATEREGISTRY_H

#include <memory>
#include <functional>
#include <vector>
#include <set>
#include <exception>
#include "CompositeStateHandle.h"
#include "runtime/checkpoint/CompletedCheckpoint.h"
#include "runtime/state/StreamStateHandle.h"
enum RestoreMode {
    CLAIM,
    NO_CLAIM,
    LEGACY
};
// This is an interface
class SharedStateRegistry {
public:
    virtual std::shared_ptr<StreamStateHandle> RegisterReference(
        const std::string& registrationKey, std::shared_ptr<StreamStateHandle> state, long checkpointID)
    {
        return RegisterReference(registrationKey, state, checkpointID, false);
    }

    virtual std::shared_ptr<StreamStateHandle> RegisterReference(
        const std::string& registrationKey,
        std::shared_ptr<StreamStateHandle> state,
        long checkpointID,
        bool preventDiscardingCreatedCheckpoint) = 0;

    virtual std::set<long> unregisterUnusedState(long lowestCheckpointID) = 0;

    template <typename IterableType>
    void RegisterAll(const IterableType& stateHandles, long checkpointID)
    {
        RegisterAllImpl(stateHandles, checkpointID);
    }

    virtual void RegisterAllAfterRestored(std::shared_ptr<CompletedCheckpoint> checkpoint, RestoreMode mode) = 0;

    virtual void CheckpointCompleted(long checkpointId) = 0;

    virtual ~SharedStateRegistry() = default;

protected:
    // Implementation method for the template registerAll method
    virtual void RegisterAllImpl(
        const std::vector<std::shared_ptr<CompositeStateHandle>>& stateHandles, long checkpointID) = 0;

    template <typename IterableType>
    void RegisterAllImpl(const IterableType& stateHandles, long checkpointID)
    {
        std::vector<std::shared_ptr<CompositeStateHandle>> handleVector;
        for (const auto& handle : stateHandles) {
            handleVector.push_back(handle);
        }
        RegisterAllImpl(handleVector, checkpointID);
    }
};

#endif // OMNISTREAM_SHAREDSTATEREGISTRY_H
