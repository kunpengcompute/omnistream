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

#ifndef OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H
#define OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <stdexcept>
#include <future>
#include <typeinfo>
#include <utility>

#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/ListState.h"
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "runtime/state/SnapshotExecutionType.h"
#include "runtime/state/bridge/TaskStateManagerBridge.h"
#include "runtime/state/bridge/OmniTaskBridge.h"

#include "HeapBroadcastState.h"
#include "PartitionableListState.h"
#include "CheckpointStreamFactory.h"
#include "DefaultOperatorStateBackendSnapshotStrategy.h"
#include "OperatorStateBackend.h"
#include "OperatorStateHandle.h"
#include "RegisteredBroadcastStateBackendMetaInfo.h"
#include "RegisteredOperatorStateBackendMetaInfo.h"
#include "SnapshotStrategyRunner.h"
#include "SnapshotResult.h"

class DefaultOperatorStateBackend : public OperatorStateBackend {
public:
    DefaultOperatorStateBackend(
        bool asynchronousSnapshots,
        std::shared_ptr<TaskStateManagerBridge> bridge,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge,
        std::shared_ptr<DefaultOperatorStateBackendSnapshotStrategy> snapshotStrategy,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedStatesByName,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedBroadcastStatesByName)
        : asynchronousSnapshots_(asynchronousSnapshots),
          bridge_(std::move(bridge)),
          omniTaskBridge_(std::move(omniTaskBridge)),
          snapshotStrategy_(std::move(snapshotStrategy)),
          registeredOperatorStates_(std::move(registeredOperatorStates)),
          registeredBroadcastStates_(std::move(registeredBroadcastStates)),
          accessedStatesByName_(std::move(accessedStatesByName)),
          accessedBroadcastStatesByName_(std::move(accessedBroadcastStatesByName))
    {
    }

    std::unordered_set<std::string> getRegisteredStateNames() override
    {
        std::unordered_set<std::string> nameSet;
        for (const auto& pair : *registeredOperatorStates_) {
            nameSet.insert(pair.first);
        }
        return nameSet;
    }

    std::unordered_set<std::string> getRegisteredBroadcastStateNames() override
    {
        std::unordered_set<std::string> nameSet;
        for (const auto& pair : *registeredBroadcastStates_) {
            nameSet.insert(pair.first);
        }
        return nameSet;
    }

    void close()
    {
    }

    void dispose() override
    {
        LOG(" DefaultOperatorStateBackend dispose");
        if (!registeredOperatorStates_->empty()) {
            registeredOperatorStates_->clear();
        }
        if (!registeredBroadcastStates_->empty()) {
            registeredBroadcastStates_->clear();
        }
        if (!accessedStatesByName_->empty()) {
            accessedStatesByName_->clear();
        }
        if (!accessedBroadcastStatesByName_->empty()) {
            accessedBroadcastStatesByName_->clear();
        }
    }

    template <typename K, typename V>
    std::shared_ptr<HeapBroadcastState<K, V>> getBroadcastState(MapStateDescriptor<K, V>* stateDescriptor)
    {
        std::string name = stateDescriptor->getName();
        auto accessedIterator = accessedBroadcastStatesByName_->find(name);
        if (accessedIterator != accessedBroadcastStatesByName_->end()) {
            std::shared_ptr<HeapBroadcastState<K, V>> state =
                std::dynamic_pointer_cast<HeapBroadcastState<K, V>>(accessedIterator->second);
            return std::dynamic_pointer_cast<HeapBroadcastState<K, V>>(accessedIterator->second);
        }

        std::shared_ptr<HeapBroadcastState<K, V>> resultState = nullptr;
        auto* broadcastStateKeySerializer = stateDescriptor->GetUserKeySerializer();
        auto* broadcastStateValueSerializer = stateDescriptor->GetValueSerializer();

        auto registeredIterator = registeredBroadcastStates_->find(name);

        if (registeredIterator == registeredBroadcastStates_->end()) {
            auto stateMetaInfo = std::make_shared<RegisteredBroadcastStateBackendMetaInfo>(
                name, OperatorStateHandle::Mode::BROADCAST, broadcastStateKeySerializer, broadcastStateValueSerializer);
            auto internalMap = std::shared_ptr<std::map<K, V>>();
            resultState = std::make_shared<HeapBroadcastState<K, V>>(stateMetaInfo, internalMap);
            registeredBroadcastStates_->emplace(name, resultState);
        } else {
            resultState = std::dynamic_pointer_cast<HeapBroadcastState<K, V>>(registeredIterator->second);
            auto stateMetaInfo = resultState->getStateMetaInfo();
            stateMetaInfo->updateKeySerializer(broadcastStateKeySerializer);
            stateMetaInfo->updateValueSerializer(broadcastStateValueSerializer);
            resultState->setStateMetaInfo(stateMetaInfo);
        }

        accessedBroadcastStatesByName_->emplace(name, (*registeredBroadcastStates_)[name]);

        return resultState;
    }

    template <typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor)
    {
        return getListState(stateDescriptor, OperatorStateHandle::Mode::SPLIT_DISTRIBUTE);
    }

    template <typename S>
    std::shared_ptr<ListState<S>> getUnionListState(ListStateDescriptor<S>* stateDescriptor)
    {
        return getListState(stateDescriptor, OperatorStateHandle::Mode::UNION);
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<OperatorStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        std::shared_ptr<CheckpointStreamFactory> streamFactory,
        CheckpointOptions* checkpointOptions) override
    {
        if (registeredOperatorStates_->empty() && registeredBroadcastStates_->empty()) {
            return nullptr;
        }

        auto snapshotStrategyRunner = std::make_unique<SnapshotStrategyRunner<OperatorStateHandle, SnapshotResources>>(
            "DefaultOperatorStateBackend snapshot",
            snapshotStrategy_,
            asynchronousSnapshots_ ? ASYNCHRONOUS : SYNCHRONOUS);

        return snapshotStrategyRunner->snapshot(
            checkpointId, timestamp, streamFactory, checkpointOptions, omniTaskBridge_, "");
    }

private:
    bool asynchronousSnapshots_;
    std::shared_ptr<TaskStateManagerBridge> bridge_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    std::shared_ptr<DefaultOperatorStateBackendSnapshotStrategy> snapshotStrategy_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStates_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedStatesByName_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> accessedBroadcastStatesByName_;

    template <typename S>
    std::shared_ptr<ListState<S>> getListState(ListStateDescriptor<S>* stateDescriptor, OperatorStateHandle::Mode mode)
    {
        std::string name = stateDescriptor->getName();

        auto accessedIterator = accessedStatesByName_->find(name);
        if (accessedIterator != accessedStatesByName_->end()) {
            auto state = std::dynamic_pointer_cast<PartitionableListState<S>>(accessedIterator->second);
            return std::dynamic_pointer_cast<PartitionableListState<S>>(accessedIterator->second);
        }

        std::shared_ptr<PartitionableListState<S>> resultState = nullptr;
        auto* operatorStateSerializer = stateDescriptor->getStateSerializer();
        auto registeredIterator = registeredOperatorStates_->find(name);
        if (registeredIterator == registeredOperatorStates_->end()) {
            auto stateMetaInfo =
                std::make_shared<RegisteredOperatorStateBackendMetaInfo>(name, mode, operatorStateSerializer);
            auto internalList = std::make_shared<std::vector<S>>();
            resultState = std::make_shared<PartitionableListState<S>>(stateMetaInfo, internalList);
            registeredOperatorStates_->emplace(name, resultState);
        } else {
            resultState = std::dynamic_pointer_cast<PartitionableListState<S>>(registeredIterator->second);
            auto stateMetaInfo = resultState->getStateMetaInfo();
            stateMetaInfo->updateStateSerializer(operatorStateSerializer);
            resultState->setStateMetaInfo(stateMetaInfo);
        }

        accessedStatesByName_->emplace(name, (*registeredOperatorStates_)[name]);
        return resultState;
    }
};

#endif // OMNISTREAM_DEFAULTOPERATORSTATEBACKEND_H
