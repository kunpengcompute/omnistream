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

#ifndef OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H
#define OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>
#include <utility>

#include "BackendWritableBroadcastState.h"
#include "DefaultOperatorStateBackend.h"
#include "DefaultOperatorStateBackendSnapshotStrategy.h"
#include "OperatorStateHandle.h"
#include "PartitionableListState.h"
#include "OperatorStateRestoreOperation.h"

class DefaultOperatorStateBackendBuilder {
public:
    DefaultOperatorStateBackendBuilder(
        bool asynchronousSnapshots,
        std::string operatorIdentifier,
        std::vector<std::shared_ptr<OperatorStateHandle>> stateHandles,
        std::shared_ptr<TaskStateManagerBridge> bridge,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : asynchronousSnapshots_(asynchronousSnapshots),
          operatorIdentifier_(operatorIdentifier),
          restoreStateHandles_(stateHandles),
          bridge_(bridge),
          omniTaskBridge_(omniTaskBridge)
    {
    }

    OperatorStateBackend* build()
    {
        auto registeredOperatorStates = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto registeredBroadcastStates = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto accessedStatesByName = std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();
        auto accessedBroadcastStatesByName =
            std::make_shared<std::unordered_map<std::string, std::shared_ptr<State>>>();

        auto snapshotStrategy = std::make_shared<DefaultOperatorStateBackendSnapshotStrategy>(
            registeredOperatorStates, registeredBroadcastStates);

        auto restoreOperation = std::make_shared<OperatorStateRestoreOperation>(
            registeredOperatorStates, registeredBroadcastStates, restoreStateHandles_, omniTaskBridge_);
        try {
            restoreOperation->restore();
        } catch (const std::exception& e) {
            INFO_RELEASE("Error:Failed when trying to restore operator state backend" << e.what());
            throw std::runtime_error("Failed when trying to restore operator state backend" + std::string(e.what()));
        }

        return new DefaultOperatorStateBackend(
            asynchronousSnapshots_,
            bridge_,
            omniTaskBridge_,
            std::move(snapshotStrategy),
            std::move(registeredOperatorStates),
            std::move(registeredBroadcastStates),
            std::move(accessedStatesByName),
            std::move(accessedBroadcastStatesByName));
    }

protected:
    bool asynchronousSnapshots_;
    std::string operatorIdentifier_;
    std::vector<std::shared_ptr<OperatorStateHandle>> restoreStateHandles_;
    std::shared_ptr<TaskStateManagerBridge> bridge_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
};

#endif // OMNISTREAM_DEFAULTOPERATORSTATEBACKENDBUILDER_H
