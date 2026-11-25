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

#ifndef OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTRESOURCES_H
#define OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTRESOURCES_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "core/include/common.h"

#include "HeapBroadcastState.h"
#include "PartitionableListState.h"
#include "SnapshotResources.h"

class DefaultOperatorStateBackendSnapshotResources : public SnapshotResources {
public:
    DefaultOperatorStateBackendSnapshotResources(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStatesDeepCopies,
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStatesDeepCopies,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> operatorStateMetaInfoSnapshots,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> broadcastStateMetaInfoSnapshots)
        : registeredOperatorStatesDeepCopies_(std::move(registeredOperatorStatesDeepCopies)),
          registeredBroadcastStatesDeepCopies_(std::move(registeredBroadcastStatesDeepCopies)),
          operatorStateMetaInfoSnapshots_(std::move(operatorStateMetaInfoSnapshots)),
          broadcastStateMetaInfoSnapshots_(std::move(broadcastStateMetaInfoSnapshots))
    {
    }

    void cleanup()
    {
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> getRegisteredOperatorStatesDeepCopies()
    {
        return registeredOperatorStatesDeepCopies_;
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> getRegisteredBroadcastStatesDeepCopies()
    {
        return registeredBroadcastStatesDeepCopies_;
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> getOperatorStateMetaInfoSnapshots()
    {
        return operatorStateMetaInfoSnapshots_;
    }

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> getBroadcastStateMetaInfoSnapshots()
    {
        return broadcastStateMetaInfoSnapshots_;
    }

private:
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredOperatorStatesDeepCopies_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<State>>> registeredBroadcastStatesDeepCopies_;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> operatorStateMetaInfoSnapshots_;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> broadcastStateMetaInfoSnapshots_;
};

#endif // OMNISTREAM_DEFAULTOPERATORSTATEBACKENDSNAPSHOTRESOURCES_H
