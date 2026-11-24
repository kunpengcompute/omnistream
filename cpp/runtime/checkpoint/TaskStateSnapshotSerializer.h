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

#ifndef OMNISTREAM_TASKSTATESNAPSHOTSERIALIZER_H
#define OMNISTREAM_TASKSTATESNAPSHOTSERIALIZER_H

#include <nlohmann/json.hpp>
#include <memory>
#include "TaskStateSnapshot.h"
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"
#include "runtime/state/DirectoryKeyedStateHandle.h"
#include "runtime/state/IncrementalLocalKeyedStateHandle.h"
#include "runtime/state/filesystem/FileStateHandle.h"
#include "runtime/checkpoint/InflightDataRescalingDescriptor.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/OperatorStreamStateHandle.h"

class TaskStateSnapshotSerializer {
public:
    static nlohmann::json Serialize(const std::shared_ptr<TaskStateSnapshot>& localState);

    static nlohmann::json parseOperatorStateAndKeyedState(
        nlohmann::json subtaskStateJson, std::shared_ptr<OperatorSubtaskState> operatorSubtaskState);

    static nlohmann::json parseOperatorState(StateObjectCollection<OperatorStateHandle> operatorState)
    {
        // OperatorStateHandle empty?
        return nlohmann::json::array();
    }

    static nlohmann::json parseKeyedState(StateObjectCollection<KeyedStateHandle> keyedState);

    static nlohmann::json parseIncrementalKeyedStateHandle(std::shared_ptr<IncrementalLocalKeyedStateHandle> kh);

    static nlohmann::json parseIncrementalRemoteKeyedStateHandle(std::shared_ptr<IncrementalRemoteKeyedStateHandle> kh);

    static nlohmann::json parseKeyGroupsStateHandle(std::shared_ptr<KeyGroupsStateHandle> kh);

    static nlohmann::json parseOperatorStreamStateHandle(std::shared_ptr<OperatorStreamStateHandle> kh);

    static nlohmann::json parseKeyGroupRange(KeyGroupRange keyGroupRange);

    static nlohmann::json parseStateHandleId(StateHandleID stateHandleID);

    static nlohmann::json parseMetaDataState(std::shared_ptr<StreamStateHandle> metaDataStateHandle);

    static nlohmann::json parseStreamStateHandleID(PhysicalStateHandleID physicalStateHandleID);

    static nlohmann::json parseSharedState(
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> handleAndLocalPaths);

    static nlohmann::json parseDirectoryStateHandle(DirectoryStateHandle* directoryStateHandle);

    static nlohmann::json parseInflightDataRescalingDescriptor(
        const std::shared_ptr<InflightDataRescalingDescriptor> rescalingDescriptor);
};

#endif // OMNISTREAM_TASKSTATESNAPSHOTSERIALIZER_H
