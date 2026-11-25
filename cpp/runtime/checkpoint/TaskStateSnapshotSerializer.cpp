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

#include "TaskStateSnapshotSerializer.h"

#include <stdexcept>

nlohmann::json TaskStateSnapshotSerializer::Serialize(const std::shared_ptr<TaskStateSnapshot>& localState)
{
    nlohmann::json j;
    j["@class"] = "org.apache.flink.runtime.checkpoint.TaskStateSnapshot";
    // Serialize subtaskStatesByOperatorID map
    nlohmann::json subtaskStatesJson;
    const auto& subtaskStates = localState->GetSubtaskStatesByOperatorID();
    for (const auto& entry : subtaskStates) {
        const auto& operatorId = entry.first;
        const auto& subtaskState = entry.second;

        nlohmann::json subtaskStateJson;
        subtaskStateJson = parseOperatorStateAndKeyedState(subtaskStateJson, subtaskState);

        // inputChannelState empty?
        subtaskStateJson["inputChannelState"] = nlohmann::json::array(
            {"org.apache.flink.runtime.checkpoint.StateObjectCollection", nlohmann::json::array()});
        // resultSubpartitionState empty?
        subtaskStateJson["resultSubpartitionState"] = nlohmann::json::array(
            {"org.apache.flink.runtime.checkpoint.StateObjectCollection", nlohmann::json::array()});

        // inputRescalingDescriptor empty?
        subtaskStateJson["inputRescalingDescriptor"] =
            parseInflightDataRescalingDescriptor(subtaskState->getInputRescalingDescriptor());

        // outputRescalingDescriptor empty?
        subtaskStateJson["outputRescalingDescriptor"] =
            parseInflightDataRescalingDescriptor(subtaskState->getOutputRescalingDescriptor());

        // Add state sizes
        subtaskStateJson["stateSize"] = subtaskState->GetStateSize();
        subtaskStateJson["checkpointedSize"] = subtaskState->GetCheckpointedSize();
        subtaskStateJson["finished"] = subtaskState->IsFinished();

        // 这里operatorId bytesToHex?
        subtaskStatesJson[operatorId.toString()] = subtaskStateJson;
        // subtaskStatesJson[operatorId.getBytesToString()] = subtaskStateJson;
    }

    j["subtaskStatesByOperatorID"] =
        nlohmann::json::object({{"@class", "java.util.HashMap"}, {"mappings", subtaskStatesJson}});

    // Add boolean flags
    j["isTaskDeployedAsFinished"] = localState->GetIsTaskDeployedAsFinished();
    j["isTaskFinished"] = localState->GetIsTaskFinished();

    // Add rescaling descriptors (using same structure as subtask states)

    j["inputRescalingDescriptor"] = parseInflightDataRescalingDescriptor(localState->GetInputRescalingDescriptor());
    j["outputRescalingDescriptor"] = parseInflightDataRescalingDescriptor(localState->GetOutputRescalingDescriptor());

    // Add duplicated boolean flags (as seen in the example)
    j["taskDeployedAsFinished"] = localState->GetIsTaskDeployedAsFinished();
    j["taskFinished"] = localState->GetIsTaskFinished();

    // Add state sizes
    j["stateSize"] = localState->GetStateSize();
    j["checkpointedSize"] = localState->GetCheckpointedSize();

    return j;
}

nlohmann::json TaskStateSnapshotSerializer::parseOperatorStateAndKeyedState(
    nlohmann::json subtaskStateJson, std::shared_ptr<OperatorSubtaskState> operatorSubtaskState)
{
    subtaskStateJson["@class"] = "org.apache.flink.runtime.checkpoint.OperatorSubtaskState";
    // Serialize state collections
    subtaskStateJson["managedOperatorState"] = nlohmann::json::array(
        {"org.apache.flink.runtime.checkpoint.StateObjectCollection",
         parseOperatorState(operatorSubtaskState->getManagedOperatorState())});
    subtaskStateJson["rawOperatorState"] = nlohmann::json::array(
        {"org.apache.flink.runtime.checkpoint.StateObjectCollection",
         parseOperatorState(operatorSubtaskState->getRawOperatorState())});
    subtaskStateJson["managedKeyedState"] = nlohmann::json::array(
        {"org.apache.flink.runtime.checkpoint.StateObjectCollection",
         parseKeyedState(operatorSubtaskState->getManagedKeyedState())});
    subtaskStateJson["rawKeyedState"] = nlohmann::json::array(
        {"org.apache.flink.runtime.checkpoint.StateObjectCollection",
         parseKeyedState(operatorSubtaskState->getRawKeyedState())});

    return subtaskStateJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseKeyedState(StateObjectCollection<KeyedStateHandle> keyedState)
{
    nlohmann::json keyedStateArray = nlohmann::json::array();
    for (auto handle : keyedState.ToArray()) {
        nlohmann::json handleJson;
        if (auto kh = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(handle)) {
            keyedStateArray.push_back(parseIncrementalRemoteKeyedStateHandle(kh));
        } else if (auto kh = std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(handle)) {
            handleJson = parseKeyGroupsStateHandle(kh);
            handleJson["@class"] = "org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle";
            handleJson["stateHandleName"] = "KeyGroupsSavepointStateHandle";
            keyedStateArray.push_back(handleJson);
        } else if (auto kh = std::dynamic_pointer_cast<KeyGroupsStateHandle>(handle)) {
            keyedStateArray.push_back(parseKeyGroupsStateHandle(kh));
        } else if (auto kh = std::dynamic_pointer_cast<DirectoryKeyedStateHandle>(handle)) {
            handleJson["@class"] = "org.apache.flink.runtime.state.DirectoryKeyedStateHandle";
            handleJson["directoryStateHandle"] = parseDirectoryStateHandle(kh->getDirectoryStateHandle());
            handleJson["keyGroupRange"] = parseKeyGroupRange(kh->GetKeyGroupRange());
            keyedStateArray.push_back(handleJson);
        } else if (auto kh = std::dynamic_pointer_cast<IncrementalLocalKeyedStateHandle>(handle)) {
            keyedStateArray.push_back(parseIncrementalKeyedStateHandle(kh));
        } else {
            keyedStateArray = nlohmann::json::array();
        }
    }
    return keyedStateArray;
}

nlohmann::json TaskStateSnapshotSerializer::parseIncrementalKeyedStateHandle(
    std::shared_ptr<IncrementalLocalKeyedStateHandle> kh)
{
    nlohmann::json handleJson;
    // IncrementalLocalKeyedStateHandle 需要 directoryStateHandle
    handleJson["@class"] = "org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle";
    handleJson["directoryStateHandle"] = parseDirectoryStateHandle(kh->GetDirectoryStateHandle());
    handleJson["keyGroupRange"] = parseKeyGroupRange(kh->GetKeyGroupRange());
    handleJson["stateHandleId"] = parseStateHandleId(kh->GetStateHandleId());
    handleJson["checkpointId"] = kh->GetCheckpointId();
    handleJson["backendIdentifier"] = kh->GetBackendIdentifier().ToString();
    handleJson["metaDataState"] = parseMetaDataState(kh->GetMetaDataState());
    handleJson["sharedState"] = parseSharedState(kh->GetSharedStateHandles());
    handleJson["sharedStateHandles"] = parseSharedState(kh->GetSharedStateHandles());
    handleJson["stateSize"] = kh->GetStateSize();
    handleJson["checkpointedSize"] = kh->GetCheckpointedSize();
    return handleJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseIncrementalRemoteKeyedStateHandle(
    std::shared_ptr<IncrementalRemoteKeyedStateHandle> kh)
{
    nlohmann::json handleJson;
    handleJson["@class"] = "org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle";
    handleJson["keyGroupRange"] = parseKeyGroupRange(kh->GetKeyGroupRange());
    handleJson["stateHandleId"] = parseStateHandleId(kh->GetStateHandleId());
    handleJson["checkpointId"] = kh->GetCheckpointId();
    handleJson["backendIdentifier"] = kh->GetBackendIdentifier().ToString();
    handleJson["metaStateHandle"] = parseMetaDataState(kh->GetMetaDataStateHandle());
    // Keep the old OmniStream spelling for compatibility with existing restore JSONs.
    handleJson["metaDataState"] = handleJson["metaStateHandle"];
    handleJson["sharedState"] = parseSharedState(kh->GetSharedStateHandles());
    handleJson["privateState"] = parseSharedState(kh->GetPrivateState());
    handleJson["persistedSizeOfThisCheckpoint"] = kh->GetCheckpointedSize();
    handleJson["checkpointedSize"] = kh->GetCheckpointedSize();
    handleJson["stateSize"] = kh->GetStateSize();
    return handleJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseKeyGroupsStateHandle(std::shared_ptr<KeyGroupsStateHandle> kh)
{
    nlohmann::json handleJson;
    const bool isSavepointHandle = std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(kh) != nullptr;
    handleJson["@class"] = isSavepointHandle ? "org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle"
                                             : "org.apache.flink.runtime.state.KeyGroupsStateHandle";
    handleJson["stateHandleName"] = isSavepointHandle ? "KeyGroupsSavepointStateHandle" : "KeyGroupsStateHandle";
    handleJson["keyGroupRange"] = parseKeyGroupRange(kh->GetKeyGroupRange());
    handleJson["groupRangeOffsets"] = nlohmann::json::parse(kh->getGroupRangeOffsets().ToString());
    handleJson["streamStateHandle"] = parseMetaDataState(kh->getDelegateStateHandle());
    // Some old C++ paths expect the delegate under stateHandle/metaDataState.
    handleJson["stateHandle"] = handleJson["streamStateHandle"];
    handleJson["metaDataState"] = handleJson["streamStateHandle"];
    handleJson["stateHandleId"] = parseStateHandleId(kh->GetStateHandleId());
    handleJson["stateSize"] = kh->GetStateSize();
    handleJson["checkpointedSize"] = kh->GetCheckpointedSize();
    return handleJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseOperatorStreamStateHandle(
    std::shared_ptr<OperatorStreamStateHandle> kh)
{
    nlohmann::json handleJson;
    handleJson["@class"] = "org.apache.flink.runtime.state.OperatorStreamStateHandle";
    nlohmann::json delegateStateHandle = parseMetaDataState(kh->getDelegateStateHandle());
    handleJson["metaDataState"] = delegateStateHandle;
    handleJson["delegateStateHandle"] = delegateStateHandle;
    handleJson["streamStateHandle"] = delegateStateHandle;
    nlohmann::json stateNameToPartitionOffsets = kh->toJson();
    handleJson["stateNameToPartitionOffsets"] = stateNameToPartitionOffsets;
    return handleJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseMetaDataState(std::shared_ptr<StreamStateHandle> metaDataStateHandle)
{
    nlohmann::json metaDataStateHandleJson;

    if (metaDataStateHandle == nullptr) {
        throw std::runtime_error("OperatorStreamStateHandle delegate is null.");
    }
    if (auto msh = std::dynamic_pointer_cast<RelativeFileStateHandle>(metaDataStateHandle)) {
        metaDataStateHandleJson["@class"] = "org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle";
        metaDataStateHandleJson["stateHandleName"] = "RelativeFileStateHandle";
        metaDataStateHandleJson["stateSize"] = msh->GetStateSize();
        metaDataStateHandleJson["filePath"] = msh->GetFilePath().toString();
        metaDataStateHandleJson["relativePath"] = msh->GetRelativePath();
        metaDataStateHandleJson["streamStateHandleID"] = parseStreamStateHandleID(msh->GetStreamStateHandleID());
    } else if (auto msh = std::dynamic_pointer_cast<FileStateHandle>(metaDataStateHandle)) {
        metaDataStateHandleJson["@class"] = "org.apache.flink.runtime.state.filesystem.FileStateHandle";
        metaDataStateHandleJson["stateHandleName"] = "FileStateHandle";
        metaDataStateHandleJson["stateSize"] = msh->GetStateSize();
        metaDataStateHandleJson["filePath"] = msh->GetFilePath().toString();
        metaDataStateHandleJson["streamStateHandleID"] = parseStreamStateHandleID(msh->GetStreamStateHandleID());
    } else if (auto msh = std::dynamic_pointer_cast<ByteStreamStateHandle>(metaDataStateHandle)) {
        metaDataStateHandleJson["@class"] = "org.apache.flink.runtime.state.memory.ByteStreamStateHandle";
        metaDataStateHandleJson["stateHandleName"] = "ByteStreamStateHandle";
        metaDataStateHandleJson["handleName"] = msh->GetHandleName();
        auto jobj = nlohmann::json::parse(msh->ToString());
        metaDataStateHandleJson["data"] = jobj["data"];
        metaDataStateHandleJson["stateSize"] = msh->GetStateSize();
        metaDataStateHandleJson["streamStateHandleID"] = parseStreamStateHandleID(msh->GetStreamStateHandleID());
    } else if (auto msh = std::dynamic_pointer_cast<PlaceholderStreamStateHandle>(metaDataStateHandle)) {
        metaDataStateHandleJson["@class"] = "org.apache.flink.runtime.state.PlaceholderStreamStateHandle";
        metaDataStateHandleJson["stateHandleName"] = "PlaceholderStreamStateHandle";
        metaDataStateHandleJson["stateSize"] = msh->GetStateSize();
        nlohmann::json physicalStateHandleIDJson;
        physicalStateHandleIDJson["@class"] = "org.apache.flink.runtime.state.PhysicalStateHandleID";
        physicalStateHandleIDJson["keyString"] = msh->GetStreamStateHandleIDKeyString();
        metaDataStateHandleJson["physicalID"] = physicalStateHandleIDJson;
    } else {
        throw std::runtime_error("Unknown metaDataStateHandle class.");
    }
    return metaDataStateHandleJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseDirectoryStateHandle(DirectoryStateHandle* directoryStateHandle)
{
    nlohmann::json directoryStateHandleJson;
    nlohmann::json directoryJson;

    directoryStateHandleJson["@class"] = "org.apache.flink.runtime.state.DirectoryStateHandle";
    directoryStateHandleJson["directoryString"] = directoryStateHandle->getDirectory().toString();

    // 需要FileSystem的scheme: file://,hdfs://,viewfs://等?
    directoryJson = nlohmann::json::array({"java.nio.file.Path", directoryStateHandle->getDirectory().toString()});
    directoryStateHandleJson["directory"] = directoryJson;
    directoryStateHandleJson["stateSize"] = directoryStateHandle->GetStateSize();

    return directoryStateHandleJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseKeyGroupRange(KeyGroupRange keyGroupRange)
{
    nlohmann::json keyGroupRangeJson;

    keyGroupRangeJson["@class"] = "org.apache.flink.runtime.state.KeyGroupRange";
    keyGroupRangeJson["startKeyGroup"] = keyGroupRange.getStartKeyGroup();
    keyGroupRangeJson["endKeyGroup"] = keyGroupRange.getEndKeyGroup();
    keyGroupRangeJson["numberOfKeyGroups"] = keyGroupRange.getNumberOfKeyGroups();

    return keyGroupRangeJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseStateHandleId(StateHandleID stateHandleID)
{
    nlohmann::json stateHandleIDJson;

    stateHandleIDJson["@class"] = "org.apache.flink.runtime.state.StateHandleID";
    stateHandleIDJson["keyString"] = stateHandleID.getKeyString();

    return stateHandleIDJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseStreamStateHandleID(PhysicalStateHandleID physicalStateHandleID)
{
    nlohmann::json physicalStateHandleIDJson;

    physicalStateHandleIDJson["@class"] = "org.apache.flink.runtime.state.PhysicalStateHandleID";
    physicalStateHandleIDJson["keyString"] = physicalStateHandleID.getKeyString();

    return physicalStateHandleIDJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseSharedState(
    std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath> handleAndLocalPaths)
{
    nlohmann::json handleAndLocalPathsJson = nlohmann::json::array();

    for (const auto& handleAndLocalPath : handleAndLocalPaths) {
        nlohmann::json handleJson;
        handleJson["handle"] = parseMetaDataState(handleAndLocalPath.getHandle());
        handleJson["localPath"] = handleAndLocalPath.getLocalPath();
        handleAndLocalPathsJson.push_back(handleJson);
    }

    nlohmann::json sharedStateJson = nlohmann::json::array({"java.util.ArrayList", handleAndLocalPathsJson});
    return sharedStateJson;
}

nlohmann::json TaskStateSnapshotSerializer::parseInflightDataRescalingDescriptor(
    const std::shared_ptr<InflightDataRescalingDescriptor> rescalingDescriptor)
{
    // TTODO: Now we assume it is empty
    nlohmann::json inputRescalingDesc;
    inputRescalingDesc["@class"] = "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor"
                                   "$NoRescalingDescriptor";
    inputRescalingDesc["gateOrPartitionDescriptors"] = nlohmann::json::array(
        {"[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor"
         "$InflightDataGateOrPartitionRescalingDescriptor;",
         nlohmann::json::array()});
    return inputRescalingDesc;
}
