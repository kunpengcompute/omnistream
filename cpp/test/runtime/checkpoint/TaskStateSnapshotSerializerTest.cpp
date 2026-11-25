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

#include <gtest/gtest.h>
#include <string>
#include <memory>

#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"

TEST(TaskStateSnapshotSerializerTest, SerializerToJson)
{
    std::unordered_map<OperatorID, std::shared_ptr<OperatorSubtaskState>> subtaskStatesByOperatorID;

    auto managedOperatorState = StateObjectCollection<OperatorStateHandle>::Empty();
    auto rawOperatorState = StateObjectCollection<OperatorStateHandle>::Empty();
    auto rawKeyedState = StateObjectCollection<KeyedStateHandle>::Empty();
    auto managedKeyedState = StateObjectCollection<KeyedStateHandle>::Empty();

    auto inputChannelState = StateObjectCollection<InputChannelStateHandle>::Empty();
    auto resultSubpartitionState = StateObjectCollection<ResultSubpartitionStateHandle>::Empty();
    auto inputRescalingDescriptor = InflightDataRescalingDescriptor::noRescale;
    auto outputRescalingDescriptor = InflightDataRescalingDescriptor::noRescale;

    std::string dirName = (std::filesystem::temp_directory_path() / "IncLocalKeyedHandleTest").string();
    std::string fileName = "metaState.bin";
    std::string content = "TestFileForIncLocalKeyedHandle";
    std::filesystem::remove_all(dirName);
    std::filesystem::create_directories(dirName);
    std::ofstream ofs(dirName + "/" + fileName, std::ios::binary);
    ofs << content;
    ofs.close();

    Path path(dirName);
    DirectoryStateHandle originalDirHandle = DirectoryStateHandle::forPathWithSize(path);
    long expectedDirSize = originalDirHandle.GetStateSize();

    std::string metaFilePath = dirName + "/" + fileName;
    Path metaPath(metaFilePath);
    long metaStateSize = std::filesystem::file_size(metaFilePath);

    nlohmann::json metaDataState = {
        {"@class", "org.apache.flink.runtime.state.filesystem.FileStateHandle"},
        {"stateHandleName", "FileStateHandle"},
        {"streamStateHandleID",
         {{"@class", "org.apache.flink.runtime.state.PhysicalStateHandleID"}, {"keyString", metaPath.toString()}}},
        {"stateSize", metaStateSize}};

    nlohmann::json jsonObj;
    jsonObj["checkpointId"] = 1234;
    jsonObj["backendIdentifier"] = "01234567-89ab-cdef-0123-456789abcdef";
    jsonObj["metaDataState"] = metaDataState;
    jsonObj["directoryStateHandle"] = {
        {"@class", "org.apache.flink.runtime.state.DirectoryStateHandle"},
        {"directoryString", path.toString()},
        {"stateSize", expectedDirSize},
        {"directory", "file://" + path.toString() + "/"},
    };
    jsonObj["keyGroupRange"] = {{"startKeyGroup", 0}, {"endKeyGroup", 9}};

    nlohmann::json sharedHandle1 = {
        {"handle",
         {{"@class", "org.apache.flink.runtime.state.filesystem.FileStateHandle"},
          {"stateHandleName", "FileStateHandle"},
          {"filePath", metaPath.toString()},
          {"stateSize", metaStateSize}}},
        {"localPath", "/local/path/1"}};
    nlohmann::json sharedHandle2 = {{"handle", nullptr}, {"localPath", "/local/path/2"}};
    jsonObj["sharedState"] = nlohmann::json::array({sharedHandle1, sharedHandle2});

    auto handle = std::make_shared<IncrementalLocalKeyedStateHandle>(jsonObj);
    managedKeyedState->Add(handle);

    auto operatorSubtaskState = std::make_shared<OperatorSubtaskState>(
        *managedOperatorState,
        *rawOperatorState,
        *managedKeyedState,
        *rawKeyedState,
        *inputChannelState,
        *resultSubpartitionState,
        inputRescalingDescriptor,
        outputRescalingDescriptor);

    subtaskStatesByOperatorID[OperatorID()] = operatorSubtaskState;

    auto taskStateSnapshot = std::make_shared<TaskStateSnapshot>(subtaskStatesByOperatorID, false, false);

    auto res = TaskStateSnapshotSerializer::Serialize(taskStateSnapshot);

    std::cout << to_string(res) << std::endl;
}
