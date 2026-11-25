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
#include <vector>

#include "runtime/checkpoint/TaskStateSnapshotDeserializer.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"

TEST(SerializerTestSuite, PreservesKeyGroupsSavepointStateHandleType)
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange, std::vector<int64_t>{0});
    auto streamHandle = std::make_shared<ByteStreamStateHandle>("savepoint-handle", std::vector<uint8_t>{});
    auto savepointHandle = std::make_shared<KeyGroupsSavepointStateHandle>(offsets, streamHandle);

    nlohmann::json handleJson = TaskStateSnapshotSerializer::parseKeyGroupsStateHandle(savepointHandle);

    EXPECT_EQ(handleJson["@class"].get<std::string>(), "org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle");
    EXPECT_EQ(handleJson["stateHandleName"].get<std::string>(), "KeyGroupsSavepointStateHandle");
}

TEST(DeserializerTestSuite, DeserializesLocalStateSnapshotFromFile)
{
    const std::string json_content = R"({
        "@class": "org.apache.flink.runtime.checkpoint.TaskStateSnapshot",
        "subtaskStatesByOperatorID": {
            "@class": "java.util.HashMap",
            "ccb29b5204e83e8a588b3828afaa7015": {
                "@class": "org.apache.flink.runtime.checkpoint.OperatorSubtaskState",
                "managedOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "rawOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "managedKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "rawKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputChannelState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "resultSubpartitionState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "outputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "stateSize": 0, "checkpointedSize": 0, "finished": false
            },
            "4bf7c1955ffe56e2106d666433eaf137": {
                "@class": "org.apache.flink.runtime.checkpoint.OperatorSubtaskState",
                "managedOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "rawOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "managedKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", [{
                    "@class": "org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle",
                    "directoryStateHandle": {
                        "@class": "org.apache.flink.runtime.state.DirectoryStateHandle",
                        "directoryString": "/home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433eaf137_sti_0/chk_11/8d43f9058fcb491d95df87e4597464e4",
                        "directory": ["java.nio.file.Path", "file:///home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433eaf137_sti_0/chk_11/8d43f9058fcb491d95df87e4597464e4/"],
                        "stateSize": 0
                    },
                    "keyGroupRange": {"@class": "org.apache.flink.runtime.state.KeyGroupRange", "startKeyGroup": 0, "endKeyGroup": 127, "numberOfKeyGroups": 128},
                    "stateHandleId": {"@class": "org.apache.flink.runtime.state.StateHandleID", "keyString": "b1e89516-e1b3-459d-8073-b986e63cfb6a"},
                    "checkpointId": 11,
                    "backendIdentifier": "8d43f905-8fcb-491d-95df-87e4597464e4",
                    "metaDataState": {"@class": "org.apache.flink.runtime.state.filesystem.FileStateHandle", "stateSize": 81584, "streamStateHandleID": {"@class": "org.apache.flink.runtime.state.PhysicalStateHandleID", "keyString": "file:/home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433eaf137_sti_0/chk_11/e5e08ea2-de9c-4a46-a338-73e661d67186"}},
                    "sharedState": ["java.util.ArrayList", []], "sharedStateHandles": ["java.util.ArrayList", []],
                    "stateSize": 81584, "checkpointedSize": 81584
                }]],
                "rawKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputChannelState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "resultSubpartitionState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "outputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "stateSize": 81584, "checkpointedSize": 81584, "finished": false
            }
        },
        "isTaskDeployedAsFinished": false, "isTaskFinished": false,
        "inputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
        "outputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
        "taskDeployedAsFinished": false, "taskFinished": false,
        "stateSize": 81584, "checkpointedSize": 81584
    })";

    auto snapshot = TaskStateSnapshotDeserializer::Deserialize(json_content);
    EXPECT_FALSE(snapshot->GetIsTaskFinished());
    EXPECT_EQ(snapshot->GetStateSize(), 81584);

    const auto& subtask_states = snapshot->GetSubtaskStateMappings();

    ASSERT_EQ(subtask_states.size(), 2);

    OperatorID opIdWithState =
        TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>("4bf7c1955ffe56e2106d666433eaf137");
    bool found = false;
    std::shared_ptr<OperatorSubtaskState> state;
    for (const auto& pair : subtask_states) {
        if (pair.first == opIdWithState) {
            found = true;
            state = pair.second;
        }
    }
    ASSERT_TRUE(found);
    const auto& keyedStateHandles = state->GetManagedKeyedState();
    ASSERT_EQ(keyedStateHandles.Size(), 1);

    auto localHandle = std::dynamic_pointer_cast<IncrementalLocalKeyedStateHandle>(keyedStateHandles.ToArray().at(0));
    ASSERT_NE(localHandle, nullptr);

    const auto& dirHandle = localHandle->getDirectoryStateHandle(); // Assuming getter
    ASSERT_NE(dirHandle, nullptr);

    const std::string expected_dir =
        "/home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/"
        "jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433eaf137_sti_0/chk_11/"
        "8d43f9058fcb491d95df87e4597464e4";
    EXPECT_EQ(dirHandle->getDirectory().toString(), expected_dir);

    std::string to_string_output = snapshot->ToString();
    std::cout << "Global: " << to_string_output << std::endl;
    nlohmann::json parsed_json;
    ASSERT_NO_THROW({ parsed_json = nlohmann::json::parse(to_string_output); })
        << "The output of ToString() is not valid JSON. Output was: " << to_string_output;

    EXPECT_EQ(parsed_json["stateHandleName"], "TaskStateSnapshot");
    std::string opIdHex = "4bf7c1955ffe56e2106d666433eaf137";
    ASSERT_TRUE(parsed_json["subtaskStatesByOperatorID"].contains(opIdHex));
}

TEST(DeserializerTestSuite, DeserializesRemoteStateSnapshotFromFile)
{
    const std::string json_content = R"({
        "@class": "org.apache.flink.runtime.checkpoint.TaskStateSnapshot",
        "subtaskStatesByOperatorID": {
            "@class": "java.util.HashMap",
            "ccb29b5204e83e8a588b3828afaa7015": {
                "@class": "org.apache.flink.runtime.checkpoint.OperatorSubtaskState",
                "managedOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []], "rawOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "managedKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []], "rawKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputChannelState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []], "resultSubpartitionState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "outputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "stateSize": 0, "checkpointedSize": 0, "finished": false
            },
            "4bf7c1955ffe56e2106d666433eaf137": {
                "@class": "org.apache.flink.runtime.checkpoint.OperatorSubtaskState",
                "managedOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []], "rawOperatorState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "managedKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", [{
                    "@class": "org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle",
                    "backendIdentifier": "8d43f905-8fcb-491d-95df-87e4597464e4",
                    "keyGroupRange": {"@class": "org.apache.flink.runtime.state.KeyGroupRange", "startKeyGroup": 0, "endKeyGroup": 127, "numberOfKeyGroups": 128},
                    "checkpointId": 1,
                    "sharedState": ["java.util.Collections$EmptyList", []],
                    "privateState": ["java.util.ArrayList", [
                        {
                            "handle": {"@class": "org.apache.flink.runtime.state.memory.ByteStreamStateHandle", "data": "Vvm4+BwAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3KYr1imAgABAgBRgcd4BgABCQADBQQARtLoITEAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMIBADIAQHJAQlMZWZ0Q2FjaGU6cy3AMgABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgIFAwoEAMgBAskBClJpZ2h0Q2FjaGXvtbdrNwABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgIFAwwEAMgBA8kBD0VhcmxpZXN0RWxlbWVudLD4/3k6AAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yAgUDDgQAyAEEyQESTGVmdENhY2hlQ2xlYW5UaW1lD5BHZzsAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMQBADIAQXJARNSaWdodENhY2hlQ2xlYW5UaW1lmNaHPDgAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMSBADIAQbJARBMZWZ0RHVwbGljYXRlUmNk0eMQ3DkAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMUBADIAQfJARFSaWdodER1cGxpY2F0ZVJjZBtIAPJDAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yAgUDFgQAyAEIyQEbTGVmdER1cGxpY2F0ZVJjZENsZWFuZXJUaW1lFa5C0EQAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMYBADIAQnJARxSaWdodER1cGxpY2F0ZVJjZENsZWFuZXJUaW1lYndGHTkAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMaBADIAQrJARFMZWZ0RGF0YVRvdGFsU2l6ZZqvcUZLAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yAgUDHAQAyAELyQEjX3RpbWVyX3N0YXRlL3Byb2Nlc3NpbmdfdXNlci10aW1lcnMatVTLRgABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgIFAx4EAMgBDMkBHl90aW1lcl9zdGF0ZS9ldmVudF91c2VyLXRpbWVyc4u+mDWhAAECBQkAAyEE1lBnACCAWTgAJmNoYW5uZWwgNDQ5LGFw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", "handleName": "file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/632e33a6-4441-403a-98ea-e9ceb4842279", "stateSize": 2602},
                            "localPath": "MANIFEST-000004", "stateSize": 2602
                        },
                        {
                            "handle": {"@class": "org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle", "stateSize": 22776, "relativePath": "5820ceb4-5f2e-489e-8185-749713ef79ce", "streamStateHandleID": {"@class": "org.apache.flink.runtime.state.PhysicalStateHandleID", "keyString": "file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/5820ceb4-5f2e-489e-8185-749713ef79ce"}},
                            "localPath": "000040.sst", "stateSize": 22776
                        }
                    ]],
                    "metaStateHandle": {"@class": "org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle", "stateSize": 81584, "relativePath": "696275ab-57d4-482f-ae52-97bfd07d891e", "streamStateHandleID": {"@class": "org.apache.flink.runtime.state.PhysicalStateHandleID", "keyString": "file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/696275ab-57d4-482f-ae52-97bfd07d891e"}},
                    "stateHandleId": {"@class": "org.apache.flink.runtime.state.StateHandleID", "keyString": "13bbca3d-116b-4d9f-aeff-ad55c9cb29e1"},
                    "persistedSizeOfThisCheckpoint": 421936, "sharedStateHandles": ["java.util.Collections$EmptyList", []],
                    "stateSize": 421936, "checkpointedSize": 421936
                }]],
                "rawKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []], "inputChannelState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "resultSubpartitionState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "inputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "outputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
                "stateSize": 421936, "checkpointedSize": 421936, "finished": false
            }
        },
        "isTaskDeployedAsFinished": false, "isTaskFinished": false,
        "inputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
        "outputRescalingDescriptor": {"@class": "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$NoRescalingDescriptor", "gateOrPartitionDescriptors": ["[Lorg.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor$InflightDataGateOrPartitionRescalingDescriptor;", []]},
        "taskDeployedAsFinished": false, "taskFinished": false,
        "stateSize": 421936, "checkpointedSize": 421936
    })";
    auto snapshot = TaskStateSnapshotDeserializer::Deserialize(json_content);
    std::cout << "Local: " << snapshot->ToString() << std::endl;
    ASSERT_NE(snapshot, nullptr);

    EXPECT_FALSE(snapshot->GetIsTaskFinished());
    //    EXPECT_EQ(snapshot->GetStateSize(), 421936);
    const auto& subtask_states = snapshot->GetSubtaskStateMappings();
    ASSERT_EQ(subtask_states.size(), 2);

    OperatorID opIdWithState =
        TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>("4bf7c1955ffe56e2106d666433eaf137");
    bool found = false;
    std::shared_ptr<OperatorSubtaskState> state;
    for (const auto& pair : subtask_states) {
        if (pair.first == opIdWithState) {
            found = true;
            state = pair.second;
            break;
        }
    }
    ASSERT_TRUE(found);
    ASSERT_NE(state, nullptr);

    // 5. Downcast to the specific handle type to test nested fields.
    const auto& keyedStateHandles = state->GetManagedKeyedState();
    ASSERT_EQ(keyedStateHandles.Size(), 1);
    auto remoteHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(keyedStateHandles.ToArray().at(0));
    ASSERT_NE(remoteHandle, nullptr);

    EXPECT_EQ(remoteHandle->GetCheckpointId(), 1);
    //    EXPECT_EQ(remoteHandle->GetStateSize(), 421936);

    const auto& privateState = remoteHandle->GetPrivateState(); // Assuming getter
    ASSERT_EQ(privateState.size(), 2);

    auto byteStreamHandle = std::dynamic_pointer_cast<ByteStreamStateHandle>(privateState.at(0).getHandle());
    ASSERT_NE(byteStreamHandle, nullptr);
    EXPECT_EQ(
        byteStreamHandle->GetHandleName(),
        "file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/"
        "632e33a6-4441-403a-98ea-e9ceb4842279");
    EXPECT_EQ(privateState.at(0).getLocalPath(), "MANIFEST-000004");

    const auto& decoded_data = byteStreamHandle->GetData();
    //    EXPECT_EQ(decoded_data.size(), 1950); // The actual decoded size of the Base64 string.

    auto relativeFileHandle = std::dynamic_pointer_cast<RelativeFileStateHandle>(privateState.at(1).getHandle());
    ASSERT_NE(relativeFileHandle, nullptr);
    EXPECT_EQ(relativeFileHandle->GetRelativePath(), "5820ceb4-5f2e-489e-8185-749713ef79ce");
    EXPECT_EQ(privateState.at(1).getLocalPath(), "000040.sst");
    EXPECT_EQ(relativeFileHandle->GetStateSize(), 22776);
    //    delete snapshot;
}

TEST(DeserializerTestSuite, DeserializesRemoteStateSnapshotWithPlainJavaHandleArrays)
{
    const std::string json_content = R"({
        "subtaskStatesByOperatorID": {
            "4bf7c1955ffe56e2106d666433eaf137": {
                "managedKeyedState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", [{
                    "@class": "IncrementalRemoteKeyedStateHandle",
                    "backendIdentifier": "8d43f905-8fcb-491d-95df-87e4597464e4",
                    "keyGroupRange": {"startKeyGroup": 0, "endKeyGroup": 127},
                    "checkpointId": 1,
                    "privateState": [{
                        "handle": {
                            "@class": "ByteStreamStateHandle",
                            "handleName": "MANIFEST-000001",
                            "data": "AQID"
                        },
                        "localPath": "MANIFEST-000001"
                    }],
                    "metaStateHandle": {
                        "@class": "ByteStreamStateHandle",
                        "handleName": "meta",
                        "data": "AQID"
                    },
                    "stateHandleId": {"keyString": "13bbca3d-116b-4d9f-aeff-ad55c9cb29e1"},
                    "persistedSizeOfThisCheckpoint": 3
                }]],
                "inputChannelState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []],
                "resultSubpartitionState": ["org.apache.flink.runtime.checkpoint.StateObjectCollection", []]
            }
        },
        "isTaskDeployedAsFinished": false,
        "isTaskFinished": false
    })";

    auto snapshot = TaskStateSnapshotDeserializer::Deserialize(json_content);
    ASSERT_NE(snapshot, nullptr);

    OperatorID opIdWithState =
        TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>("4bf7c1955ffe56e2106d666433eaf137");
    auto state = snapshot->GetSubtaskStateByOperatorID(opIdWithState);
    ASSERT_NE(state, nullptr);

    const auto& keyedStateHandles = state->GetManagedKeyedState();
    ASSERT_EQ(keyedStateHandles.Size(), 1);
    auto remoteHandle = std::dynamic_pointer_cast<IncrementalRemoteKeyedStateHandle>(keyedStateHandles.ToArray().at(0));
    ASSERT_NE(remoteHandle, nullptr);
    EXPECT_TRUE(remoteHandle->GetSharedState().empty());
    ASSERT_EQ(remoteHandle->GetPrivateState().size(), 1);
    EXPECT_EQ(remoteHandle->GetPrivateState().at(0).getLocalPath(), "MANIFEST-000001");
}
