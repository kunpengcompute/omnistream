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
#include "runtime/state/IncrementalRemoteKeyedStateHandle.h"

TEST(IncrementalRemoteKeyedStateHandleTest, InitTest)
{
    IncrementalRemoteKeyedStateHandle* handle = new IncrementalRemoteKeyedStateHandle(
        UUID::randomUUID(),
        KeyGroupRange(0, 1),
        15,
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>(),
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>(),
        nullptr,
        1,
        StateHandleID("StateHandleID"));
    delete handle;
}

TEST(IncrementalRemoteKeyedStateHandleTest, ToStringTest)
{
    auto uuid = UUID::randomUUID();
    IncrementalRemoteKeyedStateHandle* handle = new IncrementalRemoteKeyedStateHandle(
        uuid,
        KeyGroupRange(0, 1),
        15,
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>(),
        std::vector<IncrementalKeyedStateHandle::HandleAndLocalPath>(),
        nullptr,
        1,
        StateHandleID(uuid.ToString()));
    auto str = handle->ToString();
    nlohmann::json j = nlohmann::json::parse(str);

    EXPECT_EQ(j["backendIdentifier"], uuid.ToString());
    EXPECT_EQ(j["keyGroupRange"]["startKeyGroup"], 0);
    EXPECT_EQ(j["keyGroupRange"]["endKeyGroup"], 1);
    EXPECT_EQ(j["keyGroupRange"]["numberOfKeyGroups"], 2);
    EXPECT_EQ(j["stateHandleId"]["keyString"], uuid.ToString());
    EXPECT_TRUE(j["sharedState"].is_array());
    EXPECT_TRUE(j["sharedState"].empty());
    EXPECT_TRUE(j["privateState"].is_array());
    EXPECT_TRUE(j["privateState"].empty());
    EXPECT_EQ(j["checkpointId"], 15);
    delete handle;
}

TEST(IncrementalRemoteKeyedStateHandleTest, FromJsonTest)
{
    std::string jsonString = R"({
                    "backendIdentifier": "0ced51ed-dbc4-487c-a371-3f5fe758f478",
                    "keyGroupRange": {
                        "startKeyGroup": 0,
                        "endKeyGroup": 127,
                        "numberOfKeyGroups": 128
                    },
                    "checkpointId": 16,
                    "sharedState": ["java.util.Collections$EmptyList", []],
                    "privateState": ["java.util.ArrayList", []],
                    "metaStateHandle": {
                        "@class": "org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle",
                        "stateSize": 81584,
                        "relativePath": "6ea1f850-6bf2-4917-a241-cbd6e8bb8e6c",
                        "streamStateHandleID": {
                            "keyString": "file:/home/hudsonsheng/flink/checkpoints/2f9d0a799f7facb1691d9209a4b6be85/chk-16/6ea1f850-6bf2-4917-a241-cbd6e8bb8e6c"
                        },
                        "stateHandleName": "RelativePathHandle"
                    },
                    "persistedSizeOfThisCheckpoint": 425906,
                    "stateHandleId": {
                        "keyString": "a336e3e5-9c46-412b-8e5d-b7a9f7159b8f"
                    },
                    "sharedStateRegistry": null,
                    "sharedStateHandles": [],
                    "stateSize": 425906,
                    "checkpointedSize": 425906
                })";
    IncrementalRemoteKeyedStateHandle* handle =
        new IncrementalRemoteKeyedStateHandle(nlohmann::json::parse(jsonString));
    EXPECT_EQ(handle->GetBackendIdentifier().ToString(), "0ced51ed-dbc4-487c-a371-3f5fe758f478");
    EXPECT_EQ(handle->GetKeyGroupRange(), KeyGroupRange(0, 127));
    EXPECT_EQ(handle->GetCheckpointId(), 16);
    EXPECT_EQ(handle->GetSharedState().size(), 0);
    // Should be 1 when PrivateStateHandle is properly parsed
    EXPECT_EQ(handle->GetPrivateState().size(), 0);
}
