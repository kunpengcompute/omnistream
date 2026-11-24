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
#include <memory>
#include <vector>
#include <random>
#include "runtime/checkpoint/OperatorSubtaskState.h"
#include "runtime/checkpoint/TaskStateSnapshot.h"
#include "runtime/jobgraph/OperatorID.h"

class MockOperatorSubtaskState : public OperatorSubtaskState {
public:
    void DiscardState() override
    {
    }
    void RegisterSharedStates(SharedStateRegistry& stateRegistry, long checkpointID) override
    {
    }
    std::string ToString() const override
    {
        return "";
    }
};

class MockOperatorStateHandle : public OperatorStateHandle {
public:
    void DiscardState() override
    {
    }
    long GetStateSize() const override
    {
        return 2;
    }
    std::unordered_map<std::string, StateMetaInfo> getStateNameToPartitionOffsets() const override
    {
        return std::unordered_map<std::string, StateMetaInfo>();
    };
    std::shared_ptr<StreamStateHandle> getDelegateStateHandle() const override
    {
        return nullptr;
    };
    std::string ToString() const override
    {
        return "";
    }

    PhysicalStateHandleID getStreamStateHandleID()
    {
        return GetStreamStateHandleID();
    }

    std::shared_ptr<FSDataInputStream> OpenInputStream() const override
    {
        return nullptr;
    }

    std::optional<std::vector<uint8_t>> AsBytesIfInMemory() const override
    {
        return std::optional<std::vector<uint8_t>>();
    }

    PhysicalStateHandleID GetStreamStateHandleID() const override
    {
        PhysicalStateHandleID id("");
        return id;
        ;
    }

    bool operator==(const StreamStateHandle& other_) const override
    {
        return false;
    }
};
TEST(TaskStateSnapshotTest, putGetSubtaskStateByOperatorID)
{
    TaskStateSnapshot taskStateSnapshot;

    OperatorID operatorID_1 = OperatorID();
    OperatorID operatorID_2 = OperatorID();
    auto operatorSubtaskState_1 = std::make_shared<MockOperatorSubtaskState>();
    auto operatorSubtaskState_2 = std::make_shared<MockOperatorSubtaskState>();
    auto operatorSubtaskState_1_replace = std::make_shared<MockOperatorSubtaskState>();

    EXPECT_EQ(nullptr, taskStateSnapshot.GetSubtaskStateByOperatorID(operatorID_1));
    EXPECT_EQ(nullptr, taskStateSnapshot.GetSubtaskStateByOperatorID(operatorID_2));
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1);
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID_2, operatorSubtaskState_2);
    EXPECT_EQ(operatorSubtaskState_1, taskStateSnapshot.GetSubtaskStateByOperatorID(operatorID_1));
    EXPECT_EQ(operatorSubtaskState_2, taskStateSnapshot.GetSubtaskStateByOperatorID(operatorID_2));
    EXPECT_EQ(
        operatorSubtaskState_1,
        taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1_replace));
    EXPECT_EQ(operatorSubtaskState_1_replace, taskStateSnapshot.GetSubtaskStateByOperatorID(operatorID_1));
}

TEST(TaskStateSnapshotTest, hasState)
{
    TaskStateSnapshot taskStateSnapshot;
    EXPECT_FALSE(taskStateSnapshot.HasState());

    auto emptyOperatorSubtaskState = std::make_shared<MockOperatorSubtaskState>();
    EXPECT_FALSE(emptyOperatorSubtaskState->HasState());
    OperatorID operatorID = OperatorID();
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID, emptyOperatorSubtaskState);
    EXPECT_FALSE(taskStateSnapshot.HasState());

    auto stateHandle = std::make_shared<MockOperatorStateHandle>();
    auto nonEmptyOperatorSubtaskState = std::make_shared<MockOperatorSubtaskState>();
    nonEmptyOperatorSubtaskState->SetManagedOperatorState(stateHandle);

    EXPECT_TRUE(nonEmptyOperatorSubtaskState->HasState());
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID, nonEmptyOperatorSubtaskState);
    EXPECT_TRUE(taskStateSnapshot.HasState());
}

TEST(TaskStateSnapshotTest, discardState)
{
    TaskStateSnapshot taskStateSnapshot;
    auto operatorSubtaskState_1 = std::make_shared<MockOperatorSubtaskState>();
    auto operatorSubtaskState_2 = std::make_shared<MockOperatorSubtaskState>();

    taskStateSnapshot.PutSubtaskStateByOperatorID(OperatorID(), operatorSubtaskState_1);
    taskStateSnapshot.PutSubtaskStateByOperatorID(OperatorID(), operatorSubtaskState_2);
    EXPECT_NO_THROW(taskStateSnapshot.DiscardState());
}

TEST(TaskStateSnapshotTest, getStateSize)
{
    std::mt19937 random(0x42);
    TaskStateSnapshot taskStateSnapshot;
    EXPECT_EQ(0, taskStateSnapshot.GetStateSize());

    auto emptyOperatorSubtaskState = std::make_shared<MockOperatorSubtaskState>();
    EXPECT_FALSE(emptyOperatorSubtaskState->HasState());
    OperatorID operatorID = OperatorID();
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID, emptyOperatorSubtaskState);
    EXPECT_EQ(0, taskStateSnapshot.GetStateSize());

    auto stateHandle_1 = std::make_shared<MockOperatorStateHandle>();
    auto nonEmptyOperatorSubtaskState_1 = std::make_shared<MockOperatorSubtaskState>();
    nonEmptyOperatorSubtaskState_1->SetManagedOperatorState(stateHandle_1);

    auto stateHandle_2 = std::make_shared<MockOperatorStateHandle>();
    auto nonEmptyOperatorSubtaskState_2 = std::make_shared<MockOperatorSubtaskState>();
    nonEmptyOperatorSubtaskState_2->SetManagedOperatorState(stateHandle_2);

    OperatorID operatorID_1 = OperatorID();
    OperatorID operatorID_2 = OperatorID();
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID_1, nonEmptyOperatorSubtaskState_1);
    taskStateSnapshot.PutSubtaskStateByOperatorID(operatorID_2, nonEmptyOperatorSubtaskState_2);

    long totalSize = stateHandle_1->GetStateSize() + stateHandle_2->GetStateSize();
    EXPECT_EQ(totalSize, taskStateSnapshot.GetStateSize());
}
