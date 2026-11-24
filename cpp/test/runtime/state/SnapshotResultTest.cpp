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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>

#include "runtime/state/SnapshotResult.h"

using ::testing::AtLeast;
using ::testing::Exactly;

// Mock StateObject using gMock
class MockStateObject : public StateObject {
public:
    MOCK_METHOD(void, DiscardState, (), (override));
    MOCK_METHOD(long, GetStateSize, (), (const override));
    std::string ToString() const override
    {
        return "";
    }
};

class DummyStateObject : public StateObject {
    long size;

public:
    explicit DummyStateObject(long s) : size(s)
    {
    }
    void DiscardState() override
    {
    }
    long GetStateSize() const override
    {
        return size;
    }
    std::string ToString() const override
    {
        return "";
    }
};

// TEST(SnapshotResultTest, DiscardStateCallsBothSnapshots) {
//     auto jobState = std::make_shared<MockStateObject>();
//     auto localState = std::make_shared<MockStateObject>();

//     EXPECT_CALL(*jobState, DiscardState());
//     EXPECT_CALL(*localState, DiscardState());

//     auto result = SnapshotResult<StateObject>::WithLocalState(jobState, localState);
//     result->DiscardState();
// }

TEST(SnapshotResultTest, GetStateSizeReturnsCorrectValue)
{
    const long size = 42;
    auto jobState = std::make_shared<DummyStateObject>(size);
    auto localState = std::make_shared<DummyStateObject>(999);

    auto result = SnapshotResult<StateObject>::WithLocalState(jobState, localState);
    EXPECT_EQ(result->GetStateSize(), size);
}

TEST(SnapshotResultTest, EmptyReturnsSingleton)
{
    auto Empty1 = SnapshotResult<StateObject>::Empty();
    auto Empty2 = SnapshotResult<StateObject>::Empty();
    EXPECT_EQ(Empty1.get(), Empty2.get());
}

TEST(SnapshotResultTest, OfReturnsEmptyOnNullJobState)
{
    auto result = SnapshotResult<StateObject>::Of(nullptr);
    EXPECT_EQ(result->GetJobManagerOwnedSnapshot(), nullptr);
    EXPECT_EQ(result->GetTaskLocalSnapshot(), nullptr);
}

TEST(SnapshotResultTest, OfReturnsCorrectSnapshotResult)
{
    auto jobState = std::make_shared<MockStateObject>();

    auto result = SnapshotResult<StateObject>::Of(jobState);
    EXPECT_EQ(result->GetJobManagerOwnedSnapshot(), jobState);
    EXPECT_EQ(result->GetTaskLocalSnapshot(), nullptr);
}

TEST(SnapshotResultTest, constructorThrowsExceptionWhenExpected)
{
    auto jobState = nullptr;
    auto localState = std::make_shared<MockStateObject>();

    EXPECT_THROW(SnapshotResult<StateObject>::WithLocalState(jobState, localState), std::logic_error);
}
