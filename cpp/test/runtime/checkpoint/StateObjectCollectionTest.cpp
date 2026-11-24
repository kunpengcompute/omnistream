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
#include <gmock/gmock.h>
#include <vector>
#include <memory>
#include "runtime/checkpoint/StateObjectCollection.h"

// Mock StateObject for testing
class MockStateObject : public StateObject {
public:
    long GetStateSize() const override
    {
        return 42;
    }
    void DiscardState() override
    {
    }

    std::string ToString() const override
    {
        return "";
    }
};

TEST(StateObjectCollectionTest, testEmptyCollection)
{
    StateObjectCollection<StateObject> empty = *(StateObjectCollection<StateObject>::Empty().get());
    EXPECT_EQ(0, empty.GetStateSize());
}

TEST(StateObjectCollectionTest, testHasState)
{
    // Test with empty ArrayList
    std::vector<std::shared_ptr<StateObject>> emptyVector;
    StateObjectCollection<StateObject> stateObjects(emptyVector);
    EXPECT_FALSE(stateObjects.HasState());

    // Test with null object
    std::vector<std::shared_ptr<StateObject>> nullVector;
    nullVector.push_back(nullptr);
    stateObjects = StateObjectCollection<StateObject>(nullVector);
    EXPECT_FALSE(stateObjects.HasState());

    // Test with mock object
    auto mockObject = std::make_shared<MockStateObject>();
    stateObjects = StateObjectCollection<StateObject>({mockObject});
    EXPECT_TRUE(stateObjects.HasState());
}
