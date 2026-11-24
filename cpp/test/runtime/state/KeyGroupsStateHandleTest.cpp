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
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"

TEST(KeyGroupsStateHandleTest, NonEmptyIntersection)
{
    KeyGroupRangeOffsets offsets(0, 7);
    std::vector<uint8_t> dummy(10, 0);
    std::shared_ptr<StreamStateHandle> streamHandle = std::make_shared<ByteStreamStateHandle>("test", dummy);

    KeyGroupsStateHandle handle(offsets, streamHandle);

    KeyGroupRange expectedRange(0, 3);
    auto tempHandle = handle.GetIntersection(expectedRange);
    auto newHandle = std::dynamic_pointer_cast<KeyGroupsStateHandle>(tempHandle);

    ASSERT_NE(newHandle, nullptr);
    // The delegate stream handle should have the same ID as the original
    ASSERT_EQ(newHandle->getDelegateStateHandle()->GetStreamStateHandleID(), streamHandle->GetStreamStateHandleID());
    ASSERT_EQ(newHandle->GetKeyGroupRange(), expectedRange);
    ASSERT_EQ(handle.GetStateHandleId(), newHandle->GetStateHandleId());
}

TEST(KeyGroupsStateHandleTest, EmptyIntersection)
{
    KeyGroupRangeOffsets offsets(0, 7);
    std::vector<uint8_t> dummy(10, 0);

    std::shared_ptr<StreamStateHandle> streamHandle = std::make_shared<ByteStreamStateHandle>("test", dummy);
    KeyGroupsStateHandle handle(offsets, streamHandle);

    KeyGroupRange newRange(8, 11);

    // Should return nullptr since the intersection is empty
    auto newHandle = handle.GetIntersection(newRange);

    ASSERT_EQ(newHandle, nullptr);
}
