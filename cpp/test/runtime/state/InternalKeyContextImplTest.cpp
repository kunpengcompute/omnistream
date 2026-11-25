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
#include "runtime/state/InternalKeyContextImpl.h"

TEST(InternalKeyContextImplTest, SetCurrentKey)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);
    EXPECT_EQ(context->getCurrentKey(), 1);
    delete range;
    delete context;
}

TEST(InternalKeyContextImplTest, SetCurrentGroupIndex)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKeyGroupIndex(1);
    EXPECT_EQ(context->getCurrentKeyGroupIndex(), 1);
    delete range;
    delete context;
}

TEST(InternalKeyContextImplTest, GetKeyGroupRange)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    EXPECT_EQ(*context->getKeyGroupRange(), *range);
    delete range;
    delete context;
}

TEST(InternalKeyContextImplTest, GetNumberOfKeyGroups)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    EXPECT_EQ(context->getNumberOfKeyGroups(), 10);
    delete range;
    delete context;
}
