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
#include <bits/stdc++.h>
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/StateTable.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "core/typeutils/LongSerializer.h"
TEST(CopyOnWriteStateTableTest, InitTest)
{
    IntSerializer* ser = IntSerializer::INSTANCE;
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 2), 3);
    RegisteredKeyValueStateBackendMetaInfo* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser, ser);
    CopyOnWriteStateTable<int, int, int> table(context, metaInfo, ser); // metaInfo delete when table delete
    EXPECT_EQ(table.isEmpty(), true);
    EXPECT_EQ(table.getKeyGroupOffset(), 0);
    delete context;
}

// TEST(CopyOnWriteStateTableTest, PutAndGetTest)
// {
//     IntSerializer *ser = IntSerializer::INSTANCE;
//     InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 1), 3);
//     RegisteredKeyValueStateBackendMetaInfo *metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser,
//     ser); CopyOnWriteStateTable<int, int, int> table(context, metaInfo, ser); EXPECT_EQ(table.isEmpty(), true);
//     context->setCurrentKey(1);
//     table.put(1, 1);
//     EXPECT_EQ(*(table.get(1, 1)), 1);
//     context->setCurrentKey(2);
//     table.put(1, 2);
//     context->setCurrentKey(3);
//     table.put(2, 3);
//     EXPECT_EQ(table.size(), 3);
//     delete ser;
//     delete context;
//     delete metaInfo;
// }
TEST(CopyOnWriteStateTableTest, PutAndGetTest)
{
    IntSerializer* ser = IntSerializer::INSTANCE;
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 2), 3);
    RegisteredKeyValueStateBackendMetaInfo* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser, ser);
    CopyOnWriteStateTable<int, int, int> table(context, metaInfo, ser);
    EXPECT_EQ(table.isEmpty(), true);

    for (int i = 0; i < 10; i++) {
        context->setCurrentKey(i);
        context->setCurrentKeyGroupIndex(table.computeKeyGroupForKeyHash(i));
        table.put(i, i);
        EXPECT_EQ(table.get(i, i), i);
    }
    EXPECT_EQ(table.size(), 10);
    delete context;
}

// TEST(CopyOnWriteStateTableTest, IteratorTest)
// {
//         IntSerializer *ser = IntSerializer::INSTANCE;
//     InternalKeyContextImpl<int> *context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 1), 3);
//     RegisteredKeyValueStateBackendMetaInfo *metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser,
//     ser); CopyOnWriteStateTable<int, int, int> table(context, metaInfo, ser); std::unordered_set<int> compareSet =
//     {}; for (int i = 0; i < 10; i++)
//     {
//         compareSet.insert(i);
//         context->setCurrentKey(i);
//         table.put(i, i);
//     }
//     auto it = table.getStateIncrementalVisitor(1);
//     std::unordered_set<int> iteratorSet = {};
TEST(CopyOnWriteStateTableTest, IteratorTest)
{
    IntSerializer* ser = IntSerializer::INSTANCE;
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 2), 3);
    context->setCurrentKeyGroupIndex(0);
    RegisteredKeyValueStateBackendMetaInfo* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser, ser);
    CopyOnWriteStateTable<int, int, int> table(context, metaInfo, ser);
    std::unordered_set<int> compareSet = {};
    for (int i = 0; i < 10; i++) {
        compareSet.insert(i);
        context->setCurrentKey(i);
        table.put(i, i);
    }
    auto it = table.getStateIncrementalVisitor(1);
    std::unordered_set<int> iteratorSet = {};

    while (it->hasNext()) {
        iteratorSet.insert(it->nextEntries());
    }
    EXPECT_EQ(iteratorSet, compareSet);
    delete context;
}
