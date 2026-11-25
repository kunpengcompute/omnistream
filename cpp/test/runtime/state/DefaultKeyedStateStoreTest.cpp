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

#include "runtime/state/DefaultKeyedStateStore.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/LongSerializer.h"
#include "table/typeutils/StringDataSerializer.h"
#include <gtest/gtest.h>

TEST(DefaultKeyedStateStoreTest, MapStateTest)
{
    // Initialize HeapKeyedStateBackend
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<std::string>* context = new InternalKeyContextImpl<std::string>(range, 10);
    context->setCurrentKey("partition1");
    context->setCurrentKeyGroupIndex(1);
    StringDataSerializer* keySer = new StringDataSerializer();
    HeapKeyedStateBackend<std::string>* backend = new HeapKeyedStateBackend<std::string>(keySer, context);

    // Initialize DefaultKeyedStateStore
    DefaultKeyedStateStore<std::string> stateStore(backend);
    std::string name = "name";
    MapStateDescriptor<std::string, int>* stateDesc =
        new MapStateDescriptor<std::string, int>(name, new StringDataSerializer(), new IntSerializer());
    using MapStateType = HeapMapState<std::string, VoidNamespace, std::string, int>;
    auto* mapState = reinterpret_cast<HeapMapState<std::string, VoidNamespace, std::string, int>*>(
        stateStore.getMapState<std::string, int>(stateDesc));

    mapState->put("one", 1);
    mapState->put("two", 2);
    mapState->put("three", 3);
    EXPECT_EQ(*(mapState->get("one")), 1);
    EXPECT_EQ(*(mapState->get("two")), 2);
    EXPECT_EQ(*(mapState->get("three")), 3);

    delete range;
    delete context;
    delete backend;
}

TEST(DefaultKeyedStateStoreTest, ValueStateTest)
{
    // Initialize HeapKeyedStateBackend
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(range, 10);
    context->setCurrentKey(1);
    context->setCurrentKeyGroupIndex(1);
    HeapKeyedStateBackend<int>* backend = new HeapKeyedStateBackend<int>(new IntSerializer(), context);

    // Initialize DefaultKeyedStateStore
    DefaultKeyedStateStore<int> stateStore(backend);
    std::string name = "name";
    auto* stateDesc = new ValueStateDescriptor<int>(name, new IntSerializer());

    // using ValueStateType = HeapValueState<int, VoidNamespace, int>;

    auto* state = stateStore.getState<int>(stateDesc);
    auto* state2 = stateStore.getState<int>(stateDesc);
    EXPECT_EQ(state, state2);
    std::string anotherName = "another name";
    auto* stateDesc1 = new ValueStateDescriptor<int>(anotherName, new IntSerializer());
    auto* state3 = stateStore.getState<int>(stateDesc1);
    EXPECT_NE(state, state3);

    delete range;
    delete context;
    delete backend;
}
