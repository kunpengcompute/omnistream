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

#include "table/runtime/dataview/PerKeyStateDataViewStore.h"
#include "table/runtime/dataview/StateMapView.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "functions/RuntimeContext.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "core/typeutils/MapSerializer.h"
#include "runtime/state/KeyGroupRange.h"
#include <emhash7.hpp>
#include <gtest/gtest.h>

TEST(PerKeyStateDataViewStoreTest, InitTest)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    auto* keyContext = new InternalKeyContextImpl<int>(range, 10);
    keyContext->setCurrentKey(1);
    keyContext->setCurrentKeyGroupIndex(1);
    auto* backend = new HeapKeyedStateBackend<int>(new IntSerializer(), keyContext);

    // Initialize DefaultKeyedStateStore
    auto* stateStore = new DefaultKeyedStateStore(backend);

    // Initialize StreamingRuntimeContext
    auto* ctx = new StreamingRuntimeContext(stateStore, nullptr);

    // Initialized PerKeyStateDataViewStore
    auto* store = new PerKeyStateDataViewStore(ctx);

    // Should be created successfully
    ASSERT_NO_THROW(
        (store->getStateMapView<VoidNamespace, int, int*>("test", false, new IntSerializer(), new IntSerializer())));
    delete store;
    delete ctx;
    delete stateStore;
    delete backend;
    delete keyContext;
    delete range;
};

TEST(PerKeyStateDataViewStoreTest, RetrievalTest)
{
    KeyGroupRange* range = new KeyGroupRange(0, 10);
    auto* keyContext = new InternalKeyContextImpl<int>(range, 10);
    keyContext->setCurrentKey(1);
    keyContext->setCurrentKeyGroupIndex(1);
    auto* backend = new HeapKeyedStateBackend<int>(new IntSerializer(), keyContext);

    // Initialize DefaultKeyedStateStore
    auto* stateStore = new DefaultKeyedStateStore(backend);

    // Initialize StreamingRuntimeContext
    auto* ctx = new StreamingRuntimeContext(stateStore, nullptr);

    // Initialized PerKeyStateDataViewStore
    auto* store = new PerKeyStateDataViewStore(ctx);

    // Should be created successfully
    KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int>* mapView =
        reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int>*>(
            store->getStateMapView<VoidNamespace, int, int*>("test", false, new IntSerializer(), new IntSerializer()));

    mapView->put(1, 3);

    EXPECT_EQ(*(mapView->get(1)), 3);

    KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int>* newMapView =
        reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, int, int>*>(
            store->getStateMapView<VoidNamespace, int, int*>("test", false, new IntSerializer(), new IntSerializer()));

    EXPECT_EQ(*(newMapView->get(1)), 3);

    delete newMapView;
    delete mapView;
    delete store;
    delete ctx;
    delete stateStore;
    delete backend;
    delete keyContext;
    delete range;
};
