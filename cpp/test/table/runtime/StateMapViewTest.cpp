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

#include "table/runtime/dataview/StateMapView.h"
#include "runtime/state/VoidNamespace.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/heap/HeapMapState.h"
#include "runtime/state/heap/HeapValueState.h"
#include "runtime/state/heap/StateTable.h"
#include "runtime/state/InternalKeyContextImpl.h"

#include <gtest/gtest.h>

TEST(StateMapViewTest, PutGetClearTest)
{
    IntSerializer* ser = new IntSerializer();
    InternalKeyContextImpl<int>* context = new InternalKeyContextImpl<int>(new KeyGroupRange(0, 1), 3);
    context->setCurrentKey(1);
    context->setCurrentKeyGroupIndex(1);
    RegisteredKeyValueStateBackendMetaInfo* metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", ser, ser);

    // HeapMapState Init
    auto table1 = new CopyOnWriteStateTable<int, VoidNamespace, emhash7::HashMap<int, int>*>(context, metaInfo, ser);
    auto heapMapState = new HeapMapState<int, VoidNamespace, int, int>(table1, ser, ser, ser);

    // HeapValueState Init
    auto table2 = new CopyOnWriteStateTable<int, VoidNamespace, int>(context, metaInfo, ser);
    auto heapValueState = new HeapValueState<int, VoidNamespace, int>(table2, ser, ser, ser, 0);

    auto view = new KeyedStateMapViewWithKeysNullable<VoidNamespace, int, int>(heapMapState, heapValueState);

    view->put(1, 2);
    EXPECT_EQ(*(view->get(1)), 2);
    view->put(1, 3);
    EXPECT_EQ(*(view->get(1)), 3);
    view->put(std::nullopt, 4);
    EXPECT_EQ(*(view->get(std::nullopt)), 4);

    view->clear();
    // 0 s the nullvalue
    EXPECT_EQ(view->get(std::nullopt), 0);
    EXPECT_EQ(view->get(1), std::nullopt);
};
