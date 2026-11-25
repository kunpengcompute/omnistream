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
#include "core/typeutils/LongSerializer.h"
#include "runtime/state/heap/CopyOnWriteStateMap.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
using namespace omnistream;
TEST(CopyOnWriteStateMapTest, PutAndGet)
{
    IntSerializer* ser = IntSerializer::INSTANCE;
    CopyOnWriteStateMap<long, long, long> map(ser, 1);
    for (int i = 0; i < 5; i++) {
        map.put(i, 0, 2 * i);
    }
    EXPECT_EQ(6, map.get(3, 0));
    // non-existing key
    EXPECT_EQ(std::numeric_limits<long>::max(), map.get(42, 0));
    // non-existing namespace
    EXPECT_EQ(std::numeric_limits<long>::max(), map.get(3, 1));
}

TEST(CopyOnWriteStateMapTest, PutAndGetRowKey)
{
    // Use two columns to construct a binaryRowData as key
    std::vector<std::string> fieldname = {"BIGINT", "BIGINT"};
    omnistream::RowType rowType(true, fieldname);
    RowDataSerializer* ser = new RowDataSerializer(&rowType);
    CopyOnWriteStateMap<RowData*, long, long> map(ser, 1);

    BinaryRowData* key0 = BinaryRowData::createBinaryRowDataWithMem(2);
    key0->setLong(0, 1);
    key0->setLong(1, 2);
    map.put(key0, 0, 3);

    BinaryRowData* key1 = BinaryRowData::createBinaryRowDataWithMem(2);
    key1->setLong(0, 3);
    key1->setLong(1, 4);
    map.put(key1, 0, 5);

    BinaryRowData* copiedKey0 = dynamic_cast<BinaryRowData*>(key0->copy());
    BinaryRowData* copiedKey1 = dynamic_cast<BinaryRowData*>(key1->copy());

    EXPECT_EQ(3, map.get(copiedKey0, 0));
    EXPECT_EQ(5, map.get(copiedKey1, 0));
    // non-existing namespace
    EXPECT_EQ(std::numeric_limits<long>::max(), map.get(copiedKey0, 1));
    // non-existing key
    copiedKey0->setLong(0, 42);
    EXPECT_EQ(std::numeric_limits<long>::max(), map.get(copiedKey0, 0));
}

TEST(CopyOnWriteStateMapTest, PutAndRemove)
{
    IntSerializer* ser = IntSerializer::INSTANCE;

    CopyOnWriteStateMap<long, long, long> map(ser, 1);
    map.put(1, 1, 1);
    map.put(2, 2, 2);

    bool contains1 = map.containsKey(1, 1);
    EXPECT_EQ(contains1, true);
    bool contains2 = map.containsKey(2, 2);
    EXPECT_EQ(contains2, true);
    map.remove(1, 1);
    bool afterremove1 = map.containsKey(1, 1);
    EXPECT_EQ(afterremove1, false);
}

TEST(CopyOnWriteStateMapTest, IteratorTest)
{
    IntSerializer* ser = IntSerializer::INSTANCE;
    CopyOnWriteStateMap<long, long, long> map(ser, 1);
    for (int i = 1; i <= 5; i++) {
        map.put(i, 0, 2 * i);
    }

    std::unordered_set<int> keySet;
    std::unordered_set<int> compareSet = {1, 2, 3, 4, 5};

    for (auto it = map.begin(); it != map.end(); it++) {
        keySet.insert(it->first);
    }
    EXPECT_EQ(keySet, compareSet);
}
