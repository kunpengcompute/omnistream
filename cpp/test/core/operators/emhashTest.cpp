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
#include "core/include/emhash7.hpp"
#include "basictypes/Object.h"

using namespace std;
using namespace emhash7;

TEST(HashMapTest, DefaultConstructor)
{
    HashMap<string, int> map;
    EXPECT_EQ(map.size(), 0);
    EXPECT_EQ(map.empty(), true);
}

TEST(HashMapTest, Insert_Value)
{
    HashMap<string, int> map;
    map.insert({"key1", 1});
    map.insert({"key2", 2});

    EXPECT_EQ(map.size(), 2);
    EXPECT_EQ(map["key1"], 1);
    EXPECT_EQ(map["key2"], 2);
}

TEST(HashMapTest, Emplace_Value)
{
    HashMap<string, int> map;
    map.emplace("key1", 1);
    map.emplace("key2", 2);

    EXPECT_EQ(map.size(), 2);
    EXPECT_EQ(map["key1"], 1);
    EXPECT_EQ(map["key2"], 2);
}

TEST(HashMapTest, InsertOrAssign_Value)
{
    HashMap<string, int> map;
    map.insert_or_assign("key1", 1);
    map.insert_or_assign("key1", 2);

    EXPECT_EQ(map.size(), 1);
    EXPECT_EQ(map["key1"], 2);
}

TEST(HashMapTest, TryGet_Value)
{
    HashMap<string, int> map;
    map.insert({"key1", 1});
    EXPECT_EQ(map.at("key1"), 1);
}

TEST(HashMapTest, Contains_Value)
{
    HashMap<string, int> map;
    map.insert({"key1", 1});
    map.insert({"key2", 2});

    EXPECT_TRUE(map.contains("key1"));
    EXPECT_FALSE(map.contains("key3"));
}

TEST(HashMapTest, Erase_Value)
{
    HashMap<string, int> map;
    map.insert({"key1", 1});
    map.insert({"key2", 2});
    map.insert({"key3", 3});

    map.erase("key2");
    EXPECT_EQ(map.size(), 2);
    EXPECT_EQ(map.find("key2"), map.end());
    EXPECT_EQ(map["key1"], 1);
    EXPECT_EQ(map["key3"], 3);
}

TEST(HashMapTest, Clear_Map)
{
    HashMap<string, int> map;
    map.insert({"key1", 1});
    map.insert({"key2", 2});

    map.clear();
    EXPECT_EQ(map.size(), 0);
    EXPECT_EQ(map.empty(), true);
}

TEST(HashMapTest, Reserve_Map)
{
    HashMap<string, int> map;
    map.reserve(100);
    EXPECT_GE(map.bucket_count(), 100);
}

TEST(HashMapTest, Rehash_Map)
{
    HashMap<string, int> map;
    map.rehash(100);
    EXPECT_GE(map.bucket_count(), 100);
}

TEST(HashMapTest, Find_Value)
{
    HashMap<string, int> map;
    map.insert({"key1", 1});
    map.insert({"key2", 2});

    auto it = map.find("key1");
    EXPECT_NE(it, map.end());
    EXPECT_EQ(it->first, "key1");
    EXPECT_EQ(it->second, 1);

    it = map.find("key3");
    EXPECT_EQ(it, map.end());
}

TEST(HashMapTest, Operator_Bracket)
{
    HashMap<string, int> map;
    map["key1"] = 1;
    map["key2"] = 2;

    EXPECT_EQ(map.size(), 2);
    EXPECT_EQ(map["key1"], 1);
    EXPECT_EQ(map["key2"], 2);

    // Insertion when key does not exist
    EXPECT_EQ(map["key3"], 0);
    EXPECT_EQ(map.size(), 3);
}

TEST(HashMapTest, Iteration)
{
    HashMap<string, int> map;
    map["key1"] = 1;
    map["key2"] = 2;

    std::vector<std::pair<std::string, int>> expected = {{"key1", 1}, {"key2", 2}};
    std::vector<std::pair<std::string, int>> actual;

    for (const auto& pair : map) {
        actual.emplace_back(pair.first, pair.second);
    }

    EXPECT_EQ(actual, expected);
}

//
// TEST(HashMapTest, UpdateOrCreate_Value) {
//    HashMap<string, int> map;
//    map.updateOrCreate("key1", 1);
//    map.updateOrCreate("key2", 2);
//    map.updateOrCreate("key1", 2);
//
//    EXPECT_EQ(map.size(), 2);
//    EXPECT_EQ(map["key1"], 2);
//    EXPECT_EQ(map["key2"], 2);
//}

TEST(HashMapTest, Merge_Values)
{
    HashMap<string, int> map1;
    map1["key1"] = 1;
    map1["key2"] = 2;

    HashMap<string, int> map2;
    map2["key2"] = 3;
    map2["key3"] = 4;

    map1.merge(map2);

    EXPECT_EQ(map1.size(), 3);
    EXPECT_EQ(map1["key1"], 1);
    EXPECT_EQ(map1["key2"], 2);
    EXPECT_EQ(map1["key3"], 4);
}

TEST(HashMapTest, Resize_Bucket)
{
    HashMap<string, int> map;
    for (int i = 0; i < 1000; ++i) {
        map.emplace("key" + to_string(i), i);
    }

    EXPECT_GE(map.bucket_count(), 1000);
}

TEST(HashMapTest, Delete_And_Rehash)
{
    HashMap<string, int> map;
    for (int i = 0; i < 1000; ++i) {
        map.emplace("key" + to_string(i), i);
    }

    for (int i = 0; i < 500; ++i) {
        map.erase("key" + to_string(i));
    }

    map.rehash(map.bucket_count() / 2);

    EXPECT_GE(map.bucket_count(), 500);
}
