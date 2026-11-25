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
#include <unordered_set>
#include <sstream>
#include "runtime/checkpoint/CheckpointMetaData.h"

TEST(CheckpointMetaDataTest, ConstructsCorrectlyWithDefaultReceiveTimestamp)
{
    int64_t id = 42;
    int64_t ts = 100000;
    CheckpointMetaData meta(id, ts);

    EXPECT_EQ(meta.GetCheckpointId(), id);
    EXPECT_EQ(meta.GetTimestamp(), ts);

    int64_t now =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    EXPECT_LE(meta.GetReceiveTimestamp(), now);
}

TEST(CheckpointMetaDataTest, ConstructsCorrectlyWithExplicitReceiveTimestamp)
{
    CheckpointMetaData meta(10, 20, 30);
    EXPECT_EQ(meta.GetCheckpointId(), 10);
    EXPECT_EQ(meta.GetTimestamp(), 20);
    EXPECT_EQ(meta.GetReceiveTimestamp(), 30);
}

TEST(CheckpointMetaDataTest, EqualityAndInequality)
{
    CheckpointMetaData a(1, 2, 3);
    CheckpointMetaData b(1, 2, 3);
    CheckpointMetaData c(1, 2, 4);

    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST(CheckpointMetaDataTest, HashingWorksCorrectly)
{
    CheckpointMetaData a(100, 200, 300);
    CheckpointMetaData b(100, 200, 300);
    CheckpointMetaData c(100, 200, 301);

    std::hash<CheckpointMetaData> hasher;
    EXPECT_EQ(hasher(a), hasher(b));
    EXPECT_NE(hasher(a), hasher(c));
}

TEST(CheckpointMetaDataTest, CanBeUsedInUnorderedSet)
{
    std::unordered_set<CheckpointMetaData> set;
    CheckpointMetaData meta(100, 200, 300);

    set.insert(meta);
    EXPECT_TRUE(set.find(meta) != set.end());
}

TEST(CheckpointMetaDataTest, OutputFormatting)
{
    CheckpointMetaData meta(123, 456, 789);
    std::ostringstream oss;
    oss << meta;
    std::string output = oss.str();
    EXPECT_NE(output.find("checkpointId=123"), std::string::npos);
    EXPECT_NE(output.find("timestamp=456"), std::string::npos);
    EXPECT_NE(output.find("receiveTimestamp=789"), std::string::npos);
}
