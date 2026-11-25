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
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include "runtime/state/DirectoryKeyedStateHandle.h"
#include "runtime/state/DirectoryStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "core/fs/Path.h"

TEST(DirectoryKeyedStateHandleTest, Initialization)
{
    std::string dirName = (std::filesystem::temp_directory_path() / "DirectoryKeyedStateHandleTest").string();
    std::string fileName = "TestFile.tmp";
    std::string content = "Test File For DirectoryKeyedStateHandleTest.Initialization";

    std::filesystem::remove_all(dirName);
    std::filesystem::create_directories(dirName);
    std::ofstream ofs(dirName + "/" + fileName, std::ios::binary);
    ofs << content;
    ofs.close();
    ASSERT_TRUE(std::filesystem::exists(dirName));
    ASSERT_TRUE(std::filesystem::exists(dirName + "/" + fileName));

    Path path(dirName);
    DirectoryStateHandle dirHandle = DirectoryStateHandle::forPathWithSize(path);

    KeyGroupRange keyGroupRange(0, 5);
    DirectoryKeyedStateHandle directoryKeyedStateHandle(new DirectoryStateHandle(dirHandle), keyGroupRange);

    EXPECT_GT(directoryKeyedStateHandle.GetStateSize(), 10);
    EXPECT_FALSE(directoryKeyedStateHandle.GetStateHandleId().ToString().empty());
    EXPECT_EQ(directoryKeyedStateHandle.GetKeyGroupRange(), keyGroupRange);

    KeyGroupRange other(3, 10);
    auto intersectionHandle = directoryKeyedStateHandle.GetIntersection(other);
    ASSERT_NE(intersectionHandle, nullptr);
    EXPECT_EQ(intersectionHandle->GetKeyGroupRange(), KeyGroupRange(3, 5));

    KeyGroupRange disjoint(6, 8);
    EXPECT_EQ(directoryKeyedStateHandle.GetIntersection(disjoint), nullptr);

    directoryKeyedStateHandle.DiscardState();
    EXPECT_FALSE(std::filesystem::exists(dirName));

    std::filesystem::remove_all(dirName);
}

TEST(DirectoryKeyedStateHandleTest, JSONConstructor)
{
    std::string dirName = (std::filesystem::temp_directory_path() / "DirectoryKeyedStateHandleTest").string();
    std::string fileName = "JSONConstructor.tmp";
    std::string content = "Test File For DirectoryKeyedStateHandleTest.JSONConstructor";

    std::filesystem::remove_all(dirName);
    std::filesystem::create_directories(dirName);
    std::ofstream ofs(dirName + "/" + fileName, std::ios::binary);
    ofs << content;
    ofs.close();
    ASSERT_TRUE(std::filesystem::exists(dirName));
    ASSERT_TRUE(std::filesystem::exists(dirName + "/" + fileName));

    Path path(dirName);
    DirectoryStateHandle originalDirHandle = DirectoryStateHandle::forPathWithSize(path);
    long expectedSize = originalDirHandle.GetStateSize();

    nlohmann::json jsonObj;
    jsonObj["directoryStateHandle"] = {{"directoryString", path.toString()}, {"stateSize", expectedSize}};
    jsonObj["keyGroupRange"] = {{"startKeyGroup", 0}, {"endKeyGroup", 5}};

    DirectoryKeyedStateHandle directoryKeyedStateHandle(jsonObj);

    EXPECT_EQ(directoryKeyedStateHandle.GetStateSize(), expectedSize);
    EXPECT_EQ(directoryKeyedStateHandle.GetKeyGroupRange(), KeyGroupRange(0, 5));

    const std::string toStringResult = directoryKeyedStateHandle.ToString();
    auto parsed = nlohmann::json::parse(toStringResult);
    EXPECT_EQ(parsed["stateHandleName"], "DirectoryKeyedStateHandle");
    EXPECT_EQ(parsed["directoryStateHandle"]["stateHandleName"], "DirectoryStateHandle");
    EXPECT_EQ(parsed["directoryStateHandle"]["directory"], path.toString());
    EXPECT_EQ(parsed["directoryStateHandle"]["stateSize"], expectedSize);
    EXPECT_EQ(parsed["keyGroupRange"]["startKeyGroup"], 0);
    EXPECT_EQ(parsed["keyGroupRange"]["endKeyGroup"], 5);

    directoryKeyedStateHandle.DiscardState();
    EXPECT_FALSE(std::filesystem::exists(dirName));
    std::filesystem::remove_all(dirName);
}
