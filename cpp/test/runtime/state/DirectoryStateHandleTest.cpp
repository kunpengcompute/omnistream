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
#include <fstream>
#include <filesystem>

#include "runtime/state/filesystem/FileStateHandle.h"
#include "runtime/state/DirectoryStateHandle.h"
#include "core/fs/Path.h"

TEST(DirectoryStateHandleTest, DirectoryStateInitialization)
{
    std::string dirName = (std::filesystem::temp_directory_path() / "DirectoryStateHandleTest").string();
    std::string fileName = "TestFile.tmp";
    std::string content = "Test File For DirectoryStateHandleTest.DirectoryStateInitialization";

    std::filesystem::remove_all(dirName);

    std::filesystem::create_directories(dirName);
    std::ofstream ofs(dirName + "/" + fileName, std::ios::binary);
    ofs << content;
    ofs.close();

    ASSERT_TRUE(std::filesystem::exists(dirName));
    ASSERT_TRUE(std::filesystem::exists(dirName + "/" + fileName));

    Path path(dirName);
    DirectoryStateHandle handle = DirectoryStateHandle::forPathWithSize(path);

    EXPECT_GT(handle.GetStateSize(), 10);
    EXPECT_EQ(handle.getDirectory().toString(), dirName);

    handle.DiscardState();
    EXPECT_FALSE(std::filesystem::exists(dirName));

    std::filesystem::remove_all(dirName);
}
