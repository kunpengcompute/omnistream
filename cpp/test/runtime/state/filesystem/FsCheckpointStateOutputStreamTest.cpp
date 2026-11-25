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
#include "core/fs/Path.h"
#include "runtime/state/filesystem/FsCheckpointStateOutputStream.h"

namespace fs = std::filesystem;

TEST(FsCheckpointStateOutputStream, WriteAndGetPos)
{
    std::string baseName = (fs::temp_directory_path() / "Test").string();

    FsCheckpointStateOutputStream stream(Path(baseName), 0, 0, 0, true);

    const char data[] = "xyz";
    stream.Write(data, 3);
    EXPECT_EQ(stream.GetPos(), 3);

    stream.Flush();

    std::ifstream in(baseName + ".tmp", std::ios::binary);
    ASSERT_TRUE(in.good());

    std::string contents((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "xyz");
    EXPECT_FALSE(stream.IsClosed());

    in.close();
    fs::remove(baseName + ".tmp");
}
