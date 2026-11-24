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

#include "gtest/gtest.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/LocalRecoveryDirectoryProviderImpl.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/jobgraph/JobVertexID.h"
#include <filesystem>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <stdexcept>

namespace fs = std::filesystem;

using namespace omnistream;

TEST(LocalRecoveryDirectoryProviderImplTest, RecoveryDirectories)
{
    fs::path tempDir = fs::temp_directory_path() / "LocalRecoveryDirectoryProviderImplTest";
    fs::create_directories(tempDir);
    std::vector<fs::path> baseDirs = {tempDir / "dir_1", tempDir / "dir_2"};

    JobIDPOD jobID(1, 2);
    JobVertexID jobVertexID(3, 4);
    int subtaskIndex = 5;

    LocalRecoveryDirectoryProviderImpl provider(baseDirs, jobID, jobVertexID, subtaskIndex);

    EXPECT_EQ(provider.AllocationBaseDirsCount(), 2);
    EXPECT_EQ(provider.SelectAllocationBaseDirectory(0), baseDirs[0]);
    EXPECT_EQ(provider.SelectAllocationBaseDirectory(1), baseDirs[1]);
    EXPECT_THROW(provider.SelectAllocationBaseDirectory(2), std::out_of_range);

    long checkpointId = 10;
    fs::path subtaskDir = provider.SubtaskBaseDirectory(checkpointId);
    fs::path checkpointDir = provider.SubtaskSpecificCheckpointDirectory(checkpointId);

    std::string expectedSubtaskPath =
        (baseDirs[0] / ("jid_" + jobID.AbstractIDPOD::toString()) /
         ("vtx_" + jobVertexID.AbstractIDPOD::toString() + "_sti_" + std::to_string(subtaskIndex)))
            .string();
    std::string expectedCheckpointPath = expectedSubtaskPath + "/chk_10";

    EXPECT_EQ(subtaskDir.string(), expectedSubtaskPath);
    EXPECT_EQ(checkpointDir.string(), expectedCheckpointPath);

    fs::path expectedSelectSubtaskBaseDir =
        baseDirs[1] / ("jid_" + jobID.AbstractIDPOD::toString()) /
        ("vtx_" + jobVertexID.AbstractIDPOD::toString() + "_sti_" + std::to_string(subtaskIndex));
    EXPECT_EQ(provider.SelectSubtaskBaseDirectory(1), expectedSelectSubtaskBaseDir);

    fs::remove_all(tempDir);
}

TEST(LocalRecoveryDirectoryProviderImplTest, DirectoryRotation)
{
    fs::path tempDir = fs::temp_directory_path() / "LocalRecoveryDirectoryProviderImplTest";
    fs::create_directories(tempDir);
    std::vector<fs::path> baseDirs = {tempDir / "dir_1", tempDir / "dir_2", tempDir / "dir_3"};

    JobIDPOD jobID(1, 1);
    JobVertexID jobVertexID(1, 1);
    LocalRecoveryDirectoryProviderImpl provider(baseDirs, jobID, jobVertexID, 0);

    EXPECT_EQ(provider.AllocationBaseDirectory(0), baseDirs[0]); // 0 % 3 = 0
    EXPECT_EQ(provider.AllocationBaseDirectory(1), baseDirs[1]); // 1 % 3 = 1
    EXPECT_EQ(provider.AllocationBaseDirectory(2), baseDirs[2]); // 2 % 3 = 2
    EXPECT_EQ(provider.AllocationBaseDirectory(3), baseDirs[0]); // 3 % 3 = 0
    EXPECT_EQ(provider.AllocationBaseDirectory(4), baseDirs[1]); // 4 % 3 = 1

    fs::remove_all(tempDir);
}
