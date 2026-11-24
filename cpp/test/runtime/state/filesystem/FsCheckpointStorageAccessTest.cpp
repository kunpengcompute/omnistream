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
#include <memory>

#include "runtime/state/filesystem/FsCheckpointStorageAccess.h"
#include "runtime/state/DefaultOperatorStateBackendSnapshotStrategy.h"

using namespace omnistream;

TEST(FsCheckpointStorageAccessTest, InitTest)
{
    auto checkpointBaseDirectory = std::make_shared<Path>("/tmp/checkpoints");
    auto defaultSavepointDirectory = std::make_shared<Path>("/tmp/savepoints");
    JobIDPOD jobId(12345, 122222);
    int fileSizeThreshold = 1024;
    int writeBufferSize = 4096;

    auto storage = std::make_shared<FsCheckpointStorageAccess>(
        checkpointBaseDirectory, defaultSavepointDirectory, jobId, fileSizeThreshold, writeBufferSize);
    EXPECT_NE(storage, nullptr);
}

TEST(FsCheckpointStorageAccessTest, DefaultLocationsRetainSharedDirectoriesAndReleaseOwnedPaths)
{
    auto storage = std::make_shared<FsCheckpointStorageAccess>(
        std::make_shared<Path>("/tmp/checkpoints/"), nullptr, JobIDPOD(), 100, 100);
    auto reference = std::make_shared<CheckpointStorageLocationReference>();
    auto first =
        std::dynamic_pointer_cast<FsCheckpointStorageLocation>(storage->resolveCheckpointStorageLocation(1, reference));
    auto second =
        std::dynamic_pointer_cast<FsCheckpointStorageLocation>(storage->resolveCheckpointStorageLocation(2, reference));
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);

    EXPECT_NE(first->getCheckpointDirectory(), second->getCheckpointDirectory());
    EXPECT_EQ(first->getSharedStateDirectory(), second->getSharedStateDirectory());
    EXPECT_EQ(first->getTaskOwnedStateDirectory(), second->getTaskOwnedStateDirectory());
    EXPECT_EQ(first->getTargetPath(CheckpointedStateScope::EXCLUSIVE), first->getCheckpointDirectory());
    EXPECT_EQ(first->getTargetPath(CheckpointedStateScope::SHARED), first->getSharedStateDirectory());
    EXPECT_EQ(first->getMetadataFilePath()->toString(), first->getCheckpointDirectory()->toString() + "_metadata");

    std::weak_ptr<Path> firstCheckpoint = first->getCheckpointDirectory();
    std::weak_ptr<Path> firstMetadata = first->getMetadataFilePath();
    std::weak_ptr<Path> shared = first->getSharedStateDirectory();
    std::weak_ptr<Path> taskOwned = first->getTaskOwnedStateDirectory();
    storage.reset();
    EXPECT_FALSE(shared.expired());
    EXPECT_FALSE(taskOwned.expired());

    first.reset();
    EXPECT_TRUE(firstCheckpoint.expired());
    EXPECT_TRUE(firstMetadata.expired());
    EXPECT_FALSE(shared.expired());
    EXPECT_FALSE(taskOwned.expired());

    second.reset();
    EXPECT_TRUE(shared.expired());
    EXPECT_TRUE(taskOwned.expired());
}

TEST(FsCheckpointStorageAccessTest, NonDefaultLocationSharesOnePathOwnerAndReleasesReference)
{
    auto storage = std::make_shared<FsCheckpointStorageAccess>(
        std::make_shared<Path>("/tmp/checkpoints/"), nullptr, JobIDPOD(), 100, 100);
    const std::string savepointPath = "/tmp/savepoints/savepoint-1/";
    auto bytes = std::make_shared<std::vector<uint8_t>>(std::initializer_list<uint8_t>{0x05, 0x5F, 0x3F, 0x18});
    bytes->insert(bytes->end(), savepointPath.begin(), savepointPath.end());
    auto reference = std::make_shared<CheckpointStorageLocationReference>(bytes);
    auto location =
        std::dynamic_pointer_cast<FsCheckpointStorageLocation>(storage->resolveCheckpointStorageLocation(1, reference));
    ASSERT_NE(location, nullptr);

    EXPECT_EQ(location->getCheckpointDirectory()->toString(), savepointPath);
    EXPECT_EQ(location->getMetadataFilePath()->toString(), savepointPath + "_metadata");
    EXPECT_EQ(location->getCheckpointDirectory(), location->getSharedStateDirectory());
    EXPECT_EQ(location->getCheckpointDirectory(), location->getTaskOwnedStateDirectory());
    EXPECT_FALSE(location->getCheckpointDirectory().owner_before(location->getSharedStateDirectory()));
    EXPECT_FALSE(location->getSharedStateDirectory().owner_before(location->getCheckpointDirectory()));
    EXPECT_FALSE(location->getCheckpointDirectory().owner_before(location->getTaskOwnedStateDirectory()));
    EXPECT_FALSE(location->getTaskOwnedStateDirectory().owner_before(location->getCheckpointDirectory()));

    std::weak_ptr<Path> path = location->getCheckpointDirectory();
    std::weak_ptr<Path> metadata = location->getMetadataFilePath();
    std::weak_ptr<CheckpointStorageLocationReference> weakReference = reference;
    storage.reset();
    reference.reset();
    EXPECT_FALSE(path.expired());
    EXPECT_FALSE(weakReference.expired());

    location.reset();
    EXPECT_TRUE(path.expired());
    EXPECT_TRUE(metadata.expired());
    EXPECT_TRUE(weakReference.expired());
}

TEST(FsCheckpointStorageAccessTest, AsyncOperatorSnapshotRetainsFactoryAndDirectories)
{
    auto storage = std::make_shared<FsCheckpointStorageAccess>(
        std::make_shared<Path>("/tmp/checkpoints/"), nullptr, JobIDPOD(), 100, 100);
    auto factory = storage->resolveCheckpointStorageLocation(1, std::make_shared<CheckpointStorageLocationReference>());
    auto location = std::dynamic_pointer_cast<FsCheckpointStorageLocation>(factory);
    ASSERT_NE(location, nullptr);
    std::weak_ptr<CheckpointStreamFactory> weakFactory = factory;
    std::weak_ptr<Path> checkpoint = location->getCheckpointDirectory();
    std::weak_ptr<Path> shared = location->getSharedStateDirectory();
    std::weak_ptr<Path> metadata = location->getMetadataFilePath();

    // Cancel an unstarted async snapshot after its synchronous owners have gone away.
    auto operation = std::make_shared<DefaultOperatorSnapshotOperation>(1, nullptr, factory, nullptr);
    location.reset();
    factory.reset();
    storage.reset();
    EXPECT_FALSE(weakFactory.expired());
    EXPECT_FALSE(checkpoint.expired());
    EXPECT_FALSE(shared.expired());
    EXPECT_FALSE(metadata.expired());

    operation.reset();
    EXPECT_TRUE(weakFactory.expired());
    EXPECT_TRUE(checkpoint.expired());
    EXPECT_TRUE(shared.expired());
    EXPECT_TRUE(metadata.expired());
}
