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

#include <map>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/snapshot/RocksIncrementalSnapshotStrategy.h"
#include "runtime/snapshot/RocksNativeFullSnapshotStrategy.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"

namespace {

std::shared_ptr<NativeRocksDBSnapshotResources> makeResources()
{
    auto metaInfo = std::make_shared<StateMetaInfoSnapshot>(
        "state",
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
    return std::make_shared<NativeRocksDBSnapshotResources>(
        nullptr,
        PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>{metaInfo});
}

CheckpointOptions makeCheckpointOptions()
{
    return CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
}

} // namespace

TEST(RocksDBSnapshotStrategyOwnershipTest, IncrementalAsyncSupplierRetainsConcreteStrategy)
{
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation;
    std::map<long, std::vector<HandleAndLocalPath>> uploadedStateHandles;
    auto resources = makeResources();
    auto checkpointOptions = makeCheckpointOptions();
    auto uploader = std::make_shared<RocksDBStateUploader>(1);
    std::weak_ptr<RocksIncrementalSnapshotStrategy> weakStrategy;
    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> supplier;

    {
        auto strategy = std::make_shared<RocksIncrementalSnapshotStrategy>(
            nullptr,
            std::make_shared<ResourceGuard>(),
            nullptr,
            &kvStateInformation,
            KeyGroupRange(),
            0,
            std::make_shared<LocalRecoveryConfig>(nullptr),
            "",
            UUID(),
            uploadedStateHandles,
            uploader,
            -1L);
        weakStrategy = strategy;
        supplier = strategy->asyncSnapshot(resources, 1L, 1L, nullptr, &checkpointOptions, "");
        ASSERT_NE(supplier, nullptr);
        strategy.reset();
        EXPECT_FALSE(weakStrategy.expired());
    }

    supplier.reset();
    EXPECT_TRUE(weakStrategy.expired());
}

TEST(RocksDBSnapshotStrategyOwnershipTest, NativeFullAsyncSupplierRetainsConcreteStrategy)
{
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation;
    auto resources = makeResources();
    auto checkpointOptions = makeCheckpointOptions();
    auto uploader = std::make_shared<RocksDBStateUploader>(1);
    std::weak_ptr<RocksNativeFullSnapshotStrategy> weakStrategy;
    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> supplier;

    {
        auto strategy = std::make_shared<RocksNativeFullSnapshotStrategy>(
            nullptr,
            std::make_shared<ResourceGuard>(),
            nullptr,
            &kvStateInformation,
            KeyGroupRange(),
            0,
            std::make_shared<LocalRecoveryConfig>(nullptr),
            "",
            UUID(),
            uploader);
        weakStrategy = strategy;
        supplier = strategy->asyncSnapshot(resources, 1L, 1L, nullptr, &checkpointOptions, "");
        ASSERT_NE(supplier, nullptr);
        strategy.reset();
        EXPECT_FALSE(weakStrategy.expired());
    }

    supplier.reset();
    EXPECT_TRUE(weakStrategy.expired());
}

TEST(RocksDBSnapshotStrategyOwnershipTest, CleanupDeletesTemporarySnapshotDirectory)
{
    const auto directory =
        std::filesystem::temp_directory_path() / ("omnistream-rocks-snapshot-" + UUID::randomUUID().ToString());
    std::filesystem::create_directories(directory);
    std::ofstream(directory / "000001.sst") << "snapshot";

    auto snapshotDirectory = SnapshotDirectoryFactory::temporary(directory);
    NativeRocksDBSnapshotResources resources(
        std::shared_ptr<SnapshotDirectory>(std::move(snapshotDirectory)),
        PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT,
        {});

    resources.cleanup();

    EXPECT_FALSE(std::filesystem::exists(directory));
}

TEST(RocksDBSnapshotStrategyOwnershipTest, CleanupRetainsCompletedPermanentSnapshotDirectory)
{
    const auto directory =
        std::filesystem::temp_directory_path() / ("omnistream-local-recovery-" + UUID::randomUUID().ToString());
    std::filesystem::create_directories(directory);
    std::ofstream(directory / "000001.sst") << "snapshot";

    auto snapshotDirectory = SnapshotDirectoryFactory::permanent(directory);
    auto sharedDirectory = std::shared_ptr<SnapshotDirectory>(std::move(snapshotDirectory));
    auto stateHandle = sharedDirectory->completeSnapshotAndGetHandle();
    ASSERT_NE(stateHandle, nullptr);

    NativeRocksDBSnapshotResources resources(sharedDirectory, PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT, {});
    resources.cleanup();

    EXPECT_TRUE(std::filesystem::exists(directory));
    stateHandle->DiscardState();
    EXPECT_FALSE(std::filesystem::exists(directory));
}

TEST(RocksDBSnapshotStrategyOwnershipTest, CancelBeforePackagedTaskExecutionCleansTemporarySnapshot)
{
    const auto directory =
        std::filesystem::temp_directory_path() / ("omnistream-cancelled-snapshot-" + UUID::randomUUID().ToString());
    std::filesystem::create_directories(directory);
    std::ofstream(directory / "000001.sst") << "snapshot";

    auto snapshotDirectory = SnapshotDirectoryFactory::temporary(directory);
    auto resources = std::make_shared<NativeRocksDBSnapshotResources>(
        std::shared_ptr<SnapshotDirectory>(std::move(snapshotDirectory)),
        PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>{});
    auto task = std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
        [resources]() { return std::shared_ptr<SnapshotResult<KeyedStateHandle>>{}; });

    OperatorSnapshotFutures futures;
    futures.setKeyedStateManagedFuture(task);
    resources.reset();
    task.reset();
    ASSERT_TRUE(std::filesystem::exists(directory));

    futures.cancel("test checkpoint abort before packaged task execution");

    EXPECT_FALSE(std::filesystem::exists(directory));
}
