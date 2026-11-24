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
#include <stdexcept>
#include <string>
#include <vector>

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CompatibleSavepointSnapshotResources.h"
#include "runtime/state/CompatibleSavepointSnapshotStrategy.h"
#include "test/runtime/state/CompatibleSavepointTestUtils.h"

namespace {

class TrackingAdaptor : public omnistream::OperatorSavepointAdaptor {
public:
    explicit TrackingAdaptor(bool* destroyed, int* validateCount) : destroyed_(destroyed), validateCount_(validateCount)
    {
    }

    ~TrackingAdaptor() override
    {
        if (destroyed_ != nullptr) {
            *destroyed_ = true;
        }
    }

    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&) override
    {
        if (validateCount_ != nullptr) {
            (*validateCount_)++;
        }
    }

    void save(CheckpointStateOutputStreamProxy&, KeyGroupRangeOffsets&, FullSnapshotResources&, std::string) override
    {
    }

    void restore(SavepointRestoreResultIterator&, omnistream::RestoreBackendDelegate&) override
    {
    }

private:
    bool* destroyed_;
    int* validateCount_;
};

std::shared_ptr<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources> makeSourceResources(
    bool withMetaInfo)
{
    return std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(withMetaInfo);
}

std::unique_ptr<TrackingAdaptor> makeAdaptor(bool* destroyed = nullptr, int* validateCount = nullptr)
{
    return std::make_unique<TrackingAdaptor>(destroyed, validateCount);
}

} // namespace

// 看护 resources cleanup 只向底层 source resources 透传一次，避免重复释放同步准备资源。
TEST(CompatibleSavepointSnapshotStrategyTest, ResourcesCleanupPropagatesToSourceResourcesOnce)
{
    auto source = makeSourceResources(true);
    CompatibleSavepointSnapshotResources resources(
        source, makeAdaptor(), compatible_savepoint_test::makeCompatibleTestAdaptorInfo());

    resources.cleanup();
    resources.cleanup();

    EXPECT_EQ(source->cleanupCount(), 1);
}

// 看护 source resources 或 adaptor 缺失时构造期 fail-fast，防止 compatible 分支进入 async 阶段后解引用空依赖。
TEST(CompatibleSavepointSnapshotStrategyTest, ResourcesConstructThrowsForNullRequiredDependency)
{
    EXPECT_THROW(
        CompatibleSavepointSnapshotResources(
            nullptr, makeAdaptor(), compatible_savepoint_test::makeCompatibleTestAdaptorInfo()),
        std::invalid_argument);
    auto source = makeSourceResources(true);
    EXPECT_THROW(
        CompatibleSavepointSnapshotResources(
            source, nullptr, compatible_savepoint_test::makeCompatibleTestAdaptorInfo()),
        std::invalid_argument);
}

// 看护空 metadata 也进入 writer validation 边界并一次性转移 adaptor，合法无状态 operator 返回 empty result。
TEST(CompatibleSavepointSnapshotStrategyTest, AsyncSnapshotValidatesEmptyMetaAndTransfersAdaptorOwnershipOnce)
{
    bool adaptorDestroyed = false;
    int validateCount = 0;
    auto resources = std::make_shared<CompatibleSavepointSnapshotResources>(
        makeSourceResources(false),
        makeAdaptor(&adaptorDestroyed, &validateCount),
        compatible_savepoint_test::makeCompatibleTestAdaptorInfo());
    CompatibleSavepointSnapshotStrategy strategy(resources);
    auto savepointType = SavepointType::savepoint(SavepointFormatType::COMPATIBLE);
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    auto supplier = strategy.asyncSnapshot(resources, 42L, 100L, nullptr, checkpointOptions.get());
    EXPECT_NE(std::dynamic_pointer_cast<CompatibleFullSnapshotAsyncWriter>(supplier), nullptr);
    EXPECT_THROW(resources->takeAdaptor(), std::logic_error);
    EXPECT_FALSE(adaptorDestroyed);
    auto result = supplier->get(nullptr);

    EXPECT_EQ(validateCount, 1);
    EXPECT_EQ(result->GetJobManagerOwnedSnapshot(), nullptr);
    EXPECT_EQ(result->GetTaskLocalSnapshot(), nullptr);
    supplier.reset();
    EXPECT_TRUE(adaptorDestroyed);
}

// 看护非空 metadata 进入 writer 边界前会一次性转移 adaptor，避免后续重复 move 同一 adaptor。
TEST(CompatibleSavepointSnapshotStrategyTest, AsyncSnapshotTransfersAdaptorOwnershipOnce)
{
    bool adaptorDestroyed = false;
    auto resources = std::make_shared<CompatibleSavepointSnapshotResources>(
        makeSourceResources(true),
        makeAdaptor(&adaptorDestroyed),
        compatible_savepoint_test::makeCompatibleTestAdaptorInfo());
    CompatibleSavepointSnapshotStrategy strategy(resources);
    auto savepointType = SavepointType::savepoint(SavepointFormatType::COMPATIBLE);
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    auto supplier = strategy.asyncSnapshot(resources, 42L, 100L, nullptr, checkpointOptions.get());
    EXPECT_THROW(resources->takeAdaptor(), std::logic_error);
    EXPECT_FALSE(adaptorDestroyed);
    supplier.reset();
    EXPECT_TRUE(adaptorDestroyed);
}
