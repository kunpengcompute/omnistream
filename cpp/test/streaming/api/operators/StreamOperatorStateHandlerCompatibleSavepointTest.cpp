/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/SavepointResources.h"
#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "test/runtime/state/CompatibleSavepointTestUtils.h"

namespace {

class ConfigurableCheckpointedOperator : public StreamOperatorStateHandler<int>::CheckpointedStreamOperator {
public:
    explicit ConfigurableCheckpointedOperator(FlinkSavepointAdaptorInfo adaptorInfo)
        : adaptorInfo_(std::move(adaptorInfo))
    {
    }

    FlinkSavepointAdaptorInfo getSavepointAdaptorInfo() const override
    {
        return adaptorInfo_;
    }

    nlohmann::json getOperatorDescription() const override
    {
        return {{"operator", "test"}};
    }

private:
    FlinkSavepointAdaptorInfo adaptorInfo_;
};

class CountingCheckpointableKeyedStateBackend : public CheckpointableKeyedStateBackend<int> {
public:
    CountingCheckpointableKeyedStateBackend()
    {
        snapshotResources_ =
            std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(false);
    }

    void setCurrentKey(int key) override
    {
        currentKey_ = key;
    }

    int getCurrentKey() override
    {
        return currentKey_;
    }

    uintptr_t createOrUpdateInternalState(TypeSerializer*, StateDescriptor*) override
    {
        return 0;
    }

    void dispose() override
    {
    }

    TypeSerializer* getKeySerializer() override
    {
        return &keySerializer_;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return snapshotResources_->getKeyGroupRange();
    }

    std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long, long, std::shared_ptr<CheckpointStreamFactory>, CheckpointOptions*) override
    {
        snapshotCount_++;
        return std::make_shared<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>>(
            []() { return SnapshotResult<KeyedStateHandle>::Empty(); });
    }

    std::shared_ptr<SavepointResources> savepoint() override
    {
        savepointCount_++;
        return std::make_shared<SavepointResources>(snapshotResources_, SnapshotExecutionType::ASYNCHRONOUS);
    }

    std::shared_ptr<SavepointResources> compatibleSavepoint() override
    {
        compatibleSavepointCount_++;
        return std::make_shared<SavepointResources>(snapshotResources_, SnapshotExecutionType::ASYNCHRONOUS);
    }

    int savepointCount() const
    {
        return savepointCount_;
    }

    int snapshotCount() const
    {
        return snapshotCount_;
    }

    int compatibleSavepointCount() const
    {
        return compatibleSavepointCount_;
    }

private:
    int currentKey_ = 0;
    int savepointCount_ = 0;
    int snapshotCount_ = 0;
    int compatibleSavepointCount_ = 0;
    IntSerializer keySerializer_;
    std::shared_ptr<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources> snapshotResources_;
};

class DefaultCompatibleSavepointBackend : public CountingCheckpointableKeyedStateBackend {
public:
    std::shared_ptr<SavepointResources> compatibleSavepoint() override
    {
        return CheckpointableKeyedStateBackend<int>::compatibleSavepoint();
    }
};

std::unique_ptr<CheckpointOptions> makeCheckpointOptions(SnapshotType& snapshotType)
{
    return std::unique_ptr<CheckpointOptions>(
        CheckpointOptions::AlignedNoTimeout(snapshotType, CheckpointStorageLocationReference::GetDefault()));
}

std::unique_ptr<OperatorSnapshotFutures> snapshot(
    StreamOperatorStateHandler<int>& handler,
    StreamOperatorStateHandler<int>::CheckpointedStreamOperator* streamOperator,
    CheckpointOptions* checkpointOptions)
{
    return std::unique_ptr<OperatorSnapshotFutures>(
        handler.SnapshotState(streamOperator, nullptr, "test", 42L, 100L, checkpointOptions, nullptr, false, nullptr));
}

std::unique_ptr<StreamOperatorStateHandler<int>> makeStateHandler(CountingCheckpointableKeyedStateBackend* backend)
{
    return std::make_unique<StreamOperatorStateHandler<int>>(
        new StreamOperatorStateContextImpl<int>(std::nullopt, backend, nullptr, nullptr));
}

} // namespace

// 看护 CheckpointedStreamOperator 默认 adaptor 类型为 None，由具体算子提供可诊断的 unsupported reason。
TEST(StreamOperatorStateHandlerCompatibleSavepointTest, DefaultOperatorAdaptorInfoIsNone)
{
    StreamOperatorStateHandler<int>::CheckpointedStreamOperator op;

    auto adaptorInfo = op.getSavepointAdaptorInfo();

    EXPECT_EQ(adaptorInfo.type, FlinkSavepointAdaptorType::None);
}

// 看护 compatibleSavepoint 默认复用普通 savepoint，避免把 Heap 特有资源捕获语义强加给所有状态后端。
TEST(StreamOperatorStateHandlerCompatibleSavepointTest, CompatibleSavepointDefaultReusesRegularSavepoint)
{
    DefaultCompatibleSavepointBackend backend;

    auto resources = backend.compatibleSavepoint();

    EXPECT_NE(resources, nullptr);
    EXPECT_EQ(backend.savepointCount(), 1);
    EXPECT_EQ(backend.compatibleSavepointCount(), 0);
    EXPECT_EQ(backend.snapshotCount(), 0);
}

// 看护普通 checkpoint 保持既有 backend snapshot 路径，不误入任一 savepoint 路径。
TEST(StreamOperatorStateHandlerCompatibleSavepointTest, CheckpointUsesRegularBackendSnapshot)
{
    auto* backend = new CountingCheckpointableKeyedStateBackend();
    auto handler = makeStateHandler(backend);
    ConfigurableCheckpointedOperator op({FlinkSavepointAdaptorType::None, ""});
    auto checkpointOptions = makeCheckpointOptions(*CheckpointType::CHECKPOINT);

    auto futures = snapshot(*handler, &op, checkpointOptions.get());

    EXPECT_NE(futures, nullptr);
    EXPECT_EQ(backend->snapshotCount(), 1);
    EXPECT_EQ(backend->savepointCount(), 0);
    EXPECT_EQ(backend->compatibleSavepointCount(), 0);
}

// 看护 compatible + None 在任何 backend snapshot 前失败。
TEST(StreamOperatorStateHandlerCompatibleSavepointTest, CompatibleSavepointWithNoneFailsBeforeBackendSnapshot)
{
    auto* backend = new CountingCheckpointableKeyedStateBackend();
    auto handler = makeStateHandler(backend);
    ConfigurableCheckpointedOperator op({FlinkSavepointAdaptorType::None, "unsupported operator"});
    auto savepointType = SavepointType::savepoint(SavepointFormatType::COMPATIBLE);
    auto checkpointOptions = makeCheckpointOptions(*savepointType);

    auto futures = snapshot(*handler, &op, checkpointOptions.get());

    EXPECT_NE(futures, nullptr);
    auto keyedStateFuture = futures->getKeyedStateManagedFuture();
    EXPECT_NE(keyedStateFuture, nullptr);
    // cancel() injected a failed future; operator() stores the exception internally
    (*keyedStateFuture)();
    EXPECT_THROW(keyedStateFuture->get_future().get(), std::runtime_error);
    EXPECT_EQ(backend->snapshotCount(), 0);
    EXPECT_EQ(backend->savepointCount(), 0);
    EXPECT_EQ(backend->compatibleSavepointCount(), 0);
}

// 看护 compatible + OmniIsCompatible 复用 canonical savepoint 路径。
TEST(StreamOperatorStateHandlerCompatibleSavepointTest, CompatibleSavepointWithOmniIsCompatibleUsesCanonicalSnapshot)
{
    auto* backend = new CountingCheckpointableKeyedStateBackend();
    auto handler = makeStateHandler(backend);
    ConfigurableCheckpointedOperator op({FlinkSavepointAdaptorType::OmniIsCompatible, ""});
    auto savepointType = SavepointType::savepoint(SavepointFormatType::COMPATIBLE);
    auto checkpointOptions = makeCheckpointOptions(*savepointType);

    auto futures = snapshot(*handler, &op, checkpointOptions.get());

    EXPECT_NE(futures, nullptr);
    EXPECT_NE(futures->getKeyedStateManagedFuture(), nullptr);
    EXPECT_EQ(backend->snapshotCount(), 0);
    EXPECT_EQ(backend->savepointCount(), 1);
    EXPECT_EQ(backend->compatibleSavepointCount(), 0);
}
