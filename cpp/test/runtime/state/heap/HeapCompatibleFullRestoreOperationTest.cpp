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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/heap/HeapCompatibleFullRestoreOperation.h"
#include "runtime/state/heap/HeapKeyedStateBackendBuilder.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

namespace {

StateMetaInfoSnapshot makeMetaInfo()
{
    return StateMetaInfoSnapshot(
        "state",
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

std::shared_ptr<KeyedStateHandle> makeKeyedStateHandle()
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange);
    auto streamHandle = std::make_shared<ByteStreamStateHandle>("heap-compatible-restore", std::vector<uint8_t>{1});
    return std::make_shared<KeyGroupsStateHandle>(offsets, streamHandle);
}

FlinkSavepointAdaptorInfo makeAdaptorInfo()
{
    return {FlinkSavepointAdaptorType::GroupAggAdaptor, "group agg compatible adaptor"};
}

class RecordingRestoreAdaptor : public omnistream::OperatorSavepointAdaptor {
public:
    explicit RecordingRestoreAdaptor(std::vector<std::string>* events) : events_(events)
    {
    }

    void prepareForRestore(const nlohmann::json& operatorDescription) override
    {
        events_->push_back("prepare");
        operatorName_ = operatorDescription.value("operator", "");
    }

    void validateForRestore(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override
    {
        events_->push_back("validate");
        validatedMetaCounts_.push_back(metaInfos.size());
        if (failOnEmptyMetadata_ && metaInfos.empty()) {
            throw std::runtime_error("empty metadata rejected");
        }
        if (throwOnValidate_) {
            throw std::runtime_error("validate failed");
        }
    }

    void save(CheckpointStateOutputStreamProxy&, KeyGroupRangeOffsets&, FullSnapshotResources&, std::string) override
    {
    }

    void restore(SavepointRestoreResultIterator& restoreIterator, omnistream::RestoreBackendDelegate&) override
    {
        events_->push_back("restore");
        actualIteratorHadNext_ = restoreIterator.hasNext();
        if (throwOnRestore_) {
            throw std::runtime_error("restore failed");
        }
    }

    std::vector<std::string>* events_;
    bool failOnEmptyMetadata_ = false;
    bool throwOnValidate_ = false;
    bool throwOnRestore_ = false;
    bool actualIteratorHadNext_ = false;
    std::vector<size_t> validatedMetaCounts_;
    std::string operatorName_;
};

class HeapCompatibleFullRestoreOperationTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        ON_CALL(*bridge_, readMetaData(_)).WillByDefault(Return(std::vector<StateMetaInfoSnapshot>{makeMetaInfo()}));
        ON_CALL(*bridge_, getSavepointInputStream(_)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, isUsingKeyGroupCompression(_)).WillByDefault(Return(false));
        ON_CALL(*bridge_, closeSavepointInputStream(_)).WillByDefault(Return());
    }

    std::unique_ptr<HeapCompatibleFullRestoreOperation<int>> makeOperation(
        std::vector<std::shared_ptr<KeyedStateHandle>> handles,
        std::unique_ptr<RecordingRestoreAdaptor> adaptor,
        std::shared_ptr<omnistream::OmniTaskBridge> bridge = nullptr)
    {
        return std::make_unique<HeapCompatibleFullRestoreOperation<int>>(
            nullptr,
            &keyGroupRange_,
            std::move(handles),
            nullptr,
            1,
            bridge == nullptr ? bridge_ : bridge,
            makeAdaptorInfo(),
            std::move(adaptor));
    }

    KeyGroupRange keyGroupRange_{0, 0};
    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::vector<std::string> events_;
};

} // namespace

// 看护非空 handles 但 bridge 缺失时 fail-fast，避免 restore iterator 在空 bridge 上读取 metadata。
TEST_F(HeapCompatibleFullRestoreOperationTest, RestoreFailsFastWhenBridgeMissing)
{
    auto operation = std::make_unique<HeapCompatibleFullRestoreOperation<int>>(
        nullptr,
        &keyGroupRange_,
        std::vector<std::shared_ptr<KeyedStateHandle>>{makeKeyedStateHandle()},
        nullptr,
        1,
        nullptr,
        makeAdaptorInfo(),
        std::make_unique<RecordingRestoreAdaptor>(&events_));

    EXPECT_THROW(operation->restore(), std::invalid_argument);
}

// 看护 concrete 分支缺少 prepared adaptor 时 fail-fast，不能静默回退到普通 restore。
TEST_F(HeapCompatibleFullRestoreOperationTest, RestoreFailsFastWhenPreparedAdaptorMissing)
{
    auto operation = std::make_unique<HeapCompatibleFullRestoreOperation<int>>(
        nullptr,
        &keyGroupRange_,
        std::vector<std::shared_ptr<KeyedStateHandle>>{makeKeyedStateHandle()},
        nullptr,
        1,
        bridge_,
        makeAdaptorInfo(),
        nullptr);

    EXPECT_THROW(operation->restore(), std::runtime_error);
}

// 看护 Heap restore operation 只消费 prepared adaptor，按 validate -> restore 顺序调用。
TEST_F(HeapCompatibleFullRestoreOperationTest, RestoreCallsValidateAndRestoreInOrder)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    operation->restore();

    EXPECT_EQ(events_, std::vector<std::string>({"validate", "restore"}));
    EXPECT_TRUE(adaptorPtr->operatorName_.empty());
    EXPECT_EQ(adaptorPtr->validatedMetaCounts_, std::vector<size_t>{1U});
}

// 两个 handle 必须分别交给 adaptor 校验，不能扁平为一次调用。
TEST_F(HeapCompatibleFullRestoreOperationTest, MultipleHandlesWithSameStateAreValidatedIndependently)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle(), makeKeyedStateHandle()}, std::move(adaptor));

    operation->restore();

    EXPECT_EQ(adaptorPtr->validatedMetaCounts_, std::vector<size_t>({1U, 1U}));
    EXPECT_EQ(events_, std::vector<std::string>({"validate", "validate", "restore"}));
    EXPECT_TRUE(adaptorPtr->actualIteratorHadNext_);
}

// 空 metadata handle 必须仍交给 adaptor，并在 restore 前停止。
TEST_F(HeapCompatibleFullRestoreOperationTest, SecondHandleValidationFailurePreventsRestore)
{
    EXPECT_CALL(*bridge_, readMetaData(_))
        .WillOnce(Return(std::vector<StateMetaInfoSnapshot>{makeMetaInfo()}))
        .WillOnce(Return(std::vector<StateMetaInfoSnapshot>{}));

    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    adaptor->failOnEmptyMetadata_ = true;
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle(), makeKeyedStateHandle()}, std::move(adaptor));

    EXPECT_THROW(operation->restore(), std::runtime_error);
    EXPECT_EQ(adaptorPtr->validatedMetaCounts_, std::vector<size_t>({1U, 0U}));
    EXPECT_EQ(events_, std::vector<std::string>({"validate", "validate"}));
}

// 看护 metadata probe 使用独立 iterator，不会消费传给 adaptor restore 的实际 restore iterator。
TEST_F(HeapCompatibleFullRestoreOperationTest, MetadataProbeDoesNotConsumeActualRestoreIterator)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    operation->restore();

    EXPECT_TRUE(adaptorPtr->actualIteratorHadNext_);
}

// 看护 adaptor restore 抛出的异常原样透传，operation 不吞掉失败。
TEST_F(HeapCompatibleFullRestoreOperationTest, RestoreFailureRethrows)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    adaptor->throwOnRestore_ = true;
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    EXPECT_THROW(operation->restore(), std::runtime_error);
}

// 看护空 handles 按既有语义直接返回，不要求 bridge 或 adaptor 存在。
TEST_F(HeapCompatibleFullRestoreOperationTest, EmptyHandlesReturnWithoutWritingWhenNoPriorState)
{
    auto operation = std::make_unique<HeapCompatibleFullRestoreOperation<int>>(
        nullptr,
        &keyGroupRange_,
        std::vector<std::shared_ptr<KeyedStateHandle>>{},
        nullptr,
        1,
        nullptr,
        makeAdaptorInfo(),
        nullptr);

    EXPECT_NO_THROW(operation->restore());
}

// 看护 validate 失败时不会进入 restore 阶段，避免发布部分恢复的 construction backend。
TEST_F(HeapCompatibleFullRestoreOperationTest, OperationDoesNotPublishBackendOnFailure)
{
    auto adaptor = std::make_unique<RecordingRestoreAdaptor>(&events_);
    adaptor->throwOnValidate_ = true;
    auto operation = makeOperation({makeKeyedStateHandle()}, std::move(adaptor));

    EXPECT_THROW(operation->restore(), std::runtime_error);
    EXPECT_EQ(events_, std::vector<std::string>({"validate"}));
}

// 看护 Heap builder 在 compatible + None 时早失败，错误保留 unsupported reason。
TEST_F(HeapCompatibleFullRestoreOperationTest, HeapBuilderCompatibleNoneFailsBeforeRestoreWrite)
{
    std::set<std::shared_ptr<KeyedStateHandle>> handles{makeKeyedStateHandle()};
    HeapKeyedStateBackendBuilder<int> builder(nullptr, 1, &keyGroupRange_);
    builder.setStateHandles(handles)
        .setRestoreSavepointMode(RestoreSavepointMode::FLINK_COMPATIBLE)
        .setFlinkSavepointAdaptorInfo({FlinkSavepointAdaptorType::None, "unsupported"});

    EXPECT_THROW(builder.build(), std::runtime_error);
}

// 看护 concrete compatible restore 缺少 bridge 时在 builder 阶段失败，避免创建 construction backend。
TEST_F(HeapCompatibleFullRestoreOperationTest, HeapBuilderCompatibleRestoreFailsBeforeBackendCreationWhenBridgeMissing)
{
    std::set<std::shared_ptr<KeyedStateHandle>> handles{makeKeyedStateHandle()};
    HeapKeyedStateBackendBuilder<int> builder(nullptr, 1, &keyGroupRange_);
    builder.setStateHandles(handles)
        .setRestoreSavepointMode(RestoreSavepointMode::FLINK_COMPATIBLE)
        .setFlinkSavepointAdaptorInfo(makeAdaptorInfo());

    EXPECT_THROW(builder.build(), std::invalid_argument);
}
