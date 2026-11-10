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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CompatibleFullSnapshotAsyncWriter.h"
#include "runtime/state/CompatibleSavepointSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyGroupsSavepointStateHandle.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "test/runtime/state/CompatibleSavepointTestUtils.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::Throw;

namespace {

class RecordingAdaptor : public omnistream::OperatorSavepointAdaptor {
public:
    explicit RecordingAdaptor(std::vector<std::string>* events) : events_(events)
    {
    }

    void validateForSave(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& metaInfos) override
    {
        events_->push_back("validate");
        validatedMetaCount_ = metaInfos.size();
        if (rejectEmptyMeta_ && metaInfos.empty()) {
            throw std::runtime_error("required source state missing");
        }
        if (throwOnValidate_) {
            throw std::runtime_error("validate failed");
        }
    }

    void save(
        CheckpointStateOutputStreamProxy&,
        KeyGroupRangeOffsets& keyGroupOffsets,
        FullSnapshotResources& snapshotResources,
        std::string keySerializer) override
    {
        events_->push_back("save");
        sawSave_ = true;
        keySerializer_ = keySerializer;
        keyGroupStart_ = keyGroupOffsets.getKeyGroupRange().getStartKeyGroup();
        keyGroupEnd_ = keyGroupOffsets.getKeyGroupRange().getEndKeyGroup();
        snapshotResources_ = &snapshotResources;
        if (throwOnSave_) {
            throw std::runtime_error("save failed");
        }
    }

    void restore(SavepointRestoreResultIterator&, omnistream::RestoreBackendDelegate&) override
    {
    }

    std::vector<std::string>* events_;
    bool rejectEmptyMeta_ = false;
    bool throwOnValidate_ = false;
    bool throwOnSave_ = false;
    bool sawSave_ = false;
    size_t validatedMetaCount_ = 0;
    std::string keySerializer_;
    int keyGroupStart_ = -1;
    int keyGroupEnd_ = -1;
    FullSnapshotResources* snapshotResources_ = nullptr;
};

std::shared_ptr<SnapshotResult<StreamStateHandle>> makeStreamResult()
{
    auto streamHandle = std::make_shared<ByteStreamStateHandle>("compatible-savepoint", std::vector<uint8_t>{1, 2});
    return SnapshotResult<StreamStateHandle>::Of(streamHandle);
}

class CompatibleFullSnapshotAsyncWriterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        savepointType_ = SavepointType::savepoint(SavepointFormatType::COMPATIBLE);
        checkpointOptions_.reset(
            CheckpointOptions::AlignedNoTimeout(*savepointType_, CheckpointStorageLocationReference::GetDefault()));

        ON_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, CreateSavepointOutputDirectBuffer(_, _)).WillByDefault(Return(kMockDirectBuffer));
        ON_CALL(*bridge_, ReleaseSavepointOutputDirectBuffer(_)).WillByDefault(Return());
        ON_CALL(*bridge_, WriteSavepointOutputStreamDirect(_, _, _)).WillByDefault(Return(true));
        ON_CALL(*bridge_, CloseSavepointOutputStream(_)).WillByDefault(Return(makeStreamResult()));
    }

    std::shared_ptr<CompatibleSavepointSnapshotResources> makeResources(
        std::shared_ptr<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources> source,
        std::unique_ptr<RecordingAdaptor> adaptor)
    {
        return std::make_shared<CompatibleSavepointSnapshotResources>(
            source, std::move(adaptor), compatible_savepoint_test::makeCompatibleTestAdaptorInfo());
    }

    std::unique_ptr<CompatibleFullSnapshotAsyncWriter> makeWriter(
        std::shared_ptr<CompatibleSavepointSnapshotResources> resources, std::unique_ptr<RecordingAdaptor> adaptor)
    {
        return std::make_unique<CompatibleFullSnapshotAsyncWriter>(
            42L, checkpointOptions_.get(), std::move(resources), "key-ser", std::move(adaptor));
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::shared_ptr<SavepointType> savepointType_;
    std::unique_ptr<CheckpointOptions> checkpointOptions_;
    std::vector<std::string> events_;
};

} // namespace

// 看护 required-state adaptor 会拒绝空 metadata，并保留原始异常且不打开输出流。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, EmptyMetaRejectedByAdaptorBeforeOpeningStream)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(false);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    adaptor->rejectEmptyMeta_ = true;
    auto* adaptorPtr = adaptor.get();
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).Times(0);
    try {
        writer->get(bridge_);
        FAIL() << "Expected empty metadata validation to fail";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "required source state missing");
    }

    EXPECT_EQ(events_, std::vector<std::string>({"validate"}));
    EXPECT_EQ(adaptorPtr->validatedMetaCount_, 0U);
    EXPECT_FALSE(adaptorPtr->sawSave_);
}

// 看护 permissive adaptor 可接受空 metadata，validation 恰好一次且不打开 stream、不调用 save。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, EmptyMetaAllowedReturnsEmptyWithoutOpeningStreamOrSaving)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(false);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).Times(0);
    auto result = writer->get(bridge_);

    EXPECT_EQ(result->GetJobManagerOwnedSnapshot(), nullptr);
    EXPECT_EQ(result->GetTaskLocalSnapshot(), nullptr);
    EXPECT_EQ(events_, std::vector<std::string>({"validate"}));
    EXPECT_EQ(adaptorPtr->validatedMetaCount_, 0U);
    EXPECT_FALSE(adaptorPtr->sawSave_);
}

// 看护空 metadata 的成功路径无需 bridge，仍执行一次 validation 且不调用 save。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, EmptyMetaAllowedDoesNotRequireBridge)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(false);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    auto result = writer->get(nullptr);

    EXPECT_EQ(result->GetJobManagerOwnedSnapshot(), nullptr);
    EXPECT_EQ(result->GetTaskLocalSnapshot(), nullptr);
    EXPECT_EQ(events_, std::vector<std::string>({"validate"}));
    EXPECT_EQ(adaptorPtr->validatedMetaCount_, 0U);
    EXPECT_FALSE(adaptorPtr->sawSave_);
}

// 看护 writer 在 validate 失败时不会打开 savepoint 输出流，避免产生半成品 stream。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, GetCallsValidateBeforeOpeningStream)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(true, 0, 1);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    adaptor->throwOnValidate_ = true;
    auto* adaptorPtr = adaptor.get();
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).Times(0);
    try {
        writer->get(bridge_);
        FAIL() << "expected adaptor validation error";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "validate failed");
    }

    ASSERT_EQ(events_.size(), 1U);
    EXPECT_EQ(events_[0], "validate");
    EXPECT_FALSE(adaptorPtr->sawSave_);
}

// 看护 writer 按 validate -> save -> close 生命周期传入转换参数、封装 state handle，且不直接 cleanup resources。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, GetCallsSaveWithOpenedStreamAndOffsets)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(true, 0, 1);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_CALL(*bridge_, CloseSavepointOutputStream(kMockProvider)).Times(1);
    auto result = writer->get(bridge_);

    ASSERT_NE(result->GetJobManagerOwnedSnapshot(), nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<KeyGroupsSavepointStateHandle>(result->GetJobManagerOwnedSnapshot()), nullptr);
    ASSERT_EQ(events_.size(), 2U);
    EXPECT_EQ(events_[0], "validate");
    EXPECT_EQ(events_[1], "save");
    EXPECT_TRUE(adaptorPtr->sawSave_);
    EXPECT_EQ(adaptorPtr->validatedMetaCount_, 1U);
    EXPECT_EQ(adaptorPtr->keySerializer_, "key-ser");
    EXPECT_EQ(adaptorPtr->keyGroupStart_, 0);
    EXPECT_EQ(adaptorPtr->keyGroupEnd_, 1);
    EXPECT_EQ(adaptorPtr->snapshotResources_, source.get());
    EXPECT_EQ(source->cleanupCount(), 0);
}

// 看护 save 失败时 abort 未 finalization 的 provider，并保留 adaptor 的原始异常。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, SaveThrowsAbortsUnfinalizedProviderAndRethrows)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(true, 0, 1);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    adaptor->throwOnSave_ = true;
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_CALL(*bridge_, AbortSavepointOutputStream(kMockProvider)).Times(1);
    EXPECT_CALL(*bridge_, CloseSavepointOutputStream(_)).Times(0);
    try {
        writer->get(bridge_);
        FAIL() << "expected adaptor save error";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "save failed");
    }

    ASSERT_EQ(events_.size(), 2U);
    EXPECT_EQ(events_[0], "validate");
    EXPECT_EQ(events_[1], "save");
}

// 看护 abort cleanup 自身失败时，writer 仍向调用方保留 adaptor save 的原始错误。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, SaveErrorSurvivesAbortFailure)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(true, 0, 1);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    adaptor->throwOnSave_ = true;
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_CALL(*bridge_, AbortSavepointOutputStream(kMockProvider))
        .WillOnce(Throw(std::runtime_error("abort failed")));
    EXPECT_CALL(*bridge_, CloseSavepointOutputStream(_)).Times(0);

    try {
        writer->get(bridge_);
        FAIL() << "expected adaptor save error";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "save failed");
    }
}

// 看护 close 返回空结果时 fail-fast，不能把 compatible savepoint 伪装成 empty snapshot。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, CloseReturningNullFailsFast)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(true, 0, 1);
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::make_unique<RecordingAdaptor>(&events_));

    EXPECT_CALL(*bridge_, CloseSavepointOutputStream(kMockProvider)).WillOnce(Return(nullptr));
    EXPECT_CALL(*bridge_, AbortSavepointOutputStream(_)).Times(0);
    EXPECT_THROW(writer->get(bridge_), std::runtime_error);
}

// 看护非空 metadata 先完成一次 validation，再在打开 stream 前拒绝缺失 bridge。
TEST_F(CompatibleFullSnapshotAsyncWriterTest, NullBridgeFailsAfterValidationWithoutOpeningStream)
{
    auto source = std::make_shared<compatible_savepoint_test::CompatibleSavepointTestFullSnapshotResources>(true, 0, 1);
    auto adaptor = std::make_unique<RecordingAdaptor>(&events_);
    auto* adaptorPtr = adaptor.get();
    auto resources = makeResources(source, std::make_unique<RecordingAdaptor>(&events_));
    auto writer = makeWriter(resources, std::move(adaptor));

    EXPECT_THROW(writer->get(nullptr), std::invalid_argument);
    EXPECT_EQ(events_, std::vector<std::string>({"validate"}));
    EXPECT_EQ(adaptorPtr->validatedMetaCount_, 1U);
    EXPECT_FALSE(adaptorPtr->sawSave_);
}
