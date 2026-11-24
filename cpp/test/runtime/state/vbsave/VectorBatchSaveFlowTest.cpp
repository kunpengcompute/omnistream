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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "core/memory/DataInputDeserializer.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/vbsave/VectorBatchSaveFlow.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::ReturnRef;

namespace {

// ============================================================================
// MockKeyValueStateIterator — 模拟控制迭代器行为
// ============================================================================

class MockKeyValueStateIterator : public KeyValueStateIterator {
public:
    MOCK_METHOD(void, next, (), (override));
    MOCK_METHOD(int, keyGroup, (), (const, override));
    MOCK_METHOD(ByteView, key, (), (const, override));
    MOCK_METHOD(ByteView, value, (), (const, override));
    MOCK_METHOD(int, kvStateId, (), (const, override));
    MOCK_METHOD(const CurrentEntry&, current, (), (const, override));
    MOCK_METHOD(bool, isNewKeyValueState, (), (const, override));
    MOCK_METHOD(bool, isNewKeyGroup, (), (const, override));
    MOCK_METHOD(bool, isValid, (), (const, override));
    MOCK_METHOD(void, close, (), (override));
};

// ============================================================================
// MockFullSnapshotResources — 模拟 FullSnapshotResources
// ============================================================================

class MockFullSnapshotResources : public FullSnapshotResources {
public:
    MOCK_METHOD(const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&, getMetaInfoSnapshots, (), (override));
    MOCK_METHOD(KeyGroupRange*, getKeyGroupRange, (), (override));
    MOCK_METHOD(TypeSerializer*, getKeySerializer, (), (override));
    MOCK_METHOD(std::shared_ptr<KeyValueStateIterator>, createKVStateIterator, (), (override));
    MOCK_METHOD(bool, isHeapPriorityQueueStateId, (int), (const, override));
    MOCK_METHOD(int, getKeyGroupPrefixBytes, (), (const, override));
    MOCK_METHOD(void, cleanup, (), (override));
};

class EmptySaveHooks : public omnistream::VectorBatchSaveHooks {
public:
    std::vector<omnistream::VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources&, const omnistream::VectorBatchSavePlan&) override
    {
        return {};
    }

    template <typename Emit>
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry&,
        const omnistream::VectorBatchSaveStateContext&,
        const omnistream::VectorBatchSavePlan&,
        Emit&&)
    {
    }
};

class PassThroughSaveHooks : public omnistream::VectorBatchSaveHooks {
public:
    explicit PassThroughSaveHooks(std::shared_ptr<omnistream::VectorBatchSaveStateContext> context)
        : context_(std::move(context))
    {
    }

    std::vector<omnistream::VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources&, const omnistream::VectorBatchSavePlan&) override
    {
        std::vector<omnistream::VectorBatchSaveStateContext> result;
        result.push_back(std::move(*context_));
        return result;
    }

    template <typename Emit>
    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry&,
        const omnistream::VectorBatchSaveStateContext&,
        const omnistream::VectorBatchSavePlan&,
        Emit&&)
    {
    }

private:
    std::shared_ptr<omnistream::VectorBatchSaveStateContext> context_;
};

// ============================================================================
// Test fixture
// ============================================================================

class VectorBatchSaveFlowTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        ON_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, CreateSavepointOutputDirectBuffer(_, _)).WillByDefault(Return(kMockDirectBuffer));
        ON_CALL(*bridge_, GetSavepointOutputStreamPos(_)).WillByDefault(Return(0LL));

        savepointType_ = SavepointType::savepoint(SavepointFormatType::CANONICAL);
        checkpointOptions_ =
            CheckpointOptions::AlignedNoTimeout(*savepointType_, CheckpointStorageLocationReference::GetDefault());
    }

    void TearDown() override
    {
        delete checkpointOptions_;
        checkpointOptions_ = nullptr;
    }

    std::shared_ptr<StateMetaInfoSnapshot> makeMetaInfo(const std::string& name)
    {
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            std::unordered_map<std::string, std::string>{},
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::shared_ptr<SavepointType> savepointType_;
    CheckpointOptions* checkpointOptions_ = nullptr;
};

// ============================================================================
// 错误路径：createKVStateIterator 返回 nullptr
// ============================================================================

TEST_F(VectorBatchSaveFlowTest, ExecuteSaveFailsWhenIteratorIsNull)
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange, {0LL});
    CheckpointStateOutputStreamProxy stream(bridge_, 1L, checkpointOptions_);

    omnistream::VectorBatchSavePlan plan;
    plan.targetMetaInfos = {makeMetaInfo("testState")};
    plan.mainStateIds = {0};

    EmptySaveHooks hooks;
    MockFullSnapshotResources resources;
    EXPECT_CALL(resources, createKVStateIterator()).WillOnce(Return(nullptr));

    EXPECT_THROW(
        omnistream::VectorBatchSaveFlow::executeSave(hooks, plan, stream, offsets, resources, ""), std::runtime_error);
}

// ============================================================================
// 错误路径：buildSaveStateContexts 返回空 ctx 数组，kvStateId=0 超出范围
// ============================================================================

TEST_F(VectorBatchSaveFlowTest, ExecuteSaveFailsWhenKvStateIdOutOfRange)
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange, {0LL});
    CheckpointStateOutputStreamProxy stream(bridge_, 1L, checkpointOptions_);

    omnistream::VectorBatchSavePlan plan;
    plan.targetMetaInfos = {makeMetaInfo("testState")};
    plan.mainStateIds = {0};

    EmptySaveHooks hooks;
    MockFullSnapshotResources resources;

    // 有效迭代器，entry.kvStateId = 0
    auto mockIterator = std::make_shared<NiceMock<MockKeyValueStateIterator>>();
    const auto key = std::vector<int8_t>{0x10, 0x20};
    const auto value = std::vector<int8_t>{0x01};
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(key.data(), key.size());
    entry.value = ByteView(value.data(), value.size());
    entry.keyGroup = 0;
    entry.kvStateId = 0;
    entry.newKeyGroup = false;
    entry.newKeyValueState = false;

    // 进入循环后 getContext 抛出异常，catch 块中会调用 close()，但 isValid 只被调用一次
    EXPECT_CALL(*mockIterator, isValid()).WillOnce(Return(true));
    EXPECT_CALL(*mockIterator, current()).WillRepeatedly(ReturnRef(entry));
    EXPECT_CALL(*mockIterator, next()).Times(0);
    EXPECT_CALL(*mockIterator, close()).Times(1);
    EXPECT_CALL(resources, createKVStateIterator()).WillOnce(Return(mockIterator));

    // buildSaveStateContexts 返回空数组，kvStateId=0 超出范围
    EXPECT_THROW(
        omnistream::VectorBatchSaveFlow::executeSave(hooks, plan, stream, offsets, resources, ""), std::runtime_error);
}

// ============================================================================
// 正常路径：KV 状态透传，entry 写入 stream
// ============================================================================

TEST_F(VectorBatchSaveFlowTest, ExecuteSavePassesThroughKvStateEntry)
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange, {0LL});
    CheckpointStateOutputStreamProxy stream(bridge_, 1L, checkpointOptions_);

    omnistream::VectorBatchSavePlan plan;
    plan.targetMetaInfos = {makeMetaInfo("testState")};
    plan.mainStateIds = {0};

    // 为 kvStateId=0 提供有效上下文
    auto ctx = std::make_shared<omnistream::VectorBatchSaveStateContext>();
    ctx->writable = true;
    ctx->mappedKvStateId = 0;
    ctx->stateType = omnistream::VectorBatchStateType::KV;

    PassThroughSaveHooks hooks(ctx);
    MockFullSnapshotResources resources;

    auto mockIterator = std::make_shared<NiceMock<MockKeyValueStateIterator>>();
    const auto key = std::vector<int8_t>{0x10, 0x20};
    const auto value = std::vector<int8_t>{0x01};
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(key.data(), key.size());
    entry.value = ByteView(value.data(), value.size());
    entry.keyGroup = 0;
    entry.kvStateId = 0;
    entry.newKeyGroup = false;
    entry.newKeyValueState = false;

    EXPECT_CALL(*mockIterator, isValid()).WillOnce(Return(true)).WillOnce(Return(false));
    EXPECT_CALL(*mockIterator, current()).WillRepeatedly(ReturnRef(entry));
    EXPECT_CALL(*mockIterator, next()).Times(1);
    EXPECT_CALL(*mockIterator, close()).Times(1);
    EXPECT_CALL(resources, createKVStateIterator()).WillOnce(Return(mockIterator));
    EXPECT_CALL(*bridge_, WriteSavepointOutputStreamDirect(_, _, _)).Times(testing::AtLeast(0));

    // 正常执行不应抛出异常
    EXPECT_NO_THROW(omnistream::VectorBatchSaveFlow::executeSave(hooks, plan, stream, offsets, resources, ""));
}

// ============================================================================
// 正常路径：非 mainStateIds 的 entry 被跳过
// ============================================================================

TEST_F(VectorBatchSaveFlowTest, ExecuteSaveSkipsNonMainStateEntries)
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange, {0LL});
    CheckpointStateOutputStreamProxy stream(bridge_, 1L, checkpointOptions_);

    omnistream::VectorBatchSavePlan plan;
    plan.targetMetaInfos = {makeMetaInfo("testState")};
    plan.mainStateIds = {0}; // 只关心 kvStateId=0

    auto ctx = std::make_shared<omnistream::VectorBatchSaveStateContext>();
    ctx->writable = true;
    ctx->mappedKvStateId = 0;
    ctx->stateType = omnistream::VectorBatchStateType::KV;

    PassThroughSaveHooks hooks(ctx);
    MockFullSnapshotResources resources;

    auto mockIterator = std::make_shared<NiceMock<MockKeyValueStateIterator>>();
    const auto key = std::vector<int8_t>{0x10, 0x20};
    const auto value = std::vector<int8_t>{0x01};

    // 第一个 entry：kvStateId=1（非 mainStateId），第二个 entry：kvStateId=0（mainStateId）
    KeyValueStateIterator::CurrentEntry entry1;
    entry1.key = ByteView(key.data(), key.size());
    entry1.value = ByteView(value.data(), value.size());
    entry1.keyGroup = 0;
    entry1.kvStateId = 1;
    entry1.newKeyGroup = false;
    entry1.newKeyValueState = false;

    KeyValueStateIterator::CurrentEntry entry2;
    entry2.key = ByteView(key.data(), key.size());
    entry2.value = ByteView(value.data(), value.size());
    entry2.keyGroup = 0;
    entry2.kvStateId = 0;
    entry2.newKeyGroup = false;
    entry2.newKeyValueState = false;

    // 三次 isValid：true, true, false
    EXPECT_CALL(*mockIterator, isValid()).WillOnce(Return(true)).WillOnce(Return(true)).WillOnce(Return(false));
    // 第一次 current() 返回 entry1（跳过），第二次返回 entry2（处理）
    EXPECT_CALL(*mockIterator, current()).WillOnce(ReturnRef(entry1)).WillOnce(ReturnRef(entry2));
    EXPECT_CALL(*mockIterator, next()).Times(2);
    EXPECT_CALL(*mockIterator, close()).Times(1);
    EXPECT_CALL(resources, createKVStateIterator()).WillOnce(Return(mockIterator));
    EXPECT_CALL(*bridge_, WriteSavepointOutputStreamDirect(_, _, _)).Times(testing::AtLeast(0));

    EXPECT_NO_THROW(omnistream::VectorBatchSaveFlow::executeSave(hooks, plan, stream, offsets, resources, ""));
}

} // namespace
