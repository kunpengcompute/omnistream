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
#include <gmock/gmock.h>
#include <jni.h>
#include <memory>
#include <stdexcept>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/SavepointKvStreamWriter.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

namespace {

/**
 * SavepointKvStreamWriter 测试夹具。
 * 使用真实 CheckpointStateOutputStreamProxy + mock bridge 组合，
 * 同时覆盖 writer 状态机和 proxy 的 patchable/buffered 路径。
 */
class SavepointKvStreamWriterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        bridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        savepointType_ = SavepointType::savepoint(SavepointFormatType::CANONICAL);
        options_ =
            CheckpointOptions::AlignedNoTimeout(*savepointType_, CheckpointStorageLocationReference::GetDefault());

        ON_CALL(*bridge_, AcquireSavepointOutputStream(_, _)).WillByDefault(Return(kMockProvider));
        ON_CALL(*bridge_, CreateSavepointOutputDirectBuffer(_, _)).WillByDefault(Return(kMockDirectBuffer));
        ON_CALL(*bridge_, WriteSavepointOutputStreamDirect(_, _, _)).WillByDefault(Return(true));
        ON_CALL(*bridge_, ReleaseSavepointOutputDirectBuffer(_)).WillByDefault(Return());
        ON_CALL(*bridge_, WriteSavepointOutputStream(_, _, _, _)).WillByDefault(Return());
        ON_CALL(*bridge_, WriteSavepointMetadata(_, _, _)).WillByDefault(Return());
        ON_CALL(*bridge_, GetSavepointOutputStreamPos(_)).WillByDefault(Return(0));

        kgRange_ = std::make_unique<KeyGroupRange>(0, 2);
        kgOffsets_ = std::make_unique<KeyGroupRangeOffsets>(*kgRange_);
        proxy_ = std::make_unique<CheckpointStateOutputStreamProxy>(bridge_, 1L, options_);
    }

    void TearDown() override
    {
        // close 会 flush，若有未 release 的 BytePatch 会抛异常，try/catch 处理
        try {
            proxy_->close();
        } catch (...) {
        }
        proxy_.reset();
        kgOffsets_.reset();
        kgRange_.reset();
        delete options_;
        options_ = nullptr;
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> bridge_;
    std::shared_ptr<SavepointType> savepointType_;
    CheckpointOptions* options_ = nullptr;
    std::unique_ptr<KeyGroupRange> kgRange_;
    std::unique_ptr<KeyGroupRangeOffsets> kgOffsets_;
    std::unique_ptr<CheckpointStateOutputStreamProxy> proxy_;
};

/**
 * writeFirst 设置首条 KV 的 keyGroup 偏移量为当前 stream 起始位置（0）。
 */
TEST_F(SavepointKvStreamWriterTest, WriteFirstSetsKeyGroupOffset)
{
    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);

    const int8_t key[] = {10, 20};
    const int8_t value[] = {30};
    writer.writeFirst(ByteView::fromBuffer(key, 2), ByteView::fromBuffer(value, 1), 5, 1);

    // 首条 entry 从 stream 起始写入，offset 为 0
    EXPECT_EQ(kgOffsets_->getKeyGroupOffset(1), 0);
}

/**
 * writeFirst 的 key 为空时应抛出 std::runtime_error。
 */
TEST_F(SavepointKvStreamWriterTest, WriteFirstEmptyKeyThrows)
{
    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);

    const int8_t value[] = {30};
    EXPECT_THROW(writer.writeFirst(ByteView(), ByteView::fromBuffer(value, 1), 5, 1), std::runtime_error);
}

/**
 * writeNext 同 keyGroup 同 kvState：不写入头部标记，仅写入 KV 数据。
 * pos 增量应精确等于编码后的 KV 长度。
 */
TEST_F(SavepointKvStreamWriterTest, WriteNextSameKeyGroupSameKvState)
{
    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);

    const int8_t key[] = {10};
    const int8_t value[] = {30};
    writer.writeFirst(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 1);
    size_t posAfterFirst = proxy_->getPos();

    writer.writeNext(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 1, false, false);

    // 无标记写入，仅追加 encodeLen = sizeof(int32_t) + key.size() + sizeof(int32_t) + value.size()
    size_t encodedLen = sizeof(int32_t) + 1 + sizeof(int32_t) + 1;
    EXPECT_EQ(proxy_->getPos(), posAfterFirst + encodedLen);
}

/**
 * writeNext 切换至新 keyGroup：先写入 END_OF_KEY_GROUP_MASK，再设置新偏移量。
 */
TEST_F(SavepointKvStreamWriterTest, WriteNextNewKeyGroup)
{
    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);

    const int8_t key[] = {10};
    const int8_t value[] = {30};
    writer.writeFirst(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 0);

    writer.writeNext(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 1, true, true);

    // 新 keyGroup 偏移量 = writeShort(kvStateId) + encodedLen + writeShort(END_MASK)
    size_t encodedLen = sizeof(int32_t) + 1 + sizeof(int32_t) + 1;
    size_t expectedOffset = sizeof(int16_t) + encodedLen + sizeof(int16_t);
    EXPECT_EQ(kgOffsets_->getKeyGroupOffset(1), expectedOffset);
}

/**
 * writeNext 同 keyGroup 但新 kvState：写入 kvStateId 头部。
 */
TEST_F(SavepointKvStreamWriterTest, WriteNextNewKvStateSameKeyGroup)
{
    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);

    const int8_t key[] = {10};
    const int8_t value[] = {30};
    writer.writeFirst(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 0);

    size_t posBefore = proxy_->getPos();
    writer.writeNext(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 6, 0, false, true);

    // 仅写入 kvStateId (writeShort) + 编码后的 KV
    size_t encodedLen = sizeof(int32_t) + 1 + sizeof(int32_t) + 1;
    size_t expectedDelta = sizeof(int16_t) + encodedLen;
    EXPECT_EQ(proxy_->getPos(), posBefore + expectedDelta);
}

/**
 * 多条目写入后 finish 应修补最后一个 pending entry 并追加 END_OF_KEY_GROUP_MASK（2 字节）。
 */
TEST_F(SavepointKvStreamWriterTest, FinishMultiEntry)
{
    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);

    const int8_t key[] = {10};
    const int8_t value[] = {30};
    writer.writeFirst(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 0);
    writer.writeNext(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 0, false, false);
    writer.writeNext(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), 5, 1, true, false);

    size_t posBeforeFinish = proxy_->getPos();
    writer.finish();

    // finish 仅追加 END_OF_KEY_GROUP_MASK (writeShort, 2 字节)
    EXPECT_EQ(proxy_->getPos(), posBeforeFinish + sizeof(int16_t));
}

/**
 * 无 pending entry 时再次 finish 应为空操作，pos 不变。
 */
TEST_F(SavepointKvStreamWriterTest, FinishNoPendingIsNoop)
{
    proxy_->flush();

    SavepointKvStreamWriter writer(*proxy_, *kgOffsets_);
    writer.writeFirst(
        ByteView::fromBuffer(reinterpret_cast<const int8_t*>("k"), 1),
        ByteView::fromBuffer(reinterpret_cast<const int8_t*>("v"), 1),
        1,
        0);
    writer.finish();

    size_t posBefore = proxy_->getPos();
    writer.finish();
    EXPECT_EQ(proxy_->getPos(), posBefore);
}

} // namespace
