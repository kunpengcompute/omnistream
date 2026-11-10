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
#include <gmock/gmock.h>
#include <jni.h>
#include <memory>
#include <stdexcept>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;
using ::testing::Invoke;
using ::testing::Throw;

namespace {

/**
 * CheckpointStateOutputStreamProxy 测试夹具。
 * 使用共享 MockSavepointBridge + CheckpointOptions 初始化。
 * NiceMock 自动静默未预期的析构调用（ReleaseSavepointOutputDirectBuffer 等），
 * 各测试仅对需要验证的方法设置 EXPECT_CALL。
 */
class CheckpointStateOutputStreamProxyTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        mockBridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        savepointType_ = SavepointType::savepoint(SavepointFormatType::CANONICAL);
        checkpointOptions_ =
            CheckpointOptions::AlignedNoTimeout(*savepointType_, CheckpointStorageLocationReference::GetDefault());
    }

    void TearDown() override
    {
        delete checkpointOptions_;
        checkpointOptions_ = nullptr;
    }

    /** 配置 mock 以支持 Proxy 正常构造：Acquire 返回有效 provider。 */
    void ExpectProxyConstruction()
    {
        EXPECT_CALL(*mockBridge_, AcquireSavepointOutputStream(_, _)).WillOnce(Return(kMockProvider));
    }

    /** 配置 mock 以支持 Proxy 在无 DirectByteBuffer 情况下构造（走 fallback 路径）。 */
    void ExpectProxyConstructionNoDirectBuffer()
    {
        EXPECT_CALL(*mockBridge_, AcquireSavepointOutputStream(_, _)).WillOnce(Return(kMockProvider));
        EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
            .WillOnce(Return(static_cast<jobject>(nullptr)));
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> mockBridge_;
    std::shared_ptr<SavepointType> savepointType_;
    CheckpointOptions* checkpointOptions_ = nullptr;
};

// ---- 构造 / 析构 ----

/** Acquire 失败时构造应抛异常。 */
TEST_F(CheckpointStateOutputStreamProxyTest, ConstructThrowsWhenAcquireFails)
{
    EXPECT_CALL(*mockBridge_, AcquireSavepointOutputStream(_, _)).WillOnce(Return(static_cast<jobject>(nullptr)));

    EXPECT_THROW({ CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_); }, std::runtime_error);
}

// ---- 位置追踪 ----

/** writeByte/writeShort/writeInt/writeLong 写入后 pos 正确递增。 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteAdvancesPos)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeByte(0x42);
    EXPECT_EQ(proxy.getPos(), 1);
    proxy.writeShort(0x0102);
    EXPECT_EQ(proxy.getPos(), 3);
    proxy.writeInt(0x01020304);
    EXPECT_EQ(proxy.getPos(), 7);
    proxy.writeLong(0x0102030405060708LL);
    EXPECT_EQ(proxy.getPos(), 15);
}

/** writeBytes 写入后 pos 与写入长度一致。 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteBytesAdvancesPos)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    const int8_t data[] = {1, 2, 3, 4, 5};
    proxy.writeBytes(data, 5);
    EXPECT_EQ(proxy.getPos(), 5);
}

// ---- flush 行为 ----

/** DirectBuffer 路径；两次 write+flush 循环验证 offset_ 重置。 */
TEST_F(CheckpointStateOutputStreamProxyTest, FlushTriggersDirectWriteWhenDirectBufferExists)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(kMockProvider, kMockDirectBuffer, 4))
        .WillOnce(Return(true));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeInt(0x12345678);
    proxy.flush();
    EXPECT_EQ(proxy.getPos(), 4);

    // 二次写入后 flush 仍应为 4 字节，验证 offset_ 已重置
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(kMockProvider, kMockDirectBuffer, 4))
        .WillOnce(Return(true));
    proxy.writeInt(0x9ABCDEF0);
    proxy.flush();
    EXPECT_EQ(proxy.getPos(), 8);
}

/** 无 DirectBuffer 回退路径；两次循环验证 offset_ 重置。 */
TEST_F(CheckpointStateOutputStreamProxyTest, FlushUsesFallbackWhenNoDirectBuffer)
{
    ExpectProxyConstructionNoDirectBuffer();

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeInt(0x12345678);

    {
        ::testing::InSequence seq;
        EXPECT_CALL(*mockBridge_, WriteSavepointOutputStream(kMockProvider, _, 0, 4)).Times(1);
    }
    proxy.flush();
    EXPECT_EQ(proxy.getPos(), 4);

    // 二次写入后 flush 仍应为 4 字节，验证 offset_ 已重置
    {
        ::testing::InSequence seq;
        EXPECT_CALL(*mockBridge_, WriteSavepointOutputStream(kMockProvider, _, 0, 4)).Times(1);
    }
    proxy.writeInt(0x9ABCDEF0);
    proxy.flush();
    EXPECT_EQ(proxy.getPos(), 8);
}

// ---- BytePatch 守卫状态机 ----

/** tryWrite → patchByte → releasePatch → flush；验证 key 首字节被 0x80 标记。 */
TEST_F(CheckpointStateOutputStreamProxyTest, BytePatchPatchAndRelease)
{
    ExpectProxyConstructionNoDirectBuffer();

    const int8_t* flushedData = nullptr;
    size_t flushedLen = 0;
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStream(kMockProvider, _, _, _))
        .WillOnce(Invoke([&flushedData, &flushedLen](jobject, const int8_t* data, size_t, size_t len) {
            flushedData = data;
            flushedLen = len;
        }));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t key[] = {1};
    const int8_t value[] = {2, 3};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    bool ok = proxy.tryWritePatchableKeyValuePair(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 2), patch);
    EXPECT_TRUE(ok);
    EXPECT_TRUE(patch.valid);

    proxy.patchByte(patch, 0x80);
    proxy.releasePatch(patch);
    proxy.flush();

    ASSERT_NE(flushedData, nullptr);
    // KV 序列化格式: [keyLen:4][key:1][valLen:4][val:2]
    // patch offset 位于 sizeof(int32_t) 即 key 首字节
    ASSERT_GE(flushedLen, static_cast<size_t>(5));
    EXPECT_EQ(static_cast<uint8_t>(flushedData[4]), 0x81);
}

/** 两次 tryWrite 无 release 间隔应抛异常。 */
TEST_F(CheckpointStateOutputStreamProxyTest, BytePatchDoubleActivationThrows)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t key1[] = {1};
    const int8_t value1[] = {2};
    CheckpointStateOutputStreamProxy::BytePatch patch1;
    proxy.tryWritePatchableKeyValuePair(ByteView::fromBuffer(key1, 1), ByteView::fromBuffer(value1, 1), patch1);

    const int8_t key2[] = {3};
    const int8_t value2[] = {4};
    CheckpointStateOutputStreamProxy::BytePatch patch2;
    EXPECT_THROW(
        proxy.tryWritePatchableKeyValuePair(ByteView::fromBuffer(key2, 1), ByteView::fromBuffer(value2, 1), patch2),
        std::runtime_error);
}

/** BytePatch 激活期间 flush 被阻断；release 后 flush 成功；已失效 patch 不再可用。 */
TEST_F(CheckpointStateOutputStreamProxyTest, PatchByteAfterFlushThrows)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(_, _, _)).WillRepeatedly(Return(true));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t key[] = {1};
    const int8_t value[] = {2};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    proxy.tryWritePatchableKeyValuePair(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), patch);

    // 激活期间 flush 被阻断
    EXPECT_THROW(proxy.flush(), std::runtime_error);

    // release 后 flush 成功，patch 失效
    proxy.releasePatch(patch);
    proxy.flush();

    // 已失效的 patch 不能再 patchByte
    EXPECT_THROW(proxy.patchByte(patch, 0x80), std::runtime_error);
}

/** BytePatch 异常路径：无效 patch 抛异常、重复 releasePatch 抛异常、激活期间 writeBytes 被阻塞。 */
TEST_F(CheckpointStateOutputStreamProxyTest, BytePatchGuardErrorPaths)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    // 未激活守卫时 patchByte 抛异常
    CheckpointStateOutputStreamProxy::BytePatch invalidPatch;
    EXPECT_THROW(proxy.patchByte(invalidPatch, 0x80), std::runtime_error);

    // 正常激活后重复 releasePatch 抛异常
    const int8_t key[] = {1};
    const int8_t value[] = {2};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    proxy.tryWritePatchableKeyValuePair(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), patch);
    proxy.releasePatch(patch);
    EXPECT_THROW(proxy.releasePatch(patch), std::runtime_error);

    // 激活期间 writeBytes 被阻塞
    proxy.tryWritePatchableKeyValuePair(ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), patch);
    const int8_t data[] = {0x42};
    EXPECT_THROW(proxy.writeBytes(data, 1), std::runtime_error);
}

// ---- KeyValuePair 写入 ----

/**
 * writeKeyValuePair(vector 重载) 写入 key/value 并正确更新 pos。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteKeyValuePairUsesVectorOverload)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    std::vector<int8_t> key = {1, 2};
    std::vector<int8_t> value = {3, 4, 5};
    proxy.writeKeyValuePair(key, value);
    EXPECT_EQ(proxy.getPos(), static_cast<size_t>(4 + 2 + 4 + 3));
}

/**
 * tryWritePatchableKeyValuePair 的 key 为空时应抛异常。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, TryWritePatchableFailsWhenKeyEmpty)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t value[] = {1};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    EXPECT_THROW(
        proxy.tryWritePatchableKeyValuePair(ByteView(), ByteView::fromBuffer(value, 1), patch), std::runtime_error);
}

// ---- close ----

/** 未完成的 stream 离开作用域时，析构必须 abort provider 并释放 DirectByteBuffer。 */
TEST_F(CheckpointStateOutputStreamProxyTest, DestructorAbortsUnclosedProvider)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, AbortSavepointOutputStream(kMockProvider)).Times(1);
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(_)).Times(0);
    EXPECT_CALL(*mockBridge_, ReleaseSavepointOutputDirectBuffer(kMockDirectBuffer)).Times(1);

    {
        CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    }
}

/** abortNoThrow 只 abort 一次并只释放一次 DirectByteBuffer。 */
TEST_F(CheckpointStateOutputStreamProxyTest, AbortNoThrowIsIdempotent)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, AbortSavepointOutputStream(kMockProvider)).Times(1);
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(_)).Times(0);
    EXPECT_CALL(*mockBridge_, ReleaseSavepointOutputDirectBuffer(kMockDirectBuffer)).Times(1);

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.abortNoThrow();
    proxy.abortNoThrow();
}

/** abort bridge 抛异常时仍只尝试一次终态化并释放一次 DirectByteBuffer。 */
TEST_F(CheckpointStateOutputStreamProxyTest, AbortFailureStillReleasesDirectBufferOnce)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, AbortSavepointOutputStream(kMockProvider))
        .WillOnce(Throw(std::runtime_error("abort failed")));
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(_)).Times(0);
    EXPECT_CALL(*mockBridge_, ReleaseSavepointOutputDirectBuffer(kMockDirectBuffer)).Times(1);

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.abortNoThrow();
    proxy.abortNoThrow();
}

/** finalizer 抛异常后 provider 已转移，不能由 abortNoThrow 再次终态化。 */
TEST_F(CheckpointStateOutputStreamProxyTest, CloseExceptionDoesNotAbortSameProvider)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(kMockProvider))
        .WillOnce(Throw(std::runtime_error("finalize failed")));
    EXPECT_CALL(*mockBridge_, AbortSavepointOutputStream(_)).Times(0);
    EXPECT_CALL(*mockBridge_, ReleaseSavepointOutputDirectBuffer(kMockDirectBuffer)).Times(1);

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    EXPECT_THROW(proxy.close(), std::runtime_error);
    proxy.abortNoThrow();
}

/**
 * close 会先 flush 再调用 CloseSavepointOutputStream 并返回非空 handle。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, CloseFlushesAndReturnsHandle)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(_, _, _)).WillOnce(Return(true));
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(kMockProvider))
        .WillOnce(Return(SnapshotResult<StreamStateHandle>::Empty()));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeInt(0xDEADBEEF);

    auto handle = proxy.close();
    EXPECT_NE(handle, nullptr);
}

// ---- writeMetadata / writeOperatorMetaData ----

/**
 * writeMetadata 调用后 pos 应与 bridge 返回的 stream 位置一致。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteMetadataUpdatesPos)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointMetadata(_, _, _)).Times(1);
    EXPECT_CALL(*mockBridge_, GetSavepointOutputStreamPos(kMockProvider)).WillOnce(Return(1024L));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> snapshots;
    proxy.writeMetadata(snapshots, "testSerializer");
    EXPECT_EQ(proxy.getPos(), 1024);
}

/**
 * metadata position 查询失败不是终态操作；离开作用域时必须由析构触发唯一的 abort cleanup。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, MetadataPositionFailureAbortsAtScopeExit)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _)).WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointMetadata(kMockProvider, _, _)).Times(1);
    EXPECT_CALL(*mockBridge_, GetSavepointOutputStreamPos(kMockProvider))
        .WillOnce(Throw(std::runtime_error("position query failed")));
    EXPECT_CALL(*mockBridge_, AbortSavepointOutputStream(kMockProvider)).Times(1);
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(_)).Times(0);
    EXPECT_CALL(*mockBridge_, ReleaseSavepointOutputDirectBuffer(kMockDirectBuffer)).Times(1);

    {
        CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>> snapshots;
        EXPECT_THROW(proxy.writeMetadata(snapshots, "testSerializer"), std::runtime_error);
    }
}

/**
 * prepareForPatchableKeyValuePair 在大容量需求时应触发 growBuffer，
 * growBuffer 内部会 flush 现有数据并通过 WriteSavepointOutputStreamDirect 写出。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, PrepareForPatchableGrowsBuffer)
{
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer))
        .WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(_, _, _))
        .Times(::testing::AtLeast(1))
        .WillRepeatedly(Return(true));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    for (int i = 0; i < 100; i++) {
        proxy.writeInt(0x12345678);
    }
    size_t posBeforePrepare = proxy.getPos();

    proxy.prepareForPatchableKeyValuePair(128 * 1024);

    EXPECT_EQ(proxy.getPos(), posBeforePrepare);
}

} // namespace
