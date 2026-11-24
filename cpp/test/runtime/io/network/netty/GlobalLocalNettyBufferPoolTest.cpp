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
#include <thread>
#include <atomic>
#include "runtime/io/network/netty/GlobalNettyBufferPool.h"
#include "runtime/io/network/netty/LocalNettyBufferPool.h"
#include "runtime/io/network/netty/NettyBufferConf.h"

using namespace omnistream;

namespace {

void RecycleSingleReference(LocalNettyBufferPool& pool, const std::shared_ptr<NettyMemorySegment>& buffer)
{
    buffer->IncreaseRefCount();
    buffer->EnableEligibleRecycling();
    pool.recycleBuffer(reinterpret_cast<long>(buffer->GetOriginalAddress()));
}

} // namespace

// --- GlobalNettyBufferPool Tests ---

TEST(GlobalNettyBufferPoolTest, Creation)
{
    NettyBufferConf conf(100, 1024, 10, 50);
    GlobalNettyBufferPool globalPool(conf);

    EXPECT_EQ(globalPool.getTotalBufferCount(), 100);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 1);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 0);
    EXPECT_EQ(globalPool.getBufferSize(), 1024);
    EXPECT_FALSE(globalPool.isDestroyed());
}

TEST(GlobalNettyBufferPoolTest, RequestAndRecycle)
{
    NettyBufferConf conf(10, 1024, 5, 10);
    GlobalNettyBufferPool globalPool(conf);

    auto buffer = globalPool.requestPooledBuffer();
    EXPECT_NE(buffer, nullptr);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 0);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 1);

    globalPool.recyclePooledBuffer(buffer);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 1);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 0);
}

TEST(GlobalNettyBufferPoolTest, RequestReturnsNullWhenEmpty)
{
    NettyBufferConf conf(2, 1024, 1, 2);
    GlobalNettyBufferPool globalPool(conf);

    auto b1 = globalPool.requestPooledBuffer();
    auto b2 = globalPool.requestPooledBuffer();
    auto b3 = globalPool.requestPooledBuffer();

    EXPECT_NE(b1, nullptr);
    EXPECT_NE(b2, nullptr);
    EXPECT_EQ(b3, nullptr);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 0);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 2);

    globalPool.recyclePooledBuffer(b1);
    globalPool.recyclePooledBuffer(b2);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 2);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 0);
}

TEST(GlobalNettyBufferPoolTest, AllocatesRegularBuffersOnDemandUpToCapacity)
{
    NettyBufferConf conf(10, 1024, 5, 10);
    GlobalNettyBufferPool globalPool(conf);

    EXPECT_EQ(globalPool.getAvailableBufferCount(), 1);

    std::vector<std::shared_ptr<NettyMemorySegment>> buffers;
    for (int i = 0; i < 10; ++i) {
        auto buffer = globalPool.requestPooledBuffer();
        EXPECT_NE(buffer, nullptr);
        buffers.push_back(buffer);
    }

    EXPECT_EQ(globalPool.requestPooledBuffer(), nullptr);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 0);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 10);

    for (auto& buffer : buffers) {
        globalPool.recyclePooledBuffer(buffer);
    }

    EXPECT_EQ(globalPool.getAvailableBufferCount(), 10);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 0);
}

TEST(GlobalNettyBufferPoolTest, BigBuffer)
{
    NettyBufferConf conf(10, 1024, 5, 10);
    GlobalNettyBufferPool globalPool(conf);

    auto bigBuf = globalPool.allocateBigBuffer(8192);
    EXPECT_NE(bigBuf, nullptr);
    EXPECT_GT(bigBuf->GetCapacity(), 1024);

    long address = reinterpret_cast<long>(bigBuf->GetOriginalAddress());
    globalPool.recycleBigBuffer(address);
}

TEST(GlobalNettyBufferPoolTest, Destroy)
{
    NettyBufferConf conf(10, 1024, 5, 10);
    GlobalNettyBufferPool globalPool(conf);

    globalPool.destroy();
    EXPECT_TRUE(globalPool.isDestroyed());
    EXPECT_EQ(globalPool.getTotalBufferCount(), 0);
    EXPECT_EQ(globalPool.getAvailableBufferCount(), 0);
}

// --- LocalNettyBufferPool Tests ---

TEST(LocalNettyBufferPoolTest, CreateLocalPool)
{
    NettyBufferConf conf(100, 1024, 10, 10);
    GlobalNettyBufferPool globalPool(conf);

    auto localPool = globalPool.createLocalPool(4);
    EXPECT_NE(localPool, nullptr);
    EXPECT_EQ(localPool->getNumberOfRequiredBuffers(), 5);
    EXPECT_EQ(localPool->getMaxNumberOfBuffers(), 50);
    EXPECT_FALSE(localPool->isDestroyed());
}

TEST(LocalNettyBufferPoolTest, RequestFromLocal)
{
    NettyBufferConf conf(100, 1024, 10, 10);
    GlobalNettyBufferPool globalPool(conf);

    auto localPool = globalPool.createLocalPool(4);

    // Request buffer — should borrow from global
    auto buffer = localPool->requestBuffer();
    EXPECT_NE(buffer, nullptr);
    EXPECT_EQ(localPool->getNumberOfRequestedBuffers(), 1);
}

TEST(LocalNettyBufferPoolTest, RequestUpToPoolSize)
{
    NettyBufferConf conf(20, 1024, 1, 1);
    GlobalNettyBufferPool globalPool(conf);

    // numRequired == maxUsed, so currentPoolSize stays at 5
    auto localPool = globalPool.createLocalPool(4);

    // Should be able to request up to currentPoolSize
    std::vector<std::shared_ptr<NettyMemorySegment>> buffers;
    for (int i = 0; i < 5; i++) {
        auto buf = localPool->requestBuffer();
        EXPECT_NE(buf, nullptr);
        buffers.push_back(buf);
    }

    // 6th request should fail (at size limit)
    auto extra = localPool->requestBuffer();
    EXPECT_EQ(extra, nullptr);

    // Recycle all
    for (auto& buf : buffers) {
        RecycleSingleReference(*localPool, buf);
    }
}

TEST(LocalNettyBufferPoolTest, RecycleReturnsExcessToGlobal)
{
    NettyBufferConf conf(20, 1024, 4, 2);
    GlobalNettyBufferPool globalPool(conf);

    auto localPool = globalPool.createLocalPool(2);

    // Request some buffers
    std::vector<std::shared_ptr<NettyMemorySegment>> buffers;
    for (int i = 0; i < 5; i++) {
        auto buf = localPool->requestBuffer();
        EXPECT_NE(buf, nullptr);
        buffers.push_back(buf);
    }

    int globalAvailBefore = globalPool.getAvailableBufferCount();

    // Shrink pool size to create excess
    localPool->setNumBuffers(3);

    // Recycle a buffer — should go to global since we have excess
    RecycleSingleReference(*localPool, buffers[0]);

    // Global should have gotten the buffer back
    EXPECT_GT(globalPool.getAvailableBufferCount(), globalAvailBefore);
}

TEST(LocalNettyBufferPoolTest, LazyDestroy)
{
    NettyBufferConf conf(20, 1024, 2, 2);
    GlobalNettyBufferPool globalPool(conf);

    auto localPool = globalPool.createLocalPool(4);

    // Request some buffers
    auto b1 = localPool->requestBuffer();
    auto b2 = localPool->requestBuffer();

    // Recycle them so they are in local available queue
    RecycleSingleReference(*localPool, b1);
    RecycleSingleReference(*localPool, b2);

    // Destroy — should return all available buffers to global
    int expectedGlobalAvailable = globalPool.getAvailableBufferCount() + localPool->getNumberOfAvailableBuffers();
    localPool->lazyDestroy();
    EXPECT_TRUE(localPool->isDestroyed());
    EXPECT_EQ(globalPool.getAvailableBufferCount(), expectedGlobalAvailable);
    EXPECT_EQ(globalPool.getUsedBufferCount(), 0);
}

TEST(LocalNettyBufferPoolTest, BigBuffer)
{
    NettyBufferConf conf(20, 1024, 2, 2);
    GlobalNettyBufferPool globalPool(conf);

    auto localPool = globalPool.createLocalPool(4);

    auto bigBuf = localPool->requestBigBuffer(8192);
    EXPECT_NE(bigBuf, nullptr);
    EXPECT_GT(bigBuf->GetCapacity(), 1024);

    RecycleSingleReference(*localPool, bigBuf);
}

TEST(LocalNettyBufferPoolTest, BlockingRequestWakesOnRecycle)
{
    NettyBufferConf conf(5, 1024, 1, 1);
    GlobalNettyBufferPool globalPool(conf);

    auto localPool = globalPool.createLocalPool(4);

    // Exhaust all buffers
    std::vector<std::shared_ptr<NettyMemorySegment>> buffers;
    for (int i = 0; i < 5; i++) {
        buffers.push_back(localPool->requestBuffer());
    }

    std::atomic<bool> gotBuffer{false};
    std::shared_ptr<NettyMemorySegment> receivedBuffer;

    // Start a thread that does a blocking request
    std::thread requester([&]() {
        receivedBuffer = localPool->requestBufferBlocking();
        if (receivedBuffer) {
            gotBuffer.store(true);
        }
    });

    // Give the thread time to start waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(gotBuffer.load());

    // Recycle a buffer — should wake the requester
    RecycleSingleReference(*localPool, buffers[0]);

    requester.join();
    ASSERT_TRUE(gotBuffer.load());
    ASSERT_NE(receivedBuffer, nullptr);

    // Cleanup
    RecycleSingleReference(*localPool, receivedBuffer);
    for (size_t i = 1; i < buffers.size(); i++) {
        RecycleSingleReference(*localPool, buffers[i]);
    }
}

TEST(LocalNettyBufferPoolTest, MultipleLocalPools)
{
    NettyBufferConf conf(20, 1024, 4, 2);
    GlobalNettyBufferPool globalPool(conf);

    auto pool1 = globalPool.createLocalPool(2);
    auto pool2 = globalPool.createLocalPool(2);

    // Both should be able to request buffers
    auto b1 = pool1->requestBuffer();
    auto b2 = pool2->requestBuffer();
    EXPECT_NE(b1, nullptr);
    EXPECT_NE(b2, nullptr);

    // Cleanup
    RecycleSingleReference(*pool1, b1);
    RecycleSingleReference(*pool2, b2);

    pool1->lazyDestroy();
    pool2->lazyDestroy();
}

TEST(GlobalNettyBufferPoolTest, CreatePoolFailsWhenInsufficient)
{
    NettyBufferConf conf(10, 1024, 5, 10);
    GlobalNettyBufferPool globalPool(conf);

    // First pool takes 5 required
    auto pool1 = globalPool.createLocalPool(4);

    // Second pool wants 6 required — only 5 left
    EXPECT_THROW(globalPool.createLocalPool(5), std::runtime_error);

    pool1->lazyDestroy();
}

TEST(LocalNettyBufferPoolTest, Redistribution)
{
    NettyBufferConf conf(20, 1024, 4, 2);
    GlobalNettyBufferPool globalPool(conf);

    // Create two resizable pools (required < max)
    auto pool1 = globalPool.createLocalPool(2);
    auto pool2 = globalPool.createLocalPool(2);

    // After redistribution, each pool should have more than just the required 3
    // The excess (20 - 6 = 14) is distributed proportionally
    // Each pool can take up to min(14, 10-3)=7 extra, so each gets ~7
    // pool1: 3 + 7 = 10, pool2: 3 + 7 = 10

    // We can verify by requesting more than required
    std::vector<std::shared_ptr<NettyMemorySegment>> buffers;
    for (int i = 0; i < 8; i++) {
        auto buf = pool1->requestBuffer();
        if (buf) {
            buffers.push_back(buf);
        }
    }
    // Should have gotten more than 3
    EXPECT_GT(static_cast<int>(buffers.size()), 3);

    // Cleanup
    for (auto& buf : buffers) {
        RecycleSingleReference(*pool1, buf);
    }
    pool1->lazyDestroy();
    pool2->lazyDestroy();
}
