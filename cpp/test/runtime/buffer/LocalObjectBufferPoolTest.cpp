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

#include "runtime/buffer/BufferBuilder.h"
#include "runtime/buffer/BufferConsumer.h"
#include "runtime/buffer/LocalObjectBufferPool.h"
#include "runtime/buffer/NetworkObjectBufferPool.h"
#include "streaming/api/watermark/Watermark.h"

using namespace omnistream;

TEST(NetworkObjectBufferPoolTest, DirectRequestRecycleUsesObjectMemory)
{
    int segmentNum = 3;
    int segmentSize = 10;
    auto networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);
    EXPECT_EQ(networkObjectBufferPool->getAvailableMemory(), segmentNum * segmentSize);

    auto segment = networkObjectBufferPool->requestPooledObjectSegment(segmentSize);
    ASSERT_NE(segment, nullptr);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum - 1);
    EXPECT_EQ(networkObjectBufferPool->getAvailableMemory(), (segmentNum - 1) * segmentSize);

    networkObjectBufferPool->recyclePooledObjectSegment(segment);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);
    EXPECT_EQ(networkObjectBufferPool->getAvailableMemory(), segmentNum * segmentSize);
}

TEST(LocalObjectBufferPoolTest, RequestBuffer)
{
    int segmentNum = 100;
    int segmentSize = 10;
    auto networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    int requiredBufferNum = 5;
    int maxBufferAllowedToRequest = 5;
    auto localObjectBufferPool =
        std::make_shared<LocalObjectBufferPool>(networkObjectBufferPool, requiredBufferNum, maxBufferAllowedToRequest);

    EXPECT_EQ(localObjectBufferPool->getRequiredMemory(), static_cast<uint64_t>(requiredBufferNum * segmentSize));

    std::shared_ptr<Buffer> buffer1 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer1, nullptr);

    std::shared_ptr<Buffer> buffer2 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer3 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer2, nullptr);
    ASSERT_NE(buffer3, nullptr);

    std::shared_ptr<Buffer> buffer4 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer5 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer4, nullptr);
    ASSERT_NE(buffer5, nullptr);

    std::shared_ptr<Buffer> buffer6 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer6, nullptr);

    buffer1->RecycleBuffer();
    buffer1.reset();
    buffer2->RecycleBuffer();
    buffer2.reset();
    buffer3->RecycleBuffer();
    buffer3.reset();
    buffer4->RecycleBuffer();
    buffer4.reset();
    buffer5->RecycleBuffer();
    buffer5.reset();
}

TEST(LocalObjectBufferPoolTest, DISABLED_Recycle)
{
    int segmentNum = 100;
    int segmentSize = 10;
    auto networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    int requiredBufferNum = 5;
    int maxBufferAllowedToRequest = 5;
    auto localObjectBufferPool =
        std::make_shared<LocalObjectBufferPool>(networkObjectBufferPool, requiredBufferNum, maxBufferAllowedToRequest);

    std::shared_ptr<Buffer> buffer1 = localObjectBufferPool->requestBuffer();
    buffer1->RecycleBuffer();
    buffer1.reset();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 2);

    std::shared_ptr<Buffer> buffer2 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer3 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer4 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer5 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer6 = localObjectBufferPool->requestBuffer();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 0);
    // Local pools use lightweight unpooled segments; the global physical-segment deque is unchanged.
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);

    buffer2->RecycleBuffer();
    buffer2.reset();
    buffer3->RecycleBuffer();
    buffer3.reset();
    buffer4->RecycleBuffer();
    buffer4.reset();
    buffer5->RecycleBuffer();
    buffer5.reset();
    buffer6->RecycleBuffer();
    buffer6.reset();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 5);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);
}

TEST(LocalObjectBufferPoolTest, ClosingUnreadVectorBatchReturnsChargedBytes)
{
    auto networkPool = std::make_shared<NetworkObjectBufferPool>(10, 1024);
    auto bufferPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(networkPool->createBufferPool(1, 2, 1, 2));
    ASSERT_NE(bufferPool, nullptr);

    constexpr uint64_t watermarkBytes = sizeof(int64_t);
    ASSERT_TRUE(bufferPool->chargeMemory(0, watermarkBytes));

    BufferBuilder* builder = bufferPool->requestBufferBuilder(0, watermarkBytes);
    ASSERT_NE(builder, nullptr);
    auto* watermark = new Watermark(123);
    builder->appendAndCommit(watermark);
    auto consumer = builder->createBufferConsumerFromBeginning();
    builder->finish();
    builder->close();
    delete builder;

    EXPECT_EQ(bufferPool->getUsedMemory(), watermarkBytes);
    consumer->close();
    EXPECT_EQ(bufferPool->getUsedMemory(), 0);
    delete watermark;
}
