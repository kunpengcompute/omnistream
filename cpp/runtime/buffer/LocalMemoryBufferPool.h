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

#ifndef LOCALMEMORYBUFFERPOOL_H
#define LOCALMEMORYBUFFERPOOL_H

#include "NetworkBuffer.h"
#include "MemoryBufferBuilder.h"
#include "LocalBufferPool.h"
#include "NetworkMemoryBufferPool.h"

namespace omnistream::datastream {
class LocalMemoryBufferPool : public LocalBufferPool {
public:
    LocalMemoryBufferPool(
        std::shared_ptr<NetworkMemoryBufferPool> networkBufferPool, int numberOfRequiredMemorySegments)
        : LocalMemoryBufferPool(networkBufferPool, numberOfRequiredMemorySegments, INT_MAX, 0, INT_MAX)
    {
    }

    LocalMemoryBufferPool(
        std::shared_ptr<NetworkMemoryBufferPool> networkBufferPool,
        int numberOfRequiredMemorySegments,
        int maxNumberOfMemorySegments)
        : LocalMemoryBufferPool(
              networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, 0, INT_MAX)
    {
    }

    LocalMemoryBufferPool(
        std::shared_ptr<NetworkMemoryBufferPool> networkBufferPool,
        int numberOfRequiredMemorySegments,
        int maxNumberOfMemorySegments,
        int numberOfSubpartitions,
        int maxBuffersPerChannel);

    ~LocalMemoryBufferPool() override = default;

    void postConstruct();

    void lazyDestroy() override;

    void reserveSegments(int numberOfSegmentsToReserve) override;

    bool isDestroyed() override;

    int getMaxNumberOfSegments() const override;

    int getNumberOfAvailableSegments() override;

    int getNumBuffers() override;

    int bestEffortGetNumOfUsedBuffers() const override;

    std::shared_ptr<Buffer> requestBuffer() override;

    BufferBuilder* requestBufferBuilder() override;

    BufferBuilder* requestBufferBuilder(int targetChannel, uint64_t bytes = 0) override;

    BufferBuilder* requestBufferBuilderBlocking() override;

    BufferBuilder* requestBufferBuilderBlocking(int targetChannel, uint64_t bytes = 0) override;

    std::shared_ptr<NetworkBuffer> requestNetworkBuffer();

    MemoryBufferBuilder* requestMemoryBufferBuilder();

    MemoryBufferBuilder* requestMemoryBufferBuilder(int targetChannel);

    MemoryBufferBuilder* requestMemoryBufferBuilderBlocking();

    MemoryBufferBuilder* requestMemoryBufferBuilderBlocking(int targetChannel);

    Segment* requestSegment() override;

    Segment* requestSegment(int targetChannel) override;

    Segment* requestSegmentBlocking() override;

    Segment* requestSegmentBlocking(int targetChannel) override;

    MemorySegment* requestPooledMemorySegment();

    MemorySegment* requestOverdraftMemorySegmentFromGlobal();

    MemorySegment* requestMemorySegment();

    MemorySegment* requestMemorySegment(int targetChannel);

    MemorySegment* requestMemorySegmentBlocking();

    MemorySegment* requestMemorySegmentBlocking(int targetChannel);

    std::shared_ptr<NetworkBuffer> toNetworkBuffer(MemorySegment* memorySegment);

    MemoryBufferBuilder* toMemoryBufferBuilder(MemorySegment* memorySegment, int targetChannel);

    std::string toString() const override;

private:
    bool requestSegmentFromGlobal() override;

    void returnSegment(Segment* segment) override;

    void returnMemorySegment(MemorySegment* segment);

    void returnExcessSegments() override;

    void returnExcessMemorySegments();

    bool hasExcessBuffers() override;

    bool isRequestedSizeReached() override;

    std::shared_ptr<NetworkMemoryBufferPool> networkMemoryBufferPool;

    std::deque<std::shared_ptr<BufferListener>> registeredListeners_;

    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    int numberOfRequestedMemorySegments;
    std::vector<std::shared_ptr<BufferRecycler>> subpartitionBufferRecyclers_;
    bool isDestroyed_ = false;
};

} // namespace omnistream::datastream

#endif // LOCALMEMORYBUFFERPOOL_H
