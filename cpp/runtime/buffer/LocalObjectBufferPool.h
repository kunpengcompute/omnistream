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

#ifndef LOCAL_OBJECT_BUFFER_POOL_H
#define LOCAL_OBJECT_BUFFER_POOL_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>
#include <climits>
#include <io/AvailabilityHelper.h>

#include "ObjectBuffer.h"
#include "ObjectBufferRecycler.h"
#include "ObjectSegment.h"
#include "BufferPool.h"
#include "LocalBufferPool.h"

namespace omnistream {
class NetworkObjectBufferPool;
class LocalBufferPool;
class ObjectBufferBuilder;

struct ObjectBufferMemoryReservation {
    int64_t reservedBytes = 0;
};
} // namespace omnistream

namespace omnistream {

class LocalObjectBufferPool : public LocalBufferPool {
public:
    LocalObjectBufferPool(
        std::shared_ptr<NetworkObjectBufferPool> networkBufferPool, int numberOfRequiredMemorySegments)
        : LocalObjectBufferPool(networkBufferPool, numberOfRequiredMemorySegments, INT_MAX, 0, INT_MAX)
    {
    }

    LocalObjectBufferPool(
        std::shared_ptr<NetworkObjectBufferPool> networkBufferPool,
        int numberOfRequiredMemorySegments,
        int maxNumberOfMemorySegments)
        : LocalObjectBufferPool(
              networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, 0, INT_MAX)
    {
    }

    LocalObjectBufferPool(
        std::shared_ptr<NetworkObjectBufferPool> networkBufferPool,
        int numberOfRequiredMemorySegments,
        int maxNumberOfMemorySegments,
        int numberOfSubpartitions,
        int maxBuffersPerChannel);

    ~LocalObjectBufferPool() override = default;

    void postConstruct();
    void lazyDestroy() override;
    // Overrides the flag-only base cancel(): also completes the availability future so a thread
    // blocked in chargeMemoryBlocking/requestObjectSegmentBlocking wakes up, observes cancelled_
    // and aborts. Without the wake the blocked task thread sleeps forever and can't be cancelled.
    void cancel() override;

    void reserveSegments(int numberOfSegmentsToReserve) override;
    bool isDestroyed() override;
    int getMaxNumberOfSegments() const override;
    int getNumberOfAvailableSegments() override;
    int getNumBuffers() override;
    int bestEffortGetNumOfUsedBuffers() const override;
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    void setNumBuffers(int numBuffers) override;

    std::shared_ptr<Buffer> requestBuffer() override;
    BufferBuilder* requestBufferBuilder() override;
    BufferBuilder* requestBufferBuilder(int targetChannel, uint64_t bytes = 0) override;
    BufferBuilder* requestBufferBuilderBlocking() override;
    BufferBuilder* requestBufferBuilderBlocking(int targetChannel, uint64_t bytes = 0) override;

    std::shared_ptr<ObjectBuffer> requestObjectBuffer();
    ObjectBufferBuilder* requestObjectBufferBuilder();
    ObjectBufferBuilder* requestObjectBufferBuilder(int targetChannel, uint64_t bytes = 0);
    ObjectBufferBuilder* requestObjectBufferBuilderBlocking();
    ObjectBufferBuilder* requestObjectBufferBuilderBlocking(int targetChannel, uint64_t bytes = 0);

    Segment* requestSegment(uint64_t bytes = 0);
    Segment* requestSegment(int targetChannel, uint64_t bytes = 0);
    Segment* requestSegmentBlocking(uint64_t bytes = 0);
    Segment* requestSegmentBlocking(int targetChannel, uint64_t bytes = 0);

    ObjectSegment* requestObjectSegment(uint64_t bytes = 0);
    ObjectSegment* requestObjectSegment(int targetChannel, uint64_t bytes = 0);
    ObjectSegment* requestObjectSegmentBlocking(uint64_t bytes = 0);
    ObjectSegment* requestObjectSegmentBlocking(int targetChannel, uint64_t bytes = 0);

    std::shared_ptr<ObjectBuffer> toObjectBuffer(ObjectSegment* segment);
    ObjectBufferBuilder* toObjectBufferBuilder(ObjectSegment* segment, int targetChannel);

    std::string toString() const override;

    void returnSegment(Segment* segment) override;
    void returnObjectSegment(ObjectSegment* segment);

    void returnExcessSegments() override;
    void returnExcessObjectSegments();

    bool requestSegmentFromGlobal() override;

    uint64_t getRequiredMemory() const;
    uint64_t getMaxMemory() const;
    int getObjectSegmentSize() const;
    uint64_t getCurrentPoolMemoryBudget() const;
    uint64_t getUsedMemory() const;
    uint64_t getAvailableMemory() const;
    uint64_t getMaxMemoryPerChannel() const;
    int getRequestSegmentNumber() const;
    int getRecycleSegmentNumber() const;
    void setMemoryBudget(uint64_t memoryBudget);
    void returnMemory(uint64_t bytes);
    bool requestMemory(uint64_t bytes);
    bool requestMemoryFromGlobal(uint64_t bytes);
    bool shouldBeAvailable();
    void mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify);
    void notifyGlobalMemoryAvailable();
    void SetBufferPoolMetric(AbstractMetricGroup metricGroup) override;
    void chargeMemoryBlocking(int targetChannel, uint64_t bytes);
    bool chargeMemory(int targetChannel, uint64_t bytes);
    int64_t calculateByteNeedReturnToGlobal(int64_t returnBytes);

protected:
    bool checkAvailability();

private:
    void recycle(Segment* segment, int channel);
    uint64_t removeExcessObjectMemory();
    void recycleBytes(int64_t bytes, int channel);
    void lazyDestroyMemory();
    void lazyDestroySegment();

    bool hasExcessBuffers() override;
    bool isRequestedSizeReached() override;
    // void onSegmentRecycledToLocal(const std::shared_ptr<Segment>& segment) override;

public:
    // public so a sliced buffer can downcast its recycler and call recycleBytes(). The pool's own
    // private recycleBytes(bytes, channel) stays private -- this nested class can reach it.
    class SubpartitionBufferRecycler : public ObjectBufferRecycler {
    public:
        SubpartitionBufferRecycler(int channel, std::shared_ptr<LocalObjectBufferPool> bufferPool);
        void recycle(Segment* segment) override;
        void recycleBytes(int64_t bytes);

    protected:
        int channel_;
        std::shared_ptr<LocalObjectBufferPool> bufferPool_;
    };

private:
    std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool_;
    int maxNumberOfObjectSegments_;
    int objectSegmentSize;
    std::vector<std::shared_ptr<ObjectBufferRecycler>> subpartitionBufferRecyclers_;
    uint64_t requiredMemory_;
    uint64_t currentPoolMemoryBudget_;
    uint64_t maxAllowedMemory;
    uint64_t usedMemory;
    uint64_t availableMemory;
    uint64_t maxMemoryPerChannel_;
    std::shared_ptr<ObjectBufferRecycler> defaultBufferRecycler_;
    std::vector<bool> subpartitionBuffersBool_;
    int requestSegmentNumber = 0;
    int recycleSegmentNumber = 0;
    uint64_t requestedBytes = 0;
    uint64_t recycledBytes = 0;

    // True while this pool is registered as a global-memory waiter (see requestObjectSegment).
    // Task-thread only, always accessed under memoryMutex; guards against double inc/dec of
    // NetworkObjectBufferPool::memoryWaiters_ across the request retry loop.
    bool waitingForGlobalMemory_ = false;
    int localNumberOfObjectSegment = 0;
    std::recursive_mutex objectSegmentMutex;
};

} // namespace omnistream

#endif // LOCAL_OBJECT_BUFFER_POOL_H
