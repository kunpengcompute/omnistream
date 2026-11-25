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

#ifndef NETWORKOBJECTBUFFERPOOL_H
#define NETWORKOBJECTBUFFERPOOL_H
#include <memory>
#include <vector>
#include <deque>
#include <set>
#include <condition_variable>
#include <climits>
#include <mutex>
#include <atomic>

#include "ObjectSegment.h"

#include "io/AvailabilityHelper.h"
#include "BufferPool.h"
#include "BufferPoolFactory.h"
#include "LocalObjectBufferPool.h"
#include "runtime/metrics/groups/GlobalVectorBatchBufferMetricGroup.h"

namespace omnistream {
class NetworkObjectBufferPool : public NetworkBufferPool, public std::enable_shared_from_this<NetworkObjectBufferPool> {
public:
    NetworkObjectBufferPool(int numberOfSegmentsToAllocate, int segmentSize)
        : NetworkObjectBufferPool(numberOfSegmentsToAllocate, segmentSize, std::chrono::milliseconds(INT_MAX))
    {
    }

    NetworkObjectBufferPool(
        int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout);

    ~NetworkObjectBufferPool() override;

    ObjectSegment* requestPooledObjectSegment(uint64_t bytes = 0);
    ObjectSegment* requestPooledObjectSegmentsBlocking(uint64_t bytes);
    void recyclePooledObjectSegment(ObjectSegment* segment);
    void recyclePooledObjectSegmentPhysicalOnly(ObjectSegment* segment);
    void recyclePooledObjectSegmentsPhysicalOnly(std::vector<ObjectSegment*>& segments);
    std::vector<MemorySegment*> requestUnpooledMemorySegments(int numberOfSegmentsToRequest) override
    {
        THROW_LOGIC_EXCEPTION("error");
    }
    void recycleUnpooledMemorySegments(const std::vector<MemorySegment*>& segments) override
    {
        THROW_LOGIC_EXCEPTION("error");
    }
    std::vector<ObjectSegment*> requestUnpooledObjectSegments(int numberOfSegmentsToRequest) override
    {
        THROW_LOGIC_EXCEPTION("error");
    }
    void recycleUnpooledObjectSegments(const std::vector<ObjectSegment*>& segments)
    {
        THROW_LOGIC_EXCEPTION("error");
    }
    void destroy();
    bool isDestroyed() const;
    int getTotalNumberOfObjectSegments() const;
    long getTotalMemory() const;
    int getNumberOfAvailableObjectSegments();
    long getAvailableMemory();
    int getNumberOfUsedObjectSegments();
    long getUsedMemory();
    int getNumberOfRegisteredBufferPools();
    int countBuffers();
    bool requestMemory(uint64_t bytes);
    void returnMemory(uint64_t bytes);
    bool requestMemoryBlocking(uint64_t bytes);

    // Track how many local pools are currently blocked in requestObjectSegmentBlocking()
    // waiting for memory. When this is 0, returnMemory() can skip the O(all-pools)
    // notification fan-out entirely (there is nobody to wake).
    void incMemoryWaiters()
    {
        memoryWaiters_.fetch_add(1, std::memory_order_acq_rel);
    }
    void decMemoryWaiters()
    {
        memoryWaiters_.fetch_sub(1, std::memory_order_acq_rel);
    }

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    std::shared_ptr<BufferPool> createBufferPool(int numRequiredBuffers, int maxUsedBuffers) override;
    std::shared_ptr<BufferPool> createBufferPool(
        int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel) override;
    void destroyBufferPool(std::shared_ptr<BufferPool> objectBufferPool) override;
    void destroyAllBufferPools();

    std::string toString() const override;
    int getObjectSegmentSize();
    GlobalVectorBatchBufferMetricGroup::SizeSupplierFactory CreateGlobalVectorBatchBufferMetricSupplierFactory();

    ObjectSegment* requestPureObjectSegment();

private:
    ObjectSegment* internalRequestObjectSegment();
    void revertRequiredBuffers(uint64_t memoryToRevert);
    void internalRecycleObjectSegments(const std::vector<ObjectSegment*>& segments);
    std::shared_ptr<BufferPool> internalCreateObjectBufferPool(
        int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel);
    void tryRedistributeBuffers(uint64_t memoryToRequest);
    void redistributeBuffers();
    std::string getConfigDescription();

    int totalNumberOfObjectSegments;
    int objectSegmentSize;
    std::deque<ObjectSegment*> availableObjectSegments;
    std::recursive_mutex availableObjSegMutex;
    bool isDestroyed_ = false;
    std::recursive_mutex factoryLock;
    std::recursive_mutex memoryMutex_;
    std::set<std::shared_ptr<LocalObjectBufferPool>> allBufferPools;
    uint64_t numTotalRequiredMemory{};
    std::chrono::milliseconds requestSegmentsTimeout{};
    std::shared_ptr<AvailabilityHelper> availabilityHelper = std::make_shared<AvailabilityHelper>();
    std::condition_variable_any cv;
    // Count of local pools currently blocked waiting for segment memory (see inc/decMemoryWaiters).
    std::atomic<int> memoryWaiters_{0};
    uint64_t totalMemory;
    uint64_t availableMemory;
    uint64_t usedMemory = 0;
};
} // namespace omnistream

#endif
