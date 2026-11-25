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

#ifndef NETWORKMEMORYBUFFERPOOL_H
#define NETWORKMEMORYBUFFERPOOL_H
#include <memory>
#include <vector>
#include <deque>
#include <set>
#include <condition_variable>

#include <climits>
#include <memory/MemorySegment.h>
#include "runtime/buffer/ObjectSegment.h"

#include "io/AvailabilityHelper.h"
#include "BufferPool.h"
#include "NetworkBufferPool.h"

namespace omnistream::datastream {
class LocalMemoryBufferPool;
}

namespace omnistream::datastream {

using namespace omnistream;

class NetworkMemoryBufferPool : public NetworkBufferPool, public std::enable_shared_from_this<NetworkMemoryBufferPool> {
public:
    NetworkMemoryBufferPool(int numberOfSegmentsToAllocate, int segmentSize)
        : NetworkMemoryBufferPool(numberOfSegmentsToAllocate, segmentSize, std::chrono::milliseconds(INT_MAX))
    {
    }
    NetworkMemoryBufferPool(
        int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout);
    ~NetworkMemoryBufferPool() override
    {
        for (auto segment : availableMemorySegments) {
            delete segment;
        }
        availableMemorySegments.clear();
    }

    MemorySegment* requestPooledMemorySegment();
    std::vector<MemorySegment*> requestPooledMemorySegmentsBlocking(int numberOfSegmentsToRequest);
    void recyclePooledMemorySegment(MemorySegment* segment);

    std::vector<MemorySegment*> requestUnpooledMemorySegments(int numberOfSegmentsToRequest) override;
    void recycleUnpooledMemorySegments(const std::vector<MemorySegment*>& segments) override;
    std::vector<ObjectSegment*> requestUnpooledObjectSegments(int numberOfSegmentsToRequest) override
    {
        THROW_LOGIC_EXCEPTION("error");
    }

    void recycleUnpooledObjectSegments(const std::vector<ObjectSegment*>& segments) override
    {
        THROW_LOGIC_EXCEPTION("error");
    }
    void destroy();
    bool isDestroyed() const;
    int getTotalNumberOfMemorySegments() const;
    long getTotalMemory() const;
    int getNumberOfAvailableMemorySegments();
    long getAvailableMemory();
    int getNumberOfUsedMemorySegments();
    long getUsedMemory();
    int getNumberOfRegisteredBufferPools();
    int countBuffers();
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;
    std::shared_ptr<BufferPool> createBufferPool(int numRequiredBuffers, int maxUsedBuffers) override;
    std::shared_ptr<BufferPool> createBufferPool(
        int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel) override;
    void destroyBufferPool(std::shared_ptr<BufferPool> objectBufferPool) override;
    void destroyAllBufferPools();

    std::string toString() const override;

private:
    std::vector<MemorySegment*> internalRequestMemorySegments(int numberOfSegmentsToRequest);
    MemorySegment* internalRequestMemorySegment();
    void revertRequiredBuffers(int size);
    void internalRecycleMemorySegments(const std::vector<MemorySegment*>& segments);
    std::shared_ptr<LocalMemoryBufferPool> internalCreateMemoryBufferPool(
        int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel);
    void tryRedistributeBuffers(int numberOfSegmentsToRequest);
    void redistributeBuffers();
    std::string getConfigDescription();

    std::shared_ptr<AvailabilityHelper> availabilityHelper;
    int totalNumberOfMemorySegments;
    std::deque<MemorySegment*> availableMemorySegments;
    std::recursive_mutex availableMemorySegmentMutex;
    bool isDestroyed_ = false;
    std::recursive_mutex factoryLock;
    std::set<std::shared_ptr<LocalMemoryBufferPool>> allMemoryBufferPools;
    std::set<std::shared_ptr<LocalMemoryBufferPool>> resizableBufferPools;
    int numTotalRequiredBuffers{};
    std::chrono::milliseconds requestSegmentsTimeout{};
    std::condition_variable_any cv;
    int segmentSize;
};
} // namespace omnistream::datastream

#endif // NETWORKMEMORYBUFFERPOOL_H
