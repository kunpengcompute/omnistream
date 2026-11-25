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

#include "NetworkMemoryBufferPool.h"
#include <memory/MemorySegmentFactory.h>

#include "LocalMemoryBufferPool.h"

namespace omnistream::datastream {

NetworkMemoryBufferPool::NetworkMemoryBufferPool(
    int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout)
    : availabilityHelper(std::make_shared<AvailabilityHelper>()),
      requestSegmentsTimeout(requestSegmentsTimeout),
      segmentSize(segmentSize)
{
    if (requestSegmentsTimeout.count() <= 0) {
        throw std::invalid_argument("The timeout for requesting exclusive buffers should be positive.");
    }
    INFO_RELEASE(
        "numberOfSegmentsToAllocate: " << numberOfSegmentsToAllocate << "  segmentSize  is  :" << segmentSize
                                       << " requestSegmentsTimeout: " << requestSegmentsTimeout.count());
    totalNumberOfMemorySegments = numberOfSegmentsToAllocate;

    try {
        availableMemorySegments = std::deque<MemorySegment*>();
    } catch (const std::bad_alloc&) {
        throw std::bad_alloc();
    }

    try {
        //        if (segmentSize <= 0) {
        //            this->segmentSize = 1024 << 10;
        //        }

        for (int i = 0; i < numberOfSegmentsToAllocate; ++i) {
            availableMemorySegments.push_back(MemorySegmentFactory::wrap(segmentSize));
        }
    } catch (const std::bad_alloc&) {
        availableMemorySegments.clear();

        LOG("Could not allocate enough memory segments for NetworkBufferPool (required (MB):"
            << ((static_cast<long>(segmentSize) * numberOfSegmentsToAllocate) >> 20) << ", allocated (MB):"
            << ((static_cast<long>(segmentSize) * availableMemorySegments.size()) >> 20) << ", missing (MB):"
            << (((static_cast<long>(segmentSize) * numberOfSegmentsToAllocate) >> 20) -
                ((static_cast<long>(segmentSize) * availableMemorySegments.size()) >> 20))
            << ").\n");
        throw std::bad_alloc();
    }

    availabilityHelper->resetAvailable();
}

MemorySegment* NetworkMemoryBufferPool::requestPooledMemorySegment()
{
    std::lock_guard<std::recursive_mutex> lock(availableMemorySegmentMutex);
    return internalRequestMemorySegment();
}

std::vector<MemorySegment*> NetworkMemoryBufferPool::requestPooledMemorySegmentsBlocking(int numberOfSegmentsToRequest)
{
    return internalRequestMemorySegments(numberOfSegmentsToRequest);
}

void NetworkMemoryBufferPool::recyclePooledMemorySegment(MemorySegment* segment)
{
    internalRecycleMemorySegments({segment});
}

std::vector<MemorySegment*> NetworkMemoryBufferPool::requestUnpooledMemorySegments(int numberOfSegmentsToRequest)
{
    if (numberOfSegmentsToRequest < 0) {
        throw std::invalid_argument("Number of buffers to request must be non - negative.");
    }
    {
        std::lock_guard<std::recursive_mutex> lock(factoryLock);
        if (isDestroyed_) {
            throw std::runtime_error("Network buffer pool has already been destroyed.");
        }
        if (numberOfSegmentsToRequest == 0) {
            return {};
        }
        tryRedistributeBuffers(numberOfSegmentsToRequest);
    }

    try {
        return internalRequestMemorySegments(numberOfSegmentsToRequest);
    } catch (const std::exception& exception) {
        revertRequiredBuffers(numberOfSegmentsToRequest);
        throw exception;
    }
}

std::vector<MemorySegment*> NetworkMemoryBufferPool::internalRequestMemorySegments(int numberOfSegmentsToRequest)
{
    std::vector<MemorySegment*> segments;
    auto deadline = std::chrono::steady_clock::now() + requestSegmentsTimeout;
    try {
        while (true) {
            if (isDestroyed_) {
                throw std::runtime_error("Buffer pool is destroyed.");
            }

            MemorySegment* segment;
            {
                // std::lock_guard<std::mutex> lock(availableObjectSegmentsMutex);
                std::unique_lock<std::recursive_mutex> lock(availableMemorySegmentMutex);
                if (!(segment = internalRequestMemorySegment())) {
                    INFO_RELEASE("NetworkMemoryBufferPool sleep time: " << std::to_string(2000));
                    // todo: java实现可以被提前唤醒
                    // std::this_thread::sleep_for(std::chrono::milliseconds(2000));
                    cv.wait_for(lock, std::chrono::milliseconds(2000));
                }
            }
            if (segment) {
                segments.push_back(segment);
            }

            if (segments.size() >= static_cast<size_t>(numberOfSegmentsToRequest)) {
                break;
            }

            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(
                    "Timeout triggered when requesting exclusive buffers: " + getConfigDescription() +
                    ", or you may increase the timeout which is " + std::to_string(requestSegmentsTimeout.count()) +
                    "ms by setting the key 'NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS'.");
            }
        }
    } catch (const std::exception& e) {
        internalRecycleMemorySegments(segments);
        throw;
    }
    return segments;
}

MemorySegment* NetworkMemoryBufferPool::internalRequestMemorySegment()
{
    LOG("availableMemorySegments size : " << std::to_string(availableMemorySegments.size()));
    LOG("availableMemorySegments.empty() : " << std::to_string(availableMemorySegments.empty()));
    if (availableMemorySegments.empty()) {
        return nullptr;
    }

    auto segment = availableMemorySegments.front();
    availableMemorySegments.pop_front();
    if (availableMemorySegments.empty() && segment) {
        availabilityHelper->resetUnavailable();
    }
    return segment;
}

void NetworkMemoryBufferPool::recycleUnpooledMemorySegments(const std::vector<MemorySegment*>& segments)
{
    internalRecycleMemorySegments(segments);
    revertRequiredBuffers(segments.size());
}

void NetworkMemoryBufferPool::revertRequiredBuffers(int size)
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    numTotalRequiredBuffers -= size;
    redistributeBuffers();
}

void NetworkMemoryBufferPool::internalRecycleMemorySegments(const std::vector<MemorySegment*>& segments)
{
    LOG("internalRecycleObjectSegments running");
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(availableMemorySegmentMutex);

        if (availableMemorySegments.empty() && !segments.empty()) {
            toNotify = availabilityHelper->getUnavailableToResetAvailable();
        }
        for (const auto& segment : segments) {
            availableMemorySegments.push_back(segment);
        }
        cv.notify_all();
        if (toNotify != nullptr) {
            toNotify->complete();
        }
    }
}

void NetworkMemoryBufferPool::destroy()
{
    {
        // std::lock_guard<std::mutex> lock(factoryLock);
        std::lock_guard<std::recursive_mutex> lock(factoryLock);
        isDestroyed_ = true;
    }

    {
        std::lock_guard<std::recursive_mutex> lock(availableMemorySegmentMutex);
        LOG("destroy running");
        while (!availableMemorySegments.empty()) {
            auto segment = availableMemorySegments.front();
            availableMemorySegments.pop_front();
            // segment->free();
        }
    }
}

bool NetworkMemoryBufferPool::isDestroyed() const
{
    return isDestroyed_;
}

int NetworkMemoryBufferPool::getTotalNumberOfMemorySegments() const
{
    return isDestroyed() ? 0 : totalNumberOfMemorySegments;
}

long NetworkMemoryBufferPool::getTotalMemory() const
{
    return static_cast<long>(getTotalNumberOfMemorySegments()) * segmentSize;
}

int NetworkMemoryBufferPool::getNumberOfAvailableMemorySegments()
{
    std::lock_guard<std::recursive_mutex> lock(availableMemorySegmentMutex);
    return availableMemorySegments.size();
}

long NetworkMemoryBufferPool::getAvailableMemory()
{
    return static_cast<long>(getNumberOfAvailableMemorySegments()) * segmentSize;
}

int NetworkMemoryBufferPool::getNumberOfUsedMemorySegments()
{
    return getTotalNumberOfMemorySegments() - getNumberOfAvailableMemorySegments();
}

// todo: if this mehtod used, we need add implementation for memory segment
long NetworkMemoryBufferPool::getUsedMemory()
{
    return static_cast<long>(getNumberOfUsedMemorySegments()) * segmentSize;
}

int NetworkMemoryBufferPool::getNumberOfRegisteredBufferPools()
{
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    return allMemoryBufferPools.size();
}

int NetworkMemoryBufferPool::countBuffers()
{
    int buffers = 0;
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    for (const auto& bp : allMemoryBufferPools) {
        buffers += bp->getNumBuffers();
    }
    return buffers;
}

std::shared_ptr<CompletableFuture> NetworkMemoryBufferPool::GetAvailableFuture()
{
    return availabilityHelper->GetAvailableFuture();
}

std::shared_ptr<BufferPool> NetworkMemoryBufferPool::createBufferPool(int numRequiredBuffers, int maxUsedBuffers)
{
    LOG("createBufferPool func1");
    return internalCreateMemoryBufferPool(numRequiredBuffers, maxUsedBuffers, 0, INT_MAX);
}

std::shared_ptr<BufferPool> NetworkMemoryBufferPool::createBufferPool(
    int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel)
{
    LOG_INFO_IMP(
        "createBufferPool numRequiredBuffers : " << numRequiredBuffers << " maxUsedBuffers: " << maxUsedBuffers
                                                 << " numSubpartitions: " << numSubpartitions
                                                 << " maxBuffersPerChannel: " << maxBuffersPerChannel);
    auto res =
        internalCreateMemoryBufferPool(numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    LOG("createBufferPool end");
    return res;
}

std::shared_ptr<LocalMemoryBufferPool> NetworkMemoryBufferPool::internalCreateMemoryBufferPool(
    int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel)
{
    LOG("try to get lock ....");
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (isDestroyed_) {
        throw std::runtime_error("Network buffer pool has already been destroyed.");
    }
    LOG_PART(
        "numTotalRequiredBuffers=" << std::to_string(numTotalRequiredBuffers)
                                   << " totalNumberOfObjectSegments=" << std::to_string(totalNumberOfMemorySegments));

    if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
        throw std::runtime_error(
            "Insufficient number of network buffers: " + std::to_string(numRequiredBuffers) + ", but only " +
            std::to_string(totalNumberOfMemorySegments - numTotalRequiredBuffers) + " available. " +
            getConfigDescription());
    }
    numTotalRequiredBuffers += numRequiredBuffers;
    LOG_PART("Before make shared new LocalObjectBufferPool");

    auto localMemoryBufferPool = std::make_shared<LocalMemoryBufferPool>(
        shared_from_this(), numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    LOG_PART("After make shared new LocalObjectBufferPool");
    localMemoryBufferPool->postConstruct();

    LOG_PART("After make shared postConstruct");
    allMemoryBufferPools.insert(localMemoryBufferPool);
    if (numRequiredBuffers < maxUsedBuffers) {
        resizableBufferPools.insert(localMemoryBufferPool);
    }
    redistributeBuffers();
    LOG_PART("redistributeBuffers end");
    return localMemoryBufferPool;
}

void NetworkMemoryBufferPool::destroyBufferPool(std::shared_ptr<BufferPool> bufferPool)
{
    auto localMemoryBufferPool = std::reinterpret_pointer_cast<LocalMemoryBufferPool>(bufferPool);
    if (!localMemoryBufferPool) {
        throw std::invalid_argument("bufferPool is no LocalBufferPool");
    }
    // std::lock_guard<std::mutex> lock(factoryLock);
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (allMemoryBufferPools.erase(localMemoryBufferPool) > 0) {
        numTotalRequiredBuffers -= localMemoryBufferPool->getNumberOfRequiredSegments();
        resizableBufferPools.erase(localMemoryBufferPool);

        redistributeBuffers();
    }
}

void NetworkMemoryBufferPool::destroyAllBufferPools()
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    std::vector<std::shared_ptr<LocalBufferPool>> poolsCopy(allMemoryBufferPools.begin(), allMemoryBufferPools.end());
    for (const auto& pool : poolsCopy) {
        pool->lazyDestroy();
    }
    if (!allMemoryBufferPools.empty() || numTotalRequiredBuffers > 0 || resizableBufferPools.size() > 0) {
        throw std::runtime_error("NetworkBufferPool is not empty after destroying all LocalBufferPools");
    }
}

void NetworkMemoryBufferPool::tryRedistributeBuffers(int numberOfSegmentsToRequest)
{
    LOG("numTotalRequiredBuffers=" << std::to_string(numTotalRequiredBuffers)
                                   << " totalNumberOfObjectSegments=" << std::to_string(totalNumberOfMemorySegments));
    if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
        throw std::runtime_error(
            "Insufficient number of network buffers: " + std::to_string(numberOfSegmentsToRequest) + ", but only " +
            std::to_string(totalNumberOfMemorySegments - numTotalRequiredBuffers) + " available. " +
            getConfigDescription());
    }
    numTotalRequiredBuffers += numberOfSegmentsToRequest;

    try {
        redistributeBuffers();
    } catch (const std::exception& t) {
        numTotalRequiredBuffers -= numberOfSegmentsToRequest;
        redistributeBuffers();
        throw t;
    }
}

void NetworkMemoryBufferPool::redistributeBuffers()
{
    if (resizableBufferPools.empty()) {
        return;
    }
    int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

    if (numAvailableMemorySegment == 0) {
        for (const auto& bufferPool : resizableBufferPools) {
            bufferPool->setNumBuffers(bufferPool->getNumberOfRequiredSegments());
        }
        return;
    }

    long totalCapacity = 0;
    for (const auto& bufferPool : resizableBufferPools) {
        int excessMax = bufferPool->getMaxNumberOfSegments() - bufferPool->getNumberOfRequiredSegments();
        totalCapacity += std::min(numAvailableMemorySegment, excessMax);
    }

    if (totalCapacity == 0) {
        return;
    }

    int memorySegmentsToDistribute = std::min(numAvailableMemorySegment, static_cast<int>(totalCapacity));
    long totalPartsUsed = 0;
    int numDistributedMemorySegment = 0;
    for (const auto& bufferPool : resizableBufferPools) {
        int excessMax = bufferPool->getMaxNumberOfSegments() - bufferPool->getNumberOfRequiredSegments();
        if (excessMax == 0) {
            continue;
        }

        totalPartsUsed += std::min(numAvailableMemorySegment, excessMax);
        int mySize = memorySegmentsToDistribute * totalPartsUsed / totalCapacity - numDistributedMemorySegment;
        numDistributedMemorySegment += mySize;
        bufferPool->setNumBuffers(bufferPool->getNumberOfRequiredSegments() + mySize);
    }
}

std::string NetworkMemoryBufferPool::getConfigDescription()
{
    return "The total number of network buffers is currently set to " + std::to_string(totalNumberOfMemorySegments) +
           " of " + std::to_string(segmentSize) + " bytes each. " +
           "You can increase this number by setting the configuration keys 'NETWORK_MEMORY_FRACTION', "
           "'NETWORK_MEMORY_MIN', and 'NETWORK_MEMORY_MAX'";
}

std::string NetworkMemoryBufferPool::toString() const
{
    return "NetworkMemoryBufferPool";
}
} // namespace omnistream::datastream
