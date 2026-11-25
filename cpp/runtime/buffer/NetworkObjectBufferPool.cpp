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

#include "NetworkObjectBufferPool.h"

#include <limits>
#include <stdexcept>
#include <thread>

#include "LocalObjectBufferPool.h"
#include "objectsegment/ObjectSegmentFactory.h"

namespace {

int ToSizeGaugeValue(long value)
{
    return value > static_cast<long>(std::numeric_limits<int>::max()) ? std::numeric_limits<int>::max()
                                                                      : static_cast<int>(value);
}

} // namespace

namespace omnistream {

NetworkObjectBufferPool::NetworkObjectBufferPool(
    int numberOfSegmentsToAllocate, int segmentSize, std::chrono::milliseconds requestSegmentsTimeout)
    : requestSegmentsTimeout(requestSegmentsTimeout),
      availabilityHelper(std::make_shared<AvailabilityHelper>())
{
    if (requestSegmentsTimeout.count() <= 0) {
        throw std::invalid_argument("The timeout for requesting exclusive buffers should be positive.");
    }
    LOG_INFO_IMP(
        "numberOfSegmentsToAllocate: " << numberOfSegmentsToAllocate << "  segmentSize  is  :" << segmentSize
                                       << " requestSegmentsTimeout: " << requestSegmentsTimeout.count());
    objectSegmentSize = segmentSize;

    try {
        LOG("availableObjectSegments alloc numberOfSegmentsToAllocate :" << numberOfSegmentsToAllocate);

        totalNumberOfObjectSegments = numberOfSegmentsToAllocate;

        availableObjectSegments = std::deque<ObjectSegment*>();
        totalMemory = objectSegmentSize * totalNumberOfObjectSegments;
        availableMemory = totalMemory;
        for (int i = 0; i < numberOfSegmentsToAllocate; ++i) {
            availableObjectSegments.push_back(ObjectSegmentFactory::allocateUnpooledSegment(1));
        }
    } catch (const std::bad_alloc&) {
        throw std::bad_alloc();
    }

    availabilityHelper->resetAvailable();

    LOG("Allocated " << (((long)segmentSize * availableObjectSegments.size()) >> 20)
                     << " MB for network buffer pool (number of memory segments:" << availableObjectSegments.size()
                     << ", bytes per segment: " << segmentSize << ").\n");
}

NetworkObjectBufferPool::~NetworkObjectBufferPool()
{
    availableObjectSegments.clear();
}

ObjectSegment* NetworkObjectBufferPool::requestPooledObjectSegment(uint64_t bytes)
{
    if (isDestroyed()) {
        throw std::runtime_error("Buffer pool is destroyed.");
    }

    if (!requestMemory(bytes)) {
        return nullptr;
    }

    auto segment = internalRequestObjectSegment();
    segment->setCapacity(bytes);
    return segment;
}

ObjectSegment* NetworkObjectBufferPool::requestPooledObjectSegmentsBlocking(uint64_t bytes)
{
    auto deadline = std::chrono::steady_clock::now() + requestSegmentsTimeout;
    auto segment = requestPooledObjectSegment(bytes);
    while (!segment) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        segment = requestPooledObjectSegment(bytes);

        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error(
                "Timeout triggered when requesting exclusive buffers: " + getConfigDescription() +
                ", or you may increase the timeout which is " + std::to_string(requestSegmentsTimeout.count()) +
                "ms by setting the key 'NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS'.");
        }
    }
    return segment;
}

void NetworkObjectBufferPool::recyclePooledObjectSegment(ObjectSegment* segment)
{
    if (!segment) {
        throw std::invalid_argument("Segment cannot be null.");
    }
    returnMemory(segment->getCapacity());
    segment->reset();
    recyclePooledObjectSegmentPhysicalOnly(segment);
}

void NetworkObjectBufferPool::recyclePooledObjectSegmentPhysicalOnly(ObjectSegment* segment)
{
    if (!segment) {
        throw std::invalid_argument("Segment cannot be null.");
    }
    internalRecycleObjectSegments({segment});
}

void NetworkObjectBufferPool::recyclePooledObjectSegmentsPhysicalOnly(std::vector<ObjectSegment*>& segments)
{
    internalRecycleObjectSegments(segments);
}

ObjectSegment* NetworkObjectBufferPool::requestPureObjectSegment()
{
    return internalRequestObjectSegment();
}

ObjectSegment* NetworkObjectBufferPool::internalRequestObjectSegment()
{
    std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
    LOG("availableObjectSegments size : " << std::to_string(availableObjectSegments.size()));
    LOG("availableObjectSegments.empty() : " << std::to_string(availableObjectSegments.empty()));
    if (availableObjectSegments.empty()) {
        // create one
        return ObjectSegmentFactory::allocateUnpooledSegment(1);
    }
    auto segment = availableObjectSegments.front();
    availableObjectSegments.pop_front();
    return segment;
}

void NetworkObjectBufferPool::revertRequiredBuffers(uint64_t memoryToRevert)
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    numTotalRequiredMemory -= memoryToRevert;
    redistributeBuffers();
}

void NetworkObjectBufferPool::internalRecycleObjectSegments(const std::vector<ObjectSegment*>& segments)
{
    LOG("internalRecycleObjectSegments running");
    std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
    for (const auto& segment : segments) {
        availableObjectSegments.push_back(segment);
    }
}

void NetworkObjectBufferPool::destroy()
{
    {
        std::lock_guard<std::recursive_mutex> lock(factoryLock);
        isDestroyed_ = true;
    }

    {
        std::lock_guard<std::recursive_mutex> segLock(availableObjSegMutex);
        LOG("destroy running");
        availableObjectSegments.clear();
    }

    {
        std::lock_guard<std::recursive_mutex> memLock(memoryMutex_);
        availableMemory = 0;
        usedMemory = 0;
    }
}

bool NetworkObjectBufferPool::isDestroyed() const
{
    return isDestroyed_;
}

int NetworkObjectBufferPool::getTotalNumberOfObjectSegments() const
{
    return isDestroyed() ? 0 : totalNumberOfObjectSegments;
}

long NetworkObjectBufferPool::getTotalMemory() const
{
    return totalMemory;
}

int NetworkObjectBufferPool::getNumberOfAvailableObjectSegments()
{
    std::lock_guard<std::recursive_mutex> lock(availableObjSegMutex);
    return static_cast<int>(availableObjectSegments.size());
}

long NetworkObjectBufferPool::getAvailableMemory()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex_);
    return static_cast<long>(availableMemory);
}

int NetworkObjectBufferPool::getNumberOfUsedObjectSegments()
{
    return getTotalNumberOfObjectSegments() - getNumberOfAvailableObjectSegments();
}

long NetworkObjectBufferPool::getUsedMemory()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex_);
    return static_cast<long>(totalMemory - availableMemory);
}

int NetworkObjectBufferPool::getNumberOfRegisteredBufferPools()
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    return static_cast<int>(allBufferPools.size());
}

int NetworkObjectBufferPool::countBuffers()
{
    int buffers = 0;
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    for (const auto& bp : allBufferPools) {
        buffers += bp->getNumBuffers();
    }
    return buffers;
}

bool NetworkObjectBufferPool::requestMemory(uint64_t bytes)
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex_);
    if (isDestroyed_) {
        return false;
    }
    if (availableMemory == 0) {
        availabilityHelper->resetUnavailable();
        return false;
    }
    if (availableMemory < bytes) {
        return false;
    }
    availableMemory -= bytes;
    usedMemory += bytes;
    return true;
}

bool NetworkObjectBufferPool::requestMemoryBlocking(uint64_t bytes)
{
    auto deadline = std::chrono::steady_clock::now() + requestSegmentsTimeout;

    std::unique_lock<std::recursive_mutex> lock(memoryMutex_);
    while (true) {
        if (isDestroyed_) {
            return false;
        }
        if (availableMemory >= bytes) {
            availableMemory -= bytes;
            usedMemory += bytes;
            return true;
        }
        if (availableMemory == 0) {
            // what is the purpose of this
            availabilityHelper->resetUnavailable();
        }
        if (cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            throw std::runtime_error(
                "Timeout requesting memory (" + std::to_string(bytes) + " bytes): " + getConfigDescription() +
                ", or you may increase the timeout which is " + std::to_string(requestSegmentsTimeout.count()) + "ms.");
        }
    }
}

void NetworkObjectBufferPool::returnMemory(uint64_t bytes)
{
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    std::vector<std::shared_ptr<LocalObjectBufferPool>> poolsToNotify;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex_);
        availableMemory += bytes;
        usedMemory -= bytes;
        toNotify = availabilityHelper->getUnavailableToResetAvailable();
    }
    // Notify outside lock to avoid potential re-entrant locking in callbacks
    if (toNotify != nullptr) {
        toNotify->complete();
    }
    cv.notify_all();

    // Fast path: if no local pool is blocked in requestObjectSegmentBlocking() waiting for
    // memory, there is nobody to wake, so skip the O(all-pools) notification fan-out (which
    // takes factoryLock + each pool's memoryMutex). The freed memory was already published
    // above under memoryMutex_, so a waiter that registers concurrently will see it on its
    // recheck (see LocalObjectBufferPool::requestObjectSegmentBlocking). This removes the
    // per-recycle "thundering herd" that dominated the drain phase.
    if (memoryWaiters_.load(std::memory_order_acquire) == 0) {
        return;
    }

    // Notify local pools that global memory is available,
    // so pools waiting on requestMemoryFromGlobal can retry.
    {
        std::lock_guard<std::recursive_mutex> lock(factoryLock);
        for (const auto& pool : allBufferPools) {
            poolsToNotify.push_back(pool);
        }
    }
    for (const auto& pool : poolsToNotify) {
        pool->notifyGlobalMemoryAvailable();
    }
}

std::shared_ptr<CompletableFuture> NetworkObjectBufferPool::GetAvailableFuture()
{
    return availabilityHelper->GetAvailableFuture();
}

std::shared_ptr<BufferPool> NetworkObjectBufferPool::createBufferPool(int numRequiredBuffers, int maxUsedBuffers)
{
    return createBufferPool(numRequiredBuffers, maxUsedBuffers, 1, INT_MAX);
}
std::shared_ptr<BufferPool> NetworkObjectBufferPool::createBufferPool(
    int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel)
{
    LOG_INFO_IMP(
        "createBufferPool numRequiredBuffers : " << numRequiredBuffers << " maxUsedBuffers: " << maxUsedBuffers
                                                 << " numSubpartitions: " << numSubpartitions
                                                 << " maxBuffersPerChannel: " << maxBuffersPerChannel);
    auto res =
        internalCreateObjectBufferPool(numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    LOG("createBufferPool end");
    return res;
}

std::shared_ptr<BufferPool> NetworkObjectBufferPool::internalCreateObjectBufferPool(
    int numRequiredBuffers, int maxUsedBuffers, int numSubpartitions, int maxBuffersPerChannel)
{
    LOG("try to get lock ....");
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (isDestroyed_) {
        throw std::runtime_error("Network buffer pool has already been destroyed.");
    }
    uint64_t requiredMemory = static_cast<uint64_t>(numRequiredBuffers) * objectSegmentSize;
    LOG_PART(
        "numTotalRequiredMemory=" << std::to_string(numTotalRequiredMemory)
                                  << " totalMemory=" << std::to_string(totalMemory));

    if (numTotalRequiredMemory + requiredMemory > totalMemory) {
        throw std::runtime_error(
            "Insufficient network buffer memory: required " + std::to_string(requiredMemory) + " bytes, but only " +
            std::to_string(totalMemory - numTotalRequiredMemory) + " bytes available. " + getConfigDescription());
    }
    numTotalRequiredMemory += requiredMemory;
    // update availableMemory,usedMemory
    availableMemory -= requiredMemory;
    usedMemory += requiredMemory;
    LOG_PART("Before make shared new LocalObjectBufferPool");

    auto localObjectBufferPool = std::make_shared<LocalObjectBufferPool>(
        shared_from_this(), numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    LOG_PART("After make shared new LocalObjectBufferPool");
    localObjectBufferPool->postConstruct();
    LOG_PART("After make shared postConstruct");
    allBufferPools.insert(localObjectBufferPool);
    redistributeBuffers();
    LOG_PART("redistributeBuffers end");
    return localObjectBufferPool;
}

void NetworkObjectBufferPool::destroyBufferPool(std::shared_ptr<BufferPool> objectBufferPool)
{
    auto localObjectBufferPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(objectBufferPool);
    if (!localObjectBufferPool) {
        throw std::invalid_argument("bufferPool is no LocalBufferPool");
    }
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    if (allBufferPools.erase(localObjectBufferPool) > 0) {
        uint64_t releasedRequiredMemory = localObjectBufferPool->getRequiredMemory();
        numTotalRequiredMemory -= releasedRequiredMemory;
        redistributeBuffers();
    }
}

void NetworkObjectBufferPool::destroyAllBufferPools()
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    for (const auto& pool : allBufferPools) {
        pool->lazyDestroy();
    }
    allBufferPools.clear();
}

void NetworkObjectBufferPool::tryRedistributeBuffers(uint64_t memoryToRequest)
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock);

    LOG("numTotalRequiredMemory=" << std::to_string(numTotalRequiredMemory)
                                  << " totalMemory=" << std::to_string(totalMemory));
    if (numTotalRequiredMemory + memoryToRequest > totalMemory) {
        throw std::runtime_error(
            "Insufficient network buffer memory: required " + std::to_string(memoryToRequest) + " bytes, but only " +
            std::to_string(totalMemory - numTotalRequiredMemory) + " bytes available. " + getConfigDescription());
    }
    numTotalRequiredMemory += memoryToRequest;

    try {
        redistributeBuffers();
    } catch (const std::exception&) {
        numTotalRequiredMemory -= memoryToRequest;
        redistributeBuffers();
        throw;
    }
}

void NetworkObjectBufferPool::redistributeBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock);
    uint64_t availableMemoryToDistribute = totalMemory - numTotalRequiredMemory;

    if (availableMemoryToDistribute <= 0) {
        for (const auto& bufferPool : allBufferPools) {
            bufferPool->setMemoryBudget(bufferPool->getRequiredMemory());
        }
        return;
    }

    uint64_t totalCapacity = 0;
    for (const auto& bufferPool : allBufferPools) {
        uint64_t excessMax = bufferPool->getMaxMemory() - bufferPool->getRequiredMemory();
        totalCapacity += std::min(availableMemoryToDistribute, excessMax);
    }

    if (totalCapacity == 0) {
        return;
    }

    uint64_t memoryToDistribute = std::min(availableMemoryToDistribute, totalCapacity);
    uint64_t totalPartsUsed = 0;
    uint64_t numDistributedMemory = 0;
    for (const auto& bufferPool : allBufferPools) {
        uint64_t excessMax = bufferPool->getMaxMemory() - bufferPool->getRequiredMemory();
        if (excessMax == 0) {
            continue;
        }

        totalPartsUsed += std::min(availableMemoryToDistribute, excessMax);
        uint64_t myShare = memoryToDistribute * totalPartsUsed / totalCapacity - numDistributedMemory;
        numDistributedMemory += myShare;
        bufferPool->setMemoryBudget(bufferPool->getRequiredMemory() + myShare);
    }
}

std::string NetworkObjectBufferPool::getConfigDescription()
{
    return "The total network buffer memory is currently set to " + std::to_string(totalMemory) + " bytes (" +
           std::to_string(totalNumberOfObjectSegments) + " segments of " + std::to_string(objectSegmentSize) +
           " bytes each). " + "You can increase this by setting the configuration keys 'NETWORK_MEMORY_FRACTION', " +
           "'NETWORK_MEMORY_MIN', and 'NETWORK_MEMORY_MAX'";
}

std::string NetworkObjectBufferPool::toString() const
{
    return "NetworkObjectBufferPool";
}

int NetworkObjectBufferPool::getObjectSegmentSize()
{
    return objectSegmentSize;
}

GlobalVectorBatchBufferMetricGroup::SizeSupplierFactory
NetworkObjectBufferPool::CreateGlobalVectorBatchBufferMetricSupplierFactory()
{
    return [this](const std::string& metricName) -> SizeGauge::SizeSupplier {
        if (metricName == "objectSegmentSize") {
            return [this]() { return getObjectSegmentSize(); };
        }
        if (metricName == "totalNumberOfObjectSegments") {
            return [this]() { return getTotalNumberOfObjectSegments(); };
        }
        if (metricName == "totalMemory") {
            return [this]() { return ToSizeGaugeValue(getTotalMemory()); };
        }
        if (metricName == "availableObjectSegments") {
            return [this]() { return getNumberOfAvailableObjectSegments(); };
        }
        if (metricName == "availableMemory") {
            return [this]() { return ToSizeGaugeValue(getAvailableMemory()); };
        }
        if (metricName == "usedObjectSegments") {
            return [this]() { return getNumberOfUsedObjectSegments(); };
        }
        if (metricName == "usedMemory") {
            return [this]() { return ToSizeGaugeValue(getUsedMemory()); };
        }
        if (metricName == "registeredBufferPools") {
            return [this]() { return getNumberOfRegisteredBufferPools(); };
        }
        if (metricName == "bufferCount") {
            return [this]() { return countBuffers(); };
        }

        throw std::runtime_error("Unknown NetworkObjectBufferPool metric: " + metricName);
    };
}

} // namespace omnistream
