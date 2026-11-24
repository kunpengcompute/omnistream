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

#include "GlobalNettyBufferPool.h"
#include "LocalNettyBufferPool.h"
#include "core/include/common.h"

#include <stdexcept>

namespace {

int GetInitialRegularBufferCount(int totalNumberOfBuffers)
{
    if (totalNumberOfBuffers <= 0) {
        return 0;
    }
    return 1;
}

} // namespace

namespace omnistream {

GlobalNettyBufferPool::GlobalNettyBufferPool(const NettyBufferConf& conf)
    : conf_(conf),
      totalNumberOfBuffers_(conf.totalPoolSize)
{
    const int initialRegularBufferCount = GetInitialRegularBufferCount(totalNumberOfBuffers_);
    for (int i = 0; i < initialRegularBufferCount; ++i) {
        availableBuffers_.push_back(allocateRegularBuffer());
    }
    INFO_RELEASE(
        "GlobalNettyBufferPool created: totalBuffers="
        << totalNumberOfBuffers_ << ", initialAllocatedRegularBuffers=" << initialRegularBufferCount
        << ", availableBuffers=" << availableBuffers_.size() << ", bufferSize=" << conf_.bufferSize);
}

GlobalNettyBufferPool::~GlobalNettyBufferPool()
{
    destroy();
}

// --- Pooled buffer request/recycle ---

std::shared_ptr<NettyMemorySegment> GlobalNettyBufferPool::requestPooledBuffer()
{
    std::lock_guard<std::recursive_mutex> lock(availableBuffersMutex_);
    if (isDestroyed_) {
        return nullptr;
    }
    if (!availableBuffers_.empty()) {
        auto buffer = availableBuffers_.front();
        availableBuffers_.pop_front();
        return buffer;
    }
    if (allocatedRegularBufferCount_ >= totalNumberOfBuffers_) {
        return nullptr;
    }
    return allocateRegularBuffer();
}

void GlobalNettyBufferPool::recyclePooledBuffer(std::shared_ptr<NettyMemorySegment> buffer)
{
    {
        std::lock_guard<std::recursive_mutex> lock(availableBuffersMutex_);
        buffer->ResetBuffer();
        availableBuffers_.push_back(buffer);
        cv_.notify_all();
    }

    // Notify all local pools that a buffer is available in global,
    // so local waiters blocked in requestBuffer() can wake up and retry.
    {
        std::lock_guard<std::recursive_mutex> lock(factoryLock_);
        for (auto& pool : allLocalPools_) {
            pool->notifyBufferAvailable();
        }
    }
}

// --- Big buffer support ---

std::shared_ptr<NettyMemorySegment> GlobalNettyBufferPool::allocateBigBuffer(int size)
{
    int allocSize = size + NettyBufferInfo::elementNumBytes;
    uint8_t* rawBuffer = new uint8_t[allocSize]{};
    auto segment = std::make_shared<NettyMemorySegment>(rawBuffer, allocSize);
    long address = reinterpret_cast<long>(rawBuffer);
    {
        std::lock_guard<std::recursive_mutex> lock(bigBufferMutex_);
        activeBigBuffers_[address] = segment;
    }
    LOG("GlobalNettyBufferPool::allocateBigBuffer size=" << allocSize);
    return segment;
}

void GlobalNettyBufferPool::recycleBigBuffer(long bufferAddress)
{
    std::lock_guard<std::recursive_mutex> lock(bigBufferMutex_);
    activeBigBuffers_.erase(bufferAddress);
    // NettyMemorySegment frees its backing memory when the final owner releases it.
}

// --- Local pool factory ---

std::shared_ptr<LocalNettyBufferPool> GlobalNettyBufferPool::createLocalPool(int numOfSubPartition)
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock_);

    int numOfRequiredBuffer = numOfSubPartition + 1;
    int maxNumOfRequiredBuffer =
        numOfSubPartition * conf_.configuredBufferPerChannel + conf_.numOfFloatingBufferPerGate;
    if (isDestroyed_) {
        throw std::runtime_error("GlobalNettyBufferPool has already been destroyed.");
    }

    if (numTotalRequiredBuffers_ + numOfRequiredBuffer > totalNumberOfBuffers_) {
        throw std::runtime_error(
            "Insufficient number of netty buffers: required " + std::to_string(numOfRequiredBuffer) + ", but only " +
            std::to_string(totalNumberOfBuffers_ - numTotalRequiredBuffers_) + " available.");
    }

    numTotalRequiredBuffers_ += numOfRequiredBuffer;

    // NOTE: do NOT eagerly pre-allocate buffers from the global pool here.
    // Earlier versions called requestPooledBuffersBlocking() at this point while
    // holding factoryLock_, which can wait forever (no deadline) and serialise
    // every other createLocalPool / destroyLocalPool / recycle behind itself —
    // producing the silent "PartitionRequest never sent" stalls observed on
    // some TMs. LocalNettyBufferPool::requestBuffer()/requestBufferBlocking()
    // already pulls from the global pool lazily, without holding factoryLock_,
    // which is the safe path.
    std::shared_ptr<LocalNettyBufferPool> localPool;
    try {
        localPool = std::make_shared<LocalNettyBufferPool>(this, numOfRequiredBuffer, maxNumOfRequiredBuffer);

        allLocalPools_.insert(localPool);
        createdLocalPoolCount_++;
        if (numOfRequiredBuffer < maxNumOfRequiredBuffer) {
            resizableLocalPools_.insert(localPool);
        }

        redistributeBuffers();

        return localPool;
    } catch (...) {
        if (localPool) {
            allLocalPools_.erase(localPool);
            resizableLocalPools_.erase(localPool);
        }
        numTotalRequiredBuffers_ -= numOfRequiredBuffer;
        redistributeBuffers();
        throw;
    }
}

void GlobalNettyBufferPool::destroyLocalPool(std::shared_ptr<LocalNettyBufferPool> pool)
{
    std::lock_guard<std::recursive_mutex> lock(factoryLock_);

    bool destroyedLocalPool = false;
    if (allLocalPools_.erase(pool)) {
        destroyedLocalPool = true;
        destroyedLocalBufferCount_++;
        numTotalRequiredBuffers_ -= pool->getNumberOfRequiredBuffers();
        resizableLocalPools_.erase(pool);
        redistributeBuffers();
    }
    if (allLocalPools_.size() == 0) {
        int availableBufferCount = 0;
        int allocatedRegularBufferCount = 0;
        {
            std::lock_guard<std::recursive_mutex> availableLock(availableBuffersMutex_);
            availableBufferCount = static_cast<int>(availableBuffers_.size());
            allocatedRegularBufferCount = allocatedRegularBufferCount_;
        }
        INFO_RELEASE(
            "GlobalNettyBufferPool all LocalNettyBufferPool destroyed"
            << " createdLocalPoolCount=" << createdLocalPoolCount_ << " destroyedLocalBufferCount="
            << destroyedLocalBufferCount_ << " totalNumberOfBuffers=" << totalNumberOfBuffers_
            << " allocatedRegularBuffers=" << allocatedRegularBufferCount
            << " availableBuffers=" << availableBufferCount);
        destroyedLocalBufferCount_ = 0;
        createdLocalPoolCount_ = 0;
    }
}

// --- Redistribution (following Flink's algorithm) ---

void GlobalNettyBufferPool::redistributeBuffers()
{
    // Must be called under factoryLock_
    if (resizableLocalPools_.empty()) {
        return;
    }

    int numAvailable = totalNumberOfBuffers_ - numTotalRequiredBuffers_;

    if (numAvailable == 0) {
        for (auto& pool : resizableLocalPools_) {
            pool->setNumBuffers(pool->getNumberOfRequiredBuffers());
        }
        return;
    }

    // Distribute proportionally by capacity (following Flink's algorithm)
    long totalCapacity = 0;
    for (auto& pool : resizableLocalPools_) {
        int excessMax = pool->getMaxNumberOfBuffers() - pool->getNumberOfRequiredBuffers();
        totalCapacity += std::min(numAvailable, excessMax);
    }

    if (totalCapacity == 0) {
        return;
    }

    int toDistribute = static_cast<int>(std::min(static_cast<long>(numAvailable), totalCapacity));
    long totalPartsUsed = 0;
    int numDistributed = 0;

    for (auto& pool : resizableLocalPools_) {
        int excessMax = pool->getMaxNumberOfBuffers() - pool->getNumberOfRequiredBuffers();
        if (excessMax == 0) {
            continue;
        }

        totalPartsUsed += std::min(numAvailable, excessMax);
        int mySize =
            static_cast<int>(static_cast<long>(toDistribute) * totalPartsUsed / totalCapacity - numDistributed);
        numDistributed += mySize;

        pool->setNumBuffers(pool->getNumberOfRequiredBuffers() + mySize);
    }
}

// --- Metrics ---

int GlobalNettyBufferPool::getTotalBufferCount() const
{
    return isDestroyed_ ? 0 : totalNumberOfBuffers_;
}

int GlobalNettyBufferPool::getAvailableBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(availableBuffersMutex_);
    return static_cast<int>(availableBuffers_.size());
}

int GlobalNettyBufferPool::getUsedBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(availableBuffersMutex_);
    if (isDestroyed_) {
        return 0;
    }
    return allocatedRegularBufferCount_ - static_cast<int>(availableBuffers_.size());
}

int GlobalNettyBufferPool::getBufferSize() const
{
    return conf_.bufferSize;
}

GlobalNettyBufferMetricGroup::SizeSupplierFactory GlobalNettyBufferPool::CreateGlobalNettyBufferMetricSupplierFactory()
{
    return [this](const std::string& metricName) -> SizeGauge::SizeSupplier {
        if (metricName == "totalNumberOfBuffers") {
            return [this]() { return getTotalBufferCount(); };
        }
        if (metricName == "allocatedRegularBufferCount") {
            return [this]() {
                std::lock_guard<std::recursive_mutex> lock(availableBuffersMutex_);
                return allocatedRegularBufferCount_;
            };
        }
        if (metricName == "numTotalRequiredBuffers") {
            return [this]() {
                std::lock_guard<std::recursive_mutex> lock(factoryLock_);
                return numTotalRequiredBuffers_;
            };
        }
        if (metricName == "allLocalPoolsSize") {
            return [this]() {
                std::lock_guard<std::recursive_mutex> lock(factoryLock_);
                return static_cast<int>(allLocalPools_.size());
            };
        }
        if (metricName == "availableBuffers") {
            return [this]() { return getAvailableBufferCount(); };
        }

        throw std::runtime_error("Unknown GlobalNettyBufferPool metric: " + metricName);
    };
}

// --- Lifecycle ---

void GlobalNettyBufferPool::destroy()
{
    {
        std::lock_guard<std::recursive_mutex> lock(factoryLock_);
        isDestroyed_ = true;
    }
    {
        std::lock_guard<std::recursive_mutex> lock(availableBuffersMutex_);
        availableBuffers_.clear();
        allocatedRegularBufferCount_ = 0;
        cv_.notify_all();
    }
    {
        std::lock_guard<std::recursive_mutex> lock(bigBufferMutex_);
        activeBigBuffers_.clear();
    }
}

bool GlobalNettyBufferPool::isDestroyed() const
{
    return isDestroyed_;
}

std::shared_ptr<NettyMemorySegment> GlobalNettyBufferPool::allocateRegularBuffer()
{
    std::unique_ptr<uint8_t[]> rawBuffer(new uint8_t[conf_.bufferSize]{});
    auto segment = std::make_shared<NettyMemorySegment>(rawBuffer.get(), conf_.bufferSize);
    rawBuffer.release();
    allocatedRegularBufferCount_++;
    return segment;
}

} // namespace omnistream
