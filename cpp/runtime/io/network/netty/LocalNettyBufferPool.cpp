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

#include "LocalNettyBufferPool.h"
#include "GlobalNettyBufferPool.h"
#include "core/include/common.h"

namespace omnistream {

LocalNettyBufferPool::LocalNettyBufferPool(
    GlobalNettyBufferPool* globalPool, int numberOfRequiredBuffers, int maxNumberOfBuffers)
    : globalPool_(globalPool),
      numberOfRequiredBuffers_(numberOfRequiredBuffers),
      currentPoolSize_(numberOfRequiredBuffers),
      maxNumberOfBuffers_(maxNumberOfBuffers),
      bigTotalMemorySize_(static_cast<int64_t>(maxNumberOfBuffers) * globalPool->getBufferSize() * 10),
      availableBigMemorySize_(bigTotalMemorySize_)
{
    if (numberOfRequiredBuffers <= 0) {
        throw std::runtime_error(
            "Required number of buffers must be > 0, got " + std::to_string(numberOfRequiredBuffers));
    }
    if (maxNumberOfBuffers < numberOfRequiredBuffers) {
        throw std::runtime_error(
            "Max buffers (" + std::to_string(maxNumberOfBuffers) + ") must be >= required (" +
            std::to_string(numberOfRequiredBuffers) + ")");
    }
    LOG("LocalNettyBufferPool created: required=" << numberOfRequiredBuffers_ << ", max=" << maxNumberOfBuffers_
                                                  << ", bigTotalMemorySize=" << bigTotalMemorySize_);
}

LocalNettyBufferPool::~LocalNettyBufferPool()
{
    if (!isDestroyed_) {
        lazyDestroy();
    }
}

// initializeRequiredBuffers() removed — buffers are now lazily acquired from
// the global pool via requestBuffer()/requestBufferBlocking(), which avoids
// the forever-wait deadlock that the eager pre-allocation caused under
// GlobalNettyBufferPool::factoryLock_.

// --- Main API ---

std::shared_ptr<NettyMemorySegment> LocalNettyBufferPool::requestBuffer()
{
    std::unique_lock<std::recursive_mutex> lock(mutex_);

    if (isDestroyed_) {
        throw std::runtime_error("LocalNettyBufferPool has been destroyed.");
    }

    // Following Flink's LocalBufferPool.requestMemorySegment() pattern:
    // Loop until we get a buffer. If we haven't reached our entitled size,
    // we are guaranteed to eventually get one (via local recycle or global availability),
    // so we wait rather than return nullptr.
    while (true) {
        if (isDestroyed_) {
            throw std::runtime_error("LocalNettyBufferPool has been destroyed.");
        }

        // 1. Try local available queue
        if (!availableBuffers_.empty()) {
            auto buffer = availableBuffers_.front();
            availableBuffers_.pop_front();
            long address = reinterpret_cast<long>(buffer->GetOriginalAddress());
            allTrackedBuffers_[address] = buffer;
            requestRegularBufferCount_++;
            return buffer;
        }

        // 2. Try to get from global (if not at size limit)
        if (!isRequestedSizeReached()) {
            auto buffer = globalPool_->requestPooledBuffer();
            if (buffer) {
                numberOfRequestedBuffers_++;
                long address = reinterpret_cast<long>(buffer->GetOriginalAddress());
                allTrackedBuffers_[address] = buffer;
                requestRegularBufferCount_++;
                return buffer;
            }
            // Global is temporarily empty but we haven't reached our size yet —
            // wait for a buffer to become available (from local recycle or global)
            INFO_RELEASE(
                "OmniCredit Client is in back pressure state from global Netty Buffer pool is empty............");
            cv_.wait_for(lock, std::chrono::milliseconds(2000));
            continue;
        }

        // 3. Reached pool size limit — return nullptr (caller decides to block or not)
        return nullptr;
    }
}

std::shared_ptr<NettyMemorySegment> LocalNettyBufferPool::requestBufferBlocking()
{
    std::shared_ptr<NettyMemorySegment> buffer;
    while (!(buffer = requestBuffer())) {
        if (isDestroyed_) {
            throw std::runtime_error("LocalNettyBufferPool destroyed while waiting for buffer.");
        }
        std::unique_lock<std::recursive_mutex> lock(mutex_);
        // wait here means LocalBufferPool has RequestedSizeReached,so it needs to wait from its own availableBuffers_
        INFO_RELEASE("OmniCredit Client is in back pressure state from regular Netty Buffer request............");
        cv_.wait_for(lock, std::chrono::milliseconds(2000));
    }
    return buffer;
}

std::shared_ptr<NettyMemorySegment> LocalNettyBufferPool::requestBigBuffer(int size)
{
    std::unique_lock<std::recursive_mutex> lock(mutex_);

    if (isDestroyed_) {
        throw std::runtime_error("LocalNettyBufferPool has been destroyed.");
    }

    int allocSize = size + NettyBufferInfo::elementNumBytes;

    // Wait until we have enough big memory budget, with two exceptions:
    // 1. If this is the first big buffer (activeBigBufferCount_ == 0), always allow
    //    even if allocSize > availableBigMemorySize_, because the system must run.
    // 2. Otherwise, wait for big buffers to be recycled.
    while (availableBigMemorySize_ < allocSize && activeBigBufferCount_ > 0) {
        if (isDestroyed_) {
            throw std::runtime_error("LocalNettyBufferPool destroyed while waiting for big buffer.");
        }
        INFO_RELEASE("OmniCredit Client is in back pressure state from Big Netty Buffer request............");

        bigCv_.wait_for(lock, std::chrono::milliseconds(2000));
    }

    // Allocate the big buffer directly on heap
    uint8_t* raw = new uint8_t[allocSize]{};
    auto buffer = std::make_shared<NettyMemorySegment>(raw, allocSize);

    // Account memory and track
    availableBigMemorySize_ -= allocSize;
    activeBigBufferCount_++;
    long address = reinterpret_cast<long>(buffer->GetOriginalAddress());
    allTrackedBuffers_[address] = buffer;
    bigBufferSizes_[address] = allocSize;
    requestBigBufferCount_++;

    LOG("LocalNettyBufferPool::requestBigBuffer allocSize=" << allocSize
                                                            << ", availableBigMemorySize=" << availableBigMemorySize_
                                                            << ", activeBigBufferCount=" << activeBigBufferCount_);

    return buffer;
}

void LocalNettyBufferPool::recycleBuffer(long bufferAddress)
{
    std::shared_ptr<NettyMemorySegment> bufferToGlobal;

    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);

        auto it = allTrackedBuffers_.find(bufferAddress);
        if (it == allTrackedBuffers_.end()) {
            return;
        }

        auto buffer = it->second;
        if (buffer->DecreaseRefCount() > 0 || !buffer->GetEligibleRecycling()) {
            return;
        }
        buffer->ResetBuffer();
        allTrackedBuffers_.erase(it);

        // Big buffer: managed entirely in local pool
        auto bigIt = bigBufferSizes_.find(bufferAddress);
        if (bigIt != bigBufferSizes_.end()) {
            int64_t allocSize = bigIt->second;
            bigBufferSizes_.erase(bigIt);
            availableBigMemorySize_ += allocSize;
            activeBigBufferCount_--;
            recycleBigBufferCount_++;
            // buffer shared_ptr destructs -> delete[] the raw memory
            bigCv_.notify_all();
            return;
        }

        recycleRegularBufferCount_++;

        // Normal buffer: following Flink's recycle pattern
        // If destroyed or has excess -> return to global
        // (global's recyclePooledBuffer will notify all local pools)
        if (isDestroyed_ || hasExcessBuffers()) {
            numberOfRequestedBuffers_--;
            bufferToGlobal = buffer;
        } else {
            // Keep locally
            buffer->ResetBuffer();
            availableBuffers_.push_back(buffer);
            cv_.notify_one();
        }
    }

    if (bufferToGlobal) {
        globalPool_->recyclePooledBuffer(bufferToGlobal);
    }
}

void LocalNettyBufferPool::destroyCorruptBuffer(long bufferAddress)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = allTrackedBuffers_.find(bufferAddress);
    if (it != allTrackedBuffers_.end()) {
        numberOfRequestedBuffers_--;
        allTrackedBuffers_.erase(it);
        // Buffer memory is freed when shared_ptr destructs
    }
}

// --- Notification ---

void LocalNettyBufferPool::notifyBufferAvailable()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    cv_.notify_all();
}

// --- Pool management ---

void LocalNettyBufferPool::setNumBuffers(int numBuffers)
{
    std::vector<std::shared_ptr<NettyMemorySegment>> buffersToGlobal;

    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        currentPoolSize_ = std::min(numBuffers, maxNumberOfBuffers_);
        buffersToGlobal = removeExcessBuffers();
    }

    recycleBuffersToGlobal(buffersToGlobal);
}

int LocalNettyBufferPool::getNumberOfRequiredBuffers() const
{
    return numberOfRequiredBuffers_;
}

int LocalNettyBufferPool::getMaxNumberOfBuffers() const
{
    return maxNumberOfBuffers_;
}

int LocalNettyBufferPool::getCurrentPoolSize()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return currentPoolSize_;
}

int LocalNettyBufferPool::getNumberOfAvailableBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return static_cast<int>(availableBuffers_.size());
}

int LocalNettyBufferPool::getNumberOfRequestedBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return numberOfRequestedBuffers_;
}

int LocalNettyBufferPool::getNettyBufferSize()
{
    return globalPool_->getBufferSize();
}

int LocalNettyBufferPool::getRequestRegularBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return requestRegularBufferCount_;
}

int LocalNettyBufferPool::getRequestBigBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return requestBigBufferCount_;
}

int LocalNettyBufferPool::getRecycleRegularBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return recycleRegularBufferCount_;
}

int LocalNettyBufferPool::getRecycleBigBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return recycleBigBufferCount_;
}

int LocalNettyBufferPool::getBigTotalMemorySize()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return static_cast<int>(bigTotalMemorySize_);
}

int LocalNettyBufferPool::getAvailableBigMemorySize()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return static_cast<int>(availableBigMemorySize_);
}

int LocalNettyBufferPool::getActiveBigBufferCount()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return activeBigBufferCount_;
}

// --- Lifecycle ---

void LocalNettyBufferPool::lazyDestroy()
{
    std::vector<std::shared_ptr<NettyMemorySegment>> buffersToGlobal;

    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        if (isDestroyed_) {
            return;
        }

        INFO_RELEASE(
            "LocalNettyBufferPool lazyDestroy from "
            << this << " requestRegularBufferCount=" << requestRegularBufferCount_ << " requestBigBufferCount="
            << requestBigBufferCount_ << " recycleRegularBufferCount=" << recycleRegularBufferCount_
            << " recycleBigBufferCount=" << recycleBigBufferCount_
            << " numberOfRequestedBuffers=" << numberOfRequestedBuffers_
            << " availableBuffers=" << availableBuffers_.size() << " activeBigBufferCount=" << activeBigBufferCount_);

        // Return all available normal buffers to global
        while (!availableBuffers_.empty()) {
            buffersToGlobal.push_back(availableBuffers_.front());
            availableBuffers_.pop_front();
            numberOfRequestedBuffers_--;
        }

        // In-flight big buffers are still tracked in allTrackedBuffers_ and
        // bigBufferSizes_. Keep that metadata so later recycleBuffer() handles
        // them as local big buffers instead of returning them to the global pool.
        availableBigMemorySize_ = bigTotalMemorySize_;

        isDestroyed_ = true;
        cv_.notify_all();
        bigCv_.notify_all();
    }

    recycleBuffersToGlobal(buffersToGlobal);

    auto self = weak_from_this().lock();
    if (self) {
        globalPool_->destroyLocalPool(self);
    }
}

bool LocalNettyBufferPool::isDestroyed()
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return isDestroyed_;
}

// --- Private helpers ---

bool LocalNettyBufferPool::requestBufferFromGlobal()
{
    // Must be called under mutex_
    if (isRequestedSizeReached()) {
        return false;
    }

    auto buffer = globalPool_->requestPooledBuffer();
    if (buffer) {
        availableBuffers_.push_back(buffer);
        numberOfRequestedBuffers_++;
        return true;
    }
    return false;
}

bool LocalNettyBufferPool::isRequestedSizeReached()
{
    return numberOfRequestedBuffers_ >= currentPoolSize_;
}

bool LocalNettyBufferPool::hasExcessBuffers()
{
    return numberOfRequestedBuffers_ > currentPoolSize_;
}

void LocalNettyBufferPool::recycleBuffersToGlobal(const std::vector<std::shared_ptr<NettyMemorySegment>>& buffers)
{
    for (const auto& buffer : buffers) {
        globalPool_->recyclePooledBuffer(buffer);
    }
}

std::vector<std::shared_ptr<NettyMemorySegment>> LocalNettyBufferPool::removeExcessBuffers()
{
    std::vector<std::shared_ptr<NettyMemorySegment>> buffersToGlobal;

    // Must be called under mutex_
    while (hasExcessBuffers() && !availableBuffers_.empty()) {
        auto buffer = availableBuffers_.front();
        availableBuffers_.pop_front();
        numberOfRequestedBuffers_--;
        buffersToGlobal.push_back(buffer);
    }

    return buffersToGlobal;
}

} // namespace omnistream
