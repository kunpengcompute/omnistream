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

#include "LocalObjectBufferPool.h"

#include <algorithm>
#include <climits>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "NetworkObjectBufferPool.h"
#include "ObjectBufferBuilder.h"
#include "VectorBatchBuffer.h"
#include "runtime/objectsegment/ObjectSegmentFactory.h"

namespace omnistream {

LocalObjectBufferPool::LocalObjectBufferPool(
    std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool,
    int numberOfRequiredObjectSegments,
    int maxNumberOfMemorySegments,
    int numberOfSubpartitions,
    int maxBuffersPerChannel)
    : LocalBufferPool(
          networkObjBufferPool,
          numberOfSubpartitions,
          maxBuffersPerChannel,
          numberOfRequiredObjectSegments,
          numberOfRequiredObjectSegments,
          maxNumberOfMemorySegments,
          std::make_shared<AvailabilityHelper>()),
      networkObjBufferPool_(networkObjBufferPool),
      maxNumberOfObjectSegments_(maxNumberOfMemorySegments),
      objectSegmentSize(networkObjBufferPool != nullptr ? networkObjBufferPool->getObjectSegmentSize() : 0),
      subpartitionBufferRecyclers_(numberOfSubpartitions),
      subpartitionBuffersBool_(numberOfSubpartitions, false)

{
    LOG_PART("Beginning of constructor");
    LOG_PART(
        " numberOfRequiredObjectSegments_" << numberOfRequiredSegments_ << " maxNumberOfMemorySegments_"
                                           << maxNumberOfObjectSegments_ << " currentPoolSize_" << currentPoolSize_
                                           << " maxBuffersPerChannel_" << maxBuffersPerChannel_);

    if (numberOfRequiredSegments_ <= 0) {
        throw std::invalid_argument(
            "Required number of memory segments (" + std::to_string(numberOfRequiredSegments_) +
            ") should be larger than 0.");
    }

    if (maxNumberOfMemorySegments < numberOfRequiredSegments_) {
        throw std::invalid_argument(
            "Maximum number of memory segments (" + std::to_string(maxNumberOfMemorySegments) +
            ") should not be smaller than minimum (" + std::to_string(numberOfRequiredSegments_) + ").");
    }

    if (numberOfSubpartitions > 0 && maxBuffersPerChannel <= 0) {
        throw std::invalid_argument(
            "Maximum number of buffers for each channel (" + std::to_string(maxBuffersPerChannel) +
            ") should be larger than 0.");
    }

    requiredMemory_ = static_cast<uint64_t>(numberOfRequiredSegments_) * objectSegmentSize;
    maxAllowedMemory = static_cast<uint64_t>(objectSegmentSize) * maxNumberOfObjectSegments_ * 30;
    currentPoolMemoryBudget_ = requiredMemory_;
    availableMemory = requiredMemory_;
    usedMemory = 0;
    maxBuffersPerChannel_ = maxBuffersPerChannel;
    maxMemoryPerChannel_ = maxBuffersPerChannel * objectSegmentSize * 30;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        auto toNotify = checkAndUpdateAvailability();
        mayNotifyAvailable(toNotify);
    }
    LOG("LocalObjectBufferPool constructor end");
}

void LocalObjectBufferPool::postConstruct()
{
    LOG("LocalObjectBufferPool post constructor end");
    auto localPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(shared_from_this());
    defaultBufferRecycler_ = std::make_shared<SubpartitionBufferRecycler>(UNKNOWN_CHANNEL, localPool);
    for (size_t i = 0; i < subpartitionBufferRecyclers_.size(); i++) {
        subpartitionBufferRecyclers_[i] = std::make_shared<SubpartitionBufferRecycler>(i, localPool);
    }
}

// todo , should not have method like this
void LocalObjectBufferPool::reserveSegments(int numberOfSegmentsToReserve)
{
    if (numberOfSegmentsToReserve > numberOfRequiredSegments_) {
        throw std::invalid_argument("Can not reserve more segments than number of required segments.");
    }

    if (isDestroyed_) {
        throw std::runtime_error("Buffer pool has been destroyed.");
    }

    uint64_t memoryToReserve = numberOfSegmentsToReserve * objectSegmentSize;

    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    auto success = networkObjBufferPool_->requestMemoryBlocking(memoryToReserve);
    if (success) {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        availableMemory += memoryToReserve;
        toNotify = availabilityHelper_->getUnavailableToResetAvailable();
    }

    mayNotifyAvailable(toNotify);
}

std::shared_ptr<CompletableFuture> LocalObjectBufferPool::GetAvailableFuture()
{
    return availabilityHelper_->GetAvailableFuture();
}
bool LocalObjectBufferPool::isDestroyed()
{
    std::lock_guard<std::recursive_mutex> lock(objectSegmentMutex);
    return isDestroyed_;
}

int LocalObjectBufferPool::getMaxNumberOfSegments() const
{
    return maxNumberOfObjectSegments_;
}

void LocalObjectBufferPool::setNumBuffers(int numBuffers)
{
    setMemoryBudget(static_cast<uint64_t>(numBuffers) * objectSegmentSize);
}

void LocalObjectBufferPool::setMemoryBudget(uint64_t memoryBudget)
{
    std::shared_ptr<CompletableFuture> toNotify;
    uint64_t memoryToReturn = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        if (memoryBudget < requiredMemory_) {
            throw std::invalid_argument(
                "Buffer pool needs at least " + std::to_string(requiredMemory_) + " bytes, but tried to set to " +
                std::to_string(memoryBudget));
        }

        currentPoolMemoryBudget_ = std::min(memoryBudget, maxAllowedMemory);
        currentPoolSize_ = static_cast<int>(currentPoolMemoryBudget_ / objectSegmentSize);

        memoryToReturn = removeExcessObjectMemory();

        if (isDestroyed_) {
            toNotify = nullptr;
        } else if (availableMemory > 0 || usedMemory < currentPoolMemoryBudget_) {
            toNotify = availabilityHelper_->getUnavailableToResetAvailable();
        } else {
            availabilityHelper_->resetUnavailable();
        }
    }

    if (memoryToReturn > 0) {
        networkObjBufferPool_->returnMemory(memoryToReturn);
    }
    mayNotifyAvailable(toNotify);
}

bool LocalObjectBufferPool::shouldBeAvailable()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    LOG("shouldBeAvailable get lock");
    return availableMemory > 0 && unavailableSubpartitionsCount_ == 0;
}

BufferBuilder* LocalObjectBufferPool::requestBufferBuilder()
{
    return requestObjectBufferBuilder();
}

BufferBuilder* LocalObjectBufferPool::requestBufferBuilder(int targetChannel, uint64_t bytes)
{
    return requestObjectBufferBuilder(targetChannel, bytes);
}

BufferBuilder* LocalObjectBufferPool::requestBufferBuilderBlocking()
{
    return requestObjectBufferBuilderBlocking();
}

BufferBuilder* LocalObjectBufferPool::requestBufferBuilderBlocking(int targetChannel, uint64_t bytes)
{
    return requestObjectBufferBuilderBlocking(targetChannel, bytes);
}

std::shared_ptr<ObjectBuffer> LocalObjectBufferPool::requestObjectBuffer()
{
    return toObjectBuffer(requestObjectSegment(0));
}

ObjectBufferBuilder* LocalObjectBufferPool::requestObjectBufferBuilder()
{
    LOG(">>>");
    return toObjectBufferBuilder(requestObjectSegment(UNKNOWN_CHANNEL, 0), UNKNOWN_CHANNEL);
}

ObjectBufferBuilder* LocalObjectBufferPool::requestObjectBufferBuilder(int targetChannel, uint64_t bytes)
{
    return toObjectBufferBuilder(requestObjectSegment(targetChannel, bytes), targetChannel);
}

ObjectBufferBuilder* LocalObjectBufferPool::requestObjectBufferBuilderBlocking()
{
    LOG(">>>");
    return toObjectBufferBuilder(requestObjectSegmentBlocking(0), UNKNOWN_CHANNEL);
}

ObjectBufferBuilder* LocalObjectBufferPool::requestObjectBufferBuilderBlocking(int targetChannel, uint64_t bytes)
{
    return toObjectBufferBuilder(requestObjectSegmentBlocking(targetChannel, bytes), targetChannel);
}

ObjectSegment* LocalObjectBufferPool::requestObjectSegmentBlocking(uint64_t bytes)
{
    return requestObjectSegmentBlocking(UNKNOWN_CHANNEL, bytes);
}

std::shared_ptr<ObjectBuffer> LocalObjectBufferPool::toObjectBuffer(ObjectSegment* objectSegment)
{
    if (!objectSegment) {
        return nullptr;
    }
    auto recycler = defaultBufferRecycler_;
    if (!recycler) {
        auto localPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(shared_from_this());
        recycler = std::make_shared<SubpartitionBufferRecycler>(UNKNOWN_CHANNEL, localPool);
    }
    // VectorBatchBuffer has intrusive lifetime management and deletes itself when
    // its last reference is recycled. The shared_ptr is only an API handle and
    // must not try to delete the buffer a second time.
    return std::shared_ptr<ObjectBuffer>(new VectorBatchBuffer(objectSegment, recycler), [](ObjectBuffer*) {});
}

ObjectBufferBuilder* LocalObjectBufferPool::toObjectBufferBuilder(ObjectSegment* memorySegment, int targetChannel)
{
    if (!memorySegment) {
        return nullptr;
    }

    auto localPool = std::dynamic_pointer_cast<LocalObjectBufferPool>(shared_from_this());
    if (targetChannel == UNKNOWN_CHANNEL) {
        auto recycler = defaultBufferRecycler_;
        if (!recycler) {
            recycler = std::make_shared<SubpartitionBufferRecycler>(UNKNOWN_CHANNEL, localPool);
        }
        return new ObjectBufferBuilder(memorySegment, recycler);
    }

    if (!subpartitionBufferRecyclers_[targetChannel]) {
        subpartitionBufferRecyclers_[targetChannel] =
            std::make_shared<SubpartitionBufferRecycler>(targetChannel, localPool);
    }
    return new ObjectBufferBuilder(memorySegment, subpartitionBufferRecyclers_[targetChannel]);
}

void LocalObjectBufferPool::recycle(Segment* segment, int channel)
{
    LOG_TRACE("recycle an object segment............. " << segment << " for channel " << channel);
    auto* objectSegment = dynamic_cast<ObjectSegment*>(segment);
    if (!objectSegment) {
        throw std::runtime_error("Segment is not of type ObjectSegment.");
    }

    bool deleteSegment = false;
    int requestedSegmentCount = 0;
    int recycledSegmentCount = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(objectSegmentMutex);
        recycleSegmentNumber++;

        if (isDestroyed_) {
            deleteSegment = true;
            requestedSegmentCount = requestSegmentNumber;
            recycledSegmentCount = recycleSegmentNumber;
        } else {
            objectSegment->reset();
            availableSegments.push_back(objectSegment);
        }
    }

    if (deleteSegment) {
        INFO_RELEASE(
            "LocalObjectBufferPool recycled segment after destroy for channel "
            << channel << " from " << this << " and objectSegment = " << objectSegment << " request segment number = "
            << requestedSegmentCount << " recycle segment number = " << recycledSegmentCount);
        delete objectSegment;
    }
}

void LocalObjectBufferPool::recycleBytes(int64_t bytes, int channel)
{
    uint64_t returnedBytes = static_cast<uint64_t>(std::max<int64_t>(0, bytes));

    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    // bool returnToGlobal = false;
    uint64_t memoryToReturn = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        if (channel != UNKNOWN_CHANNEL) {
            subpartitionBuffersCount_[channel] -= static_cast<int>(returnedBytes);
            if (subpartitionBuffersBool_[channel] && subpartitionBuffersCount_[channel] < maxMemoryPerChannel_) {
                unavailableSubpartitionsCount_--;
                subpartitionBuffersBool_[channel] = false;
            }
        }

        if (!isDestroyed()) {
        }
        uint64_t returnToGlobalBytes = calculateByteNeedReturnToGlobal(returnedBytes);

        if (returnedBytes > 0) {
            usedMemory -= returnedBytes;
        }

        if (returnedBytes > 0) {
            availableMemory += (returnedBytes - returnToGlobalBytes);
        }

        if (availableMemory > 0 && unavailableSubpartitionsCount_ == 0) {
            toNotify = availabilityHelper_->getUnavailableToResetAvailable();
        }
        if (returnToGlobalBytes > 0) {
            memoryToReturn = returnToGlobalBytes;
        }
        recycledBytes += bytes;
        if (isDestroyed()) {
            INFO_RELEASE(
                "LocalObjectBufferPool after destroy+++++++++++++++++++++++ availableMemory = "
                << availableMemory << " usedMemory = " << usedMemory << " return bytes = " << returnedBytes
                << " for channel " << channel << " from " << this << " maxNumberOfObjectSegments_ = "
                << maxNumberOfObjectSegments_ << " numberOfRequiredSegments_ = " << numberOfRequiredSegments_
                << " maxBuffersPerChannel = " << maxBuffersPerChannel_
                << " currentPoolMemoryBudget_ = " << currentPoolMemoryBudget_ << " request bytes = " << requestedBytes
                << " recycle bytes  = " << recycledBytes);
        }
    }

    if (memoryToReturn > 0) {
        networkObjBufferPool_->returnMemory(memoryToReturn);
    }
    // When not retained in the local freelist (excess/shrink path), the lightweight segment
    // is simply dropped here: objectSegment's shared_ptr releases at scope end and frees it.
    // No global segment-pool interaction on the recycle hot path.
    mayNotifyAvailable(toNotify);
}

void LocalObjectBufferPool::mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify)
{
    if (toNotify != nullptr) {
        toNotify->complete();
    }
}

void LocalObjectBufferPool::notifyGlobalMemoryAvailable()
{
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        // Only wake up if the local pool still has room to grow from global
        if (!isRequestedSizeReached()) {
            toNotify = availabilityHelper_->getUnavailableToResetAvailable();
        }
    }
    mayNotifyAvailable(toNotify);
}

bool LocalObjectBufferPool::requestMemory(uint64_t bytes)
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    if (isDestroyed_) {
        return false;
    }
    if (bytes > availableMemory) {
        return false;
    }
    availableMemory -= bytes;
    usedMemory += bytes;
    return true;
}

bool LocalObjectBufferPool::requestMemoryFromGlobal(uint64_t bytes)
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    if (isDestroyed_) {
        return false;
    }
    return networkObjBufferPool_->requestMemory(bytes);
}

void LocalObjectBufferPool::returnMemory(uint64_t bytes)
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    availableMemory += bytes;
    usedMemory -= bytes;
}

Segment* LocalObjectBufferPool::requestSegment(uint64_t bytes)
{
    return requestObjectSegment(UNKNOWN_CHANNEL, bytes);
}

Segment* LocalObjectBufferPool::requestSegment(int targetChannel, uint64_t bytes)
{
    return requestObjectSegment(targetChannel, bytes);
}

Segment* LocalObjectBufferPool::requestSegmentBlocking(uint64_t bytes)
{
    return requestSegmentBlocking(UNKNOWN_CHANNEL, bytes);
}

Segment* LocalObjectBufferPool::requestSegmentBlocking(int targetChannel, uint64_t bytes)
{
    return requestObjectSegmentBlocking(targetChannel, bytes);
}

ObjectSegment* LocalObjectBufferPool::requestObjectSegmentBlocking(int targetChannel, uint64_t bytes)
{
    ObjectSegment* segment;
    while (!(segment = requestObjectSegment(targetChannel, bytes))) {
        // since requestObjectSegment will always return an ObjectSegment, so we do not need to wait here
    }
    return segment;
}

ObjectSegment* LocalObjectBufferPool::requestObjectSegment(uint64_t bytes)
{
    return requestObjectSegment(UNKNOWN_CHANNEL, bytes);
}

ObjectSegment* LocalObjectBufferPool::requestObjectSegment(int targetChannel, uint64_t bytes)
{
    ObjectSegment* segment = nullptr; // may be served from the local freelist
    std::lock_guard<std::recursive_mutex> lock(objectSegmentMutex);
    if (!availableSegments.empty()) {
        segment = static_cast<ObjectSegment*>(availableSegments.front());
        availableSegments.pop_front();
    }
    if (!segment) {
        segment = ObjectSegmentFactory::allocateUnpooledSegment(std::max(100, objectSegmentSize / 100));
    }
    requestSegmentNumber++;
    return segment;
}

void LocalObjectBufferPool::cancel()
{
    LocalBufferPool::cancel(); // sets cancelled_ = true

    // Wake any thread blocked on the availability future so it re-runs its loop, sees
    // cancelled_ and throws. If the pool is currently "available" the future is already
    // completed and blocked threads wake by themselves -- getUnavailableToResetAvailable()
    // then returns nullptr and this is a no-op.
    std::shared_ptr<CompletableFuture> toNotify;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        toNotify = availabilityHelper_->getUnavailableToResetAvailable();
    }
    mayNotifyAvailable(toNotify);
}

void LocalObjectBufferPool::lazyDestroyMemory()
{
    uint64_t memoryToReturn = 0;
    uint64_t usedMemoryAtDestroy = 0;
    uint64_t poolMemoryBudgetAtDestroy = 0;
    uint64_t requestedBytesAtDestroy = 0;
    uint64_t recycledBytesAtDestroy = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        memoryToReturn = availableMemory;
        availableMemory = 0;

        while (!registeredListeners_.empty()) {
            auto listener = registeredListeners_.front();
            registeredListeners_.pop_front();
            listener->notifyBufferDestroyed();
        }

        if (waitingForGlobalMemory_) {
            waitingForGlobalMemory_ = false;
            networkObjBufferPool_->decMemoryWaiters();
        }

        usedMemoryAtDestroy = usedMemory;
        poolMemoryBudgetAtDestroy = currentPoolMemoryBudget_;
        requestedBytesAtDestroy = requestedBytes;
        recycledBytesAtDestroy = recycledBytes;
    }

    if (memoryToReturn > 0) {
        networkObjBufferPool_->returnMemory(memoryToReturn);
    }
    INFO_RELEASE(
        "LocalObjectBufferPool::::::: lazy destroy memory --------------> returned availableMemory = "
        << memoryToReturn << " usedMemory = " << usedMemoryAtDestroy << " from " << this
        << " maxNumberOfObjectSegments_ = " << maxNumberOfObjectSegments_ << " numberOfRequiredSegments_ = "
        << numberOfRequiredSegments_ << " maxBuffersPerChannel = " << maxBuffersPerChannel_
        << " currentPoolMemoryBudget_ = " << poolMemoryBudgetAtDestroy << " request bytes number = "
        << requestedBytesAtDestroy << " recycle bytes number = " << recycledBytesAtDestroy);
}

void LocalObjectBufferPool::lazyDestroySegment()
{
    int requestedSegmentsAtDestroy = 0;
    int recycledSegmentsAtDestroy = 0;
    std::deque<Segment*> segmentsToDelete;
    {
        std::lock_guard<std::recursive_mutex> lock(objectSegmentMutex);
        availableSegments.swap(segmentsToDelete);
        requestedSegmentsAtDestroy = requestSegmentNumber;
        recycledSegmentsAtDestroy = recycleSegmentNumber;
    }

    const size_t releasedSegmentCount = segmentsToDelete.size();
    for (Segment* segment : segmentsToDelete) {
        delete segment;
    }
    INFO_RELEASE(
        "LocalObjectBufferPool::::::: lazy destroy segment --------------> from "
        << this << " request segment number = " << requestedSegmentsAtDestroy << " recycle segment number = "
        << recycledSegmentsAtDestroy << " released cached segment number = " << releasedSegmentCount);
}

void LocalObjectBufferPool::lazyDestroy()
{
    {
        std::lock_guard<std::recursive_mutex> memoryLock(memoryMutex);
        std::lock_guard<std::recursive_mutex> objectSegmentLock(objectSegmentMutex);
        if (isDestroyed_) {
            return;
        }

        isDestroyed_ = true;
    }

    lazyDestroyMemory();
    lazyDestroySegment();
    networkObjBufferPool_->destroyBufferPool(shared_from_this());
}

std::string LocalObjectBufferPool::toString() const
{
    return "[size: " + std::to_string(currentPoolSize_) + ", required: " + std::to_string(numberOfRequiredSegments_) +
           ", usedMemory: " + std::to_string(usedMemory) + ", cachedMemory: " + std::to_string(availableMemory) +
           ", available: " + std::to_string(availableSegments.size()) +
           ", max: " + std::to_string(maxNumberOfObjectSegments_) +
           ", memoryBudget: " + std::to_string(currentPoolMemoryBudget_) +
           ", listeners: " + std::to_string(registeredListeners_.size()) +
           ", subpartitions: " + std::to_string(subpartitionBuffersCount_.size()) +
           ", maxBuffersPerChannel: " + std::to_string(maxBuffersPerChannel_) +
           ", destroyed: " + (isDestroyed_ ? "true" : "false") + "]";
}

void LocalObjectBufferPool::returnSegment(Segment* segment)
{
    auto toRecycledSegment = dynamic_cast<ObjectSegment*>(segment);
    if (!toRecycledSegment) {
        throw std::runtime_error("Segment is not of type ObjectSegment.");
    }
    returnObjectSegment(toRecycledSegment);
}

void LocalObjectBufferPool::returnObjectSegment(ObjectSegment* segment)
{
    if (!segment) {
        return;
    }
    recycle(segment, UNKNOWN_CHANNEL);
}

void LocalObjectBufferPool::returnExcessSegments()
{
    returnExcessObjectSegments();
}

void LocalObjectBufferPool::returnExcessObjectSegments()
{
    uint64_t memoryToReturn = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        memoryToReturn = removeExcessObjectMemory();
    }
    if (memoryToReturn > 0) {
        networkObjBufferPool_->returnMemory(memoryToReturn);
    }
}

uint64_t LocalObjectBufferPool::removeExcessObjectMemory()
{
    // Must be called under memoryMutex.
    if (usedMemory + availableMemory <= currentPoolMemoryBudget_) {
        return 0;
    }

    uint64_t excessiveMemory = availableMemory + usedMemory - currentPoolMemoryBudget_;
    uint64_t memoryToReturn = std::min(availableMemory, excessiveMemory);
    availableMemory -= memoryToReturn;
    return memoryToReturn;
}

bool LocalObjectBufferPool::hasExcessBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    return usedMemory + availableMemory > currentPoolMemoryBudget_;
}

bool LocalObjectBufferPool::isRequestedSizeReached()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    return usedMemory + availableMemory > currentPoolMemoryBudget_;
}

LocalObjectBufferPool::SubpartitionBufferRecycler::SubpartitionBufferRecycler(
    int channel, std::shared_ptr<LocalObjectBufferPool> bufferPool)
    : channel_(channel),
      bufferPool_(bufferPool)
{
}

void LocalObjectBufferPool::SubpartitionBufferRecycler::recycle(Segment* segment)
{
    bufferPool_->recycle(segment, channel_);
}

void LocalObjectBufferPool::SubpartitionBufferRecycler::recycleBytes(int64_t bytes)
{
    bufferPool_->recycleBytes(bytes, channel_);
}

uint64_t LocalObjectBufferPool::getRequiredMemory() const
{
    return requiredMemory_;
}

uint64_t LocalObjectBufferPool::getMaxMemory() const
{
    return maxAllowedMemory;
}

int LocalObjectBufferPool::getObjectSegmentSize() const
{
    return objectSegmentSize;
}

uint64_t LocalObjectBufferPool::getCurrentPoolMemoryBudget() const
{
    return currentPoolMemoryBudget_;
}

uint64_t LocalObjectBufferPool::getUsedMemory() const
{
    return usedMemory;
}

uint64_t LocalObjectBufferPool::getAvailableMemory() const
{
    return availableMemory;
}

uint64_t LocalObjectBufferPool::getMaxMemoryPerChannel() const
{
    return maxMemoryPerChannel_;
}

int LocalObjectBufferPool::getRequestSegmentNumber() const
{
    return requestSegmentNumber;
}

int LocalObjectBufferPool::getRecycleSegmentNumber() const
{
    return recycleSegmentNumber;
}

int LocalObjectBufferPool::getNumberOfAvailableSegments()
{
    std::lock_guard<std::recursive_mutex> lock(objectSegmentMutex);
    return static_cast<int>(availableSegments.size());
}

int LocalObjectBufferPool::getNumBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    return currentPoolSize_;
}

int LocalObjectBufferPool::bestEffortGetNumOfUsedBuffers() const
{
    int best = requestSegmentNumber - recycleSegmentNumber;
    return best > 0 ? best : 0;
}

bool LocalObjectBufferPool::requestSegmentFromGlobal()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    if (isDestroyed_) {
        return false;
    }
    if (availableMemory > 0) {
        return true;
    }
    if (isRequestedSizeReached()) {
        return false;
    }
    return networkObjBufferPool_->getAvailableMemory() > 0;
}

std::shared_ptr<Buffer> LocalObjectBufferPool::requestBuffer()
{
    return requestObjectBuffer();
}

bool LocalObjectBufferPool::checkAvailability()
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    if (availableMemory > 0) {
        return unavailableSubpartitionsCount_ == 0;
    } else {
        return false;
    }
}

void LocalObjectBufferPool::SetBufferPoolMetric(AbstractMetricGroup metricGroup)
{
}
void LocalObjectBufferPool::chargeMemoryBlocking(int targetChannel, uint64_t bytes)
{
    while (!(chargeMemory(targetChannel, bytes))) {
        if (bytes > 8) {
            INFO_RELEASE(
                "backpressure::::::::::: availableMemory = "
                << availableMemory << " usedMemory = " << usedMemory << " required bytes = " << bytes << " for channel "
                << targetChannel << " from " << this << " maxNumberOfObjectSegments_ = " << maxNumberOfObjectSegments_
                << " numberOfRequiredSegments_ = " << numberOfRequiredSegments_ << " maxBuffersPerChannel = "
                << maxBuffersPerChannel_ << " currentPoolMemoryBudget_ = " << currentPoolMemoryBudget_
                << " request segment number = " << requestSegmentNumber
                << " recycle segment number = " << recycleSegmentNumber << " requested bytes  = " << requestedBytes
                << " recycled bytes = " << recycledBytes);
        }
        if (cancelled_.load()) {
            // Deregister our waiter reservation (if any) so the global counter doesn't leak.
            if (waitingForGlobalMemory_) {
                waitingForGlobalMemory_ = false;
                networkObjBufferPool_->decMemoryWaiters();
            }
            throw std::runtime_error("task has been cancelled");
        }
        // Recheck once more before blocking: the failed chargeMemory above registered us as a
        // global-memory waiter (memoryWaiters_), but a concurrent returnMemory() may have added
        // memory AND read memoryWaiters_ == 0 just before our registration, skipping its notify
        // fan-out. This recheck re-acquires the global lock and observes that memory, so we
        // proceed instead of sleeping on a notification that was skipped. Same pattern as
        // requestObjectSegmentBlocking.
        if (chargeMemory(targetChannel, bytes)) {
            break;
        }
        availabilityHelper_->GetAvailableFuture()->get();
    }
}

bool LocalObjectBufferPool::chargeMemory(int targetChannel, uint64_t bytes)
{
    {
        std::lock_guard<std::recursive_mutex> lock(memoryMutex);
        if (isDestroyed_) {
            throw std::runtime_error("Buffer pool is destroyed.");
        }

        bool allocMemoryStatue = false;

        if (requestMemory(bytes)) {
            allocMemoryStatue = true;
        } else if (!isRequestedSizeReached()) {
            // The local cache is already charged to the global pool. Consume it
            // and request only the shortfall, otherwise cached free bytes can
            // make a single batch permanently impossible to admit.
            const uint64_t missingBytes = bytes - availableMemory;
            if (requestMemoryFromGlobal(missingBytes)) {
                availableMemory = 0;
                usedMemory += bytes;
                // acquiredFromGlobal = true;
                allocMemoryStatue = true;
            } else if (!waitingForGlobalMemory_) {
                // We are allowed to grow from the global pool but it is exhausted, so this
                // request will block in requestObjectSegmentBlocking() waiting for global memory.
                // Register once as a waiter so returnMemory() performs its notification fan-out
                // (which it otherwise skips when memoryWaiters_ == 0). Cleared on acquisition below.
                waitingForGlobalMemory_ = true;
                networkObjBufferPool_->incMemoryWaiters();
            }
        }

        if (!allocMemoryStatue) {
            availabilityHelper_->resetUnavailable();
            return false;
        }

        // Acquired memory: if we had registered as a global-memory waiter, deregister now.
        if (waitingForGlobalMemory_) {
            waitingForGlobalMemory_ = false;
            networkObjBufferPool_->decMemoryWaiters();
        }

        if (targetChannel != UNKNOWN_CHANNEL) {
            subpartitionBuffersCount_[targetChannel] += static_cast<int>(bytes);
            if (!subpartitionBuffersBool_[targetChannel] &&
                subpartitionBuffersCount_[targetChannel] >= maxMemoryPerChannel_) {
                // channelBecameUnavailable = true;
                if (targetChannel >= 0) {
                    unavailableSubpartitionsCount_++;
                    subpartitionBuffersBool_[targetChannel] = true;
                }
            }
        }

        requestedBytes += bytes;
    }
    return true;
}

int64_t LocalObjectBufferPool::calculateByteNeedReturnToGlobal(int64_t returnBytes)
{
    std::lock_guard<std::recursive_mutex> lock(memoryMutex);
    if (isDestroyed_) {
        return returnBytes;
    } else {
        if (usedMemory + availableMemory > currentPoolMemoryBudget_) {
            int64_t excessiveUsage = usedMemory + availableMemory - currentPoolMemoryBudget_;
            return std::min(returnBytes, excessiveUsage);
        } else {
            return 0;
        }
    }
}
} // namespace omnistream
