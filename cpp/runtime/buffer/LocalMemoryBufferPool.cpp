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

#include "LocalMemoryBufferPool.h"

namespace omnistream::datastream {
LocalMemoryBufferPool::LocalMemoryBufferPool(
    std::shared_ptr<NetworkMemoryBufferPool> networkMemoryBufferPool,
    int numberOfRequiredMemorySegments,
    int maxNumberOfMemorySegments,
    int numberOfSubpartitions,
    int maxBuffersPerChannel)
    : LocalBufferPool(
          networkMemoryBufferPool,
          numberOfSubpartitions,
          maxBuffersPerChannel,
          numberOfRequiredMemorySegments,
          numberOfRequiredMemorySegments,
          maxNumberOfMemorySegments,
          std::make_shared<AvailabilityHelper>()),
      networkMemoryBufferPool(networkMemoryBufferPool),
      numberOfRequestedMemorySegments(0),
      subpartitionBufferRecyclers_(numberOfSubpartitions, nullptr)
{
    LOG_PART("Beginning of constructor");
    INFO_RELEASE(
        " numberOfRequiredObjectSegments_" << numberOfRequiredSegments_ << " maxNumberOfMemorySegments_"
                                           << maxNumberOfSegments << "currentPoolSize_" << currentPoolSize_
                                           << " maxBuffersPerChannel_" << maxBuffersPerChannel_);

    if (numberOfRequiredMemorySegments <= 0) {
        throw std::invalid_argument(
            "Required number of memory segments (" + std::to_string(numberOfRequiredMemorySegments) +
            ") should be larger than 0.");
    }

    if (numberOfSubpartitions > 0) {
        if (maxBuffersPerChannel <= 0) {
            throw std::invalid_argument(
                "Maximum number of buffers for each channel (" + std::to_string(maxBuffersPerChannel) +
                ") should be larger than 0.");
        }
    }

    {
        std::unique_lock<std::recursive_mutex> lock(availableSegmentsLock);
        LOG("constructor get lock");
        //            if (checkAvailability()) {
        //                availabilityHelper_->resetAvailable();
        //            }
        //            checkConsistentAvailability();
        auto toNotify = checkAndUpdateAvailability();
        mayNotifyAvailable(toNotify);
    }
    LOG("LocalObjectBufferPool constructor end");
}

void LocalMemoryBufferPool::postConstruct()
{
    LOG("LocalObjectBufferPool post constructor end");
    for (size_t i = 0; i < subpartitionBufferRecyclers_.size(); i++) {
        subpartitionBufferRecyclers_[i] = std::make_shared<SubpartitionBufferRecycler>(i, shared_from_this());
    }
}

void LocalMemoryBufferPool::reserveSegments(int numberOfSegmentsToReserve)
{
    /*if (numberOfSegmentsToReserve > numberOfRequiredMemorySegments) {
        throw std::invalid_argument("Can not reserve more segments than number of required segments.");
    }*/

    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
        if (isDestroyed_) {
            throw std::runtime_error("Buffer pool has been destroyed.");
        }

        if (numberOfRequestedMemorySegments < numberOfSegmentsToReserve) {
            auto segments = networkMemoryBufferPool->requestPooledMemorySegmentsBlocking(
                numberOfSegmentsToReserve - numberOfRequiredSegments_);
            availableSegments.insert(availableSegments.end(), segments.begin(), segments.end());
            INFO_RELEASE(
                "availableSegments size: " << availableSegments.size() << ", segments: " << segments.size()
                                           << "numberOfSegmentsToReserve: " << numberOfSegmentsToReserve
                                           << ", numberOfRequiredMemorySegments: " << numberOfRequiredSegments_);
            toNotify = availabilityHelper_->getUnavailableToResetAvailable();
        }
    }
    mayNotifyAvailable(toNotify);
}

bool LocalMemoryBufferPool::isDestroyed()
{
    std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
    return isDestroyed_;
}

int LocalMemoryBufferPool::getMaxNumberOfSegments() const
{
    return maxNumberOfSegments;
}

int LocalMemoryBufferPool::getNumberOfAvailableSegments()
{
    std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
    return static_cast<int>(availableSegments.size());
}

int LocalMemoryBufferPool::getNumBuffers()
{
    std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
    return static_cast<int>(currentPoolSize_);
}

int LocalMemoryBufferPool::bestEffortGetNumOfUsedBuffers() const
{
    int best = numberOfRequestedMemorySegments - availableSegments.size();
    return best > 0 ? best : 0;
}

void LocalMemoryBufferPool::lazyDestroy()
{
}

void LocalMemoryBufferPool::returnSegment(Segment* segment)
{
    auto toRecycledSegment = dynamic_cast<MemorySegment*>(segment);
    if (!toRecycledSegment) {
        throw std::runtime_error("Segment is not of type MemorySegment.");
    }
    returnMemorySegment(toRecycledSegment);
}

void LocalMemoryBufferPool::returnMemorySegment(MemorySegment* segment)
{
    numberOfRequestedMemorySegments--;
    networkMemoryBufferPool->recyclePooledMemorySegment(segment);
}

std::shared_ptr<Buffer> LocalMemoryBufferPool::requestBuffer()
{
    return requestNetworkBuffer();
};

BufferBuilder* LocalMemoryBufferPool::requestBufferBuilder()
{
    return requestMemoryBufferBuilder();
};

BufferBuilder* LocalMemoryBufferPool::requestBufferBuilder(int targetChannel, uint64_t /*bytes*/)
{
    return requestMemoryBufferBuilder(targetChannel);
};

BufferBuilder* LocalMemoryBufferPool::requestBufferBuilderBlocking()
{
    return requestMemoryBufferBuilderBlocking();
};

BufferBuilder* LocalMemoryBufferPool::requestBufferBuilderBlocking(int targetChannel, uint64_t /*bytes*/)
{
    return requestMemoryBufferBuilderBlocking(targetChannel);
};

std::shared_ptr<NetworkBuffer> LocalMemoryBufferPool::toNetworkBuffer(MemorySegment* memorySegment)
{
    LOG(">>>");
    if (!memorySegment) {
        return nullptr;
    }
    // NetworkBuffer has intrusive lifetime management and deletes itself when
    // its last reference is recycled. The shared_ptr is only an API handle and
    // must not try to delete the buffer a second time.
    return std::shared_ptr<NetworkBuffer>(new NetworkBuffer(memorySegment, shared_from_this()), [](NetworkBuffer*) {});
}

std::shared_ptr<NetworkBuffer> LocalMemoryBufferPool::requestNetworkBuffer()
{
    return toNetworkBuffer(requestMemorySegment());
}

MemoryBufferBuilder* LocalMemoryBufferPool::toMemoryBufferBuilder(MemorySegment* memorySegment, int targetChannel)
{
    LOG("LocalMemoryBufferPool::toMemoryBufferBuilder running");
    if (!memorySegment) {
        return nullptr;
    }

    if (targetChannel == UNKNOWN_CHANNEL) {
        LOG("LocalMemoryBufferPool with subpartitionBufferRecyclers_ with this");
        return new MemoryBufferBuilder(memorySegment, shared_from_this());
    } else {
        LOG("LocalMemoryBufferPool with subpartitionBufferRecyclers_");
        LOG("subpartitionBufferRecyclers_[targetChannel] "
            << std::to_string(targetChannel) << "  "
            << ((subpartitionBufferRecyclers_[targetChannel])
                    ? std::to_string(reinterpret_cast<long>(subpartitionBufferRecyclers_[targetChannel].get()))
                    : "nullptr")
            << std::endl);

        return new MemoryBufferBuilder(memorySegment, subpartitionBufferRecyclers_[targetChannel]);
    }
}

MemoryBufferBuilder* LocalMemoryBufferPool::requestMemoryBufferBuilder()
{
    return toMemoryBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
}

MemoryBufferBuilder* LocalMemoryBufferPool::requestMemoryBufferBuilder(int targetChannel)
{
    return toMemoryBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
}

MemoryBufferBuilder* LocalMemoryBufferPool::requestMemoryBufferBuilderBlocking()
{
    return toMemoryBufferBuilder(requestMemorySegmentBlocking(), UNKNOWN_CHANNEL);
}

MemoryBufferBuilder* LocalMemoryBufferPool::requestMemoryBufferBuilderBlocking(int targetChannel)
{
    return toMemoryBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
}

Segment* LocalMemoryBufferPool::requestSegment()
{
    return requestMemorySegment();
}

Segment* LocalMemoryBufferPool::requestSegment(int targetChannel)
{
    return requestMemorySegment(targetChannel);
}

Segment* LocalMemoryBufferPool::requestSegmentBlocking()
{
    return requestMemorySegmentBlocking();
}

Segment* LocalMemoryBufferPool::requestSegmentBlocking(int targetChannel)
{
    return requestMemorySegmentBlocking(targetChannel);
}

MemorySegment* LocalMemoryBufferPool::requestMemorySegment()
{
    return requestMemorySegment(UNKNOWN_CHANNEL);
}

MemorySegment* LocalMemoryBufferPool::requestPooledMemorySegment()
{
    MemorySegment* segment = networkMemoryBufferPool->requestPooledMemorySegment();
    if (segment != nullptr) {
        numberOfRequestedMemorySegments++;
    }
    return segment;
}

MemorySegment* LocalMemoryBufferPool::requestOverdraftMemorySegmentFromGlobal()
{
    int maxOverdraftBuffersPerGate = 0; // todo: value from config
    if (numberOfRequestedMemorySegments - currentPoolSize_ >= maxOverdraftBuffersPerGate) {
        return nullptr;
    }

    return requestPooledMemorySegment();
}

MemorySegment* LocalMemoryBufferPool::requestMemorySegment(int targetChannel)
{
    LOG("requestObjectSegment in LocalObjectBufferPool");
    MemorySegment* segment = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
        LOG("get lock std::this_thread::get_id()" << std::this_thread::get_id());
        if (isDestroyed_) {
            throw std::runtime_error("Buffer pool is destroyed.");
        }

        //            //todo ? why add this check
        //            if (targetChannel != UNKNOWN_CHANNEL && subpartitionBuffersCount_[targetChannel] >=
        //            maxBuffersPerChannel_) {
        //                INFO_RELEASE("----wait buffer release----")
        //                return nullptr;
        //            }

        if (!availableSegments.empty()) {
            LOG("availableMemorySegments is not empty");
            segment = reinterpret_cast<MemorySegment*>(availableSegments.front());
            LOG("availableMemorySegments is segment.get()" << segment << "segment " << segment);
            availableSegments.pop_front();
            LOG("availableSegments.size()" << availableSegments.size());
            LOG_PART(
                "requestMemorySegment for targetChannel " << targetChannel << "availableSegments.size()"
                                                          << availableSegments.size());
        } else if (isRequestedSizeReached()) {
            LOG("availableMemorySegments is empty");
            // todo: the method needs to be improved.
            segment = requestOverdraftMemorySegmentFromGlobal();
        }

        if (segment == nullptr) {
            return nullptr;
        }

        if (targetChannel != UNKNOWN_CHANNEL) {
            if ((++subpartitionBuffersCount_[targetChannel]) == maxBuffersPerChannel_) {
                unavailableSubpartitionsCount_++;
            }
        }

        auto toNotify = checkAndUpdateAvailability();
        mayNotifyAvailable(toNotify);

        /*if (!checkAvailability()) {
            availabilityHelper_->resetUnavailable();
        }else {
            availabilityHelper_->resetAvailable();
        }

        checkConsistentAvailability();*/
        LOG("unlock std::this_thread::get_id()" << std::this_thread::get_id());
    }
    return segment;
}

MemorySegment* LocalMemoryBufferPool::requestMemorySegmentBlocking()
{
    return requestMemorySegmentBlocking(UNKNOWN_CHANNEL);
}

MemorySegment* LocalMemoryBufferPool::requestMemorySegmentBlocking(int targetChannel)
{
    MemorySegment* segment;
    LOG("requestObjectSegment loop will running");
    LOG_PART(" Back Pressure possible happens, current segment in pool is " << availableSegments.size());
    while (!(segment = requestMemorySegment(targetChannel))) {
        // INFO_RELEASE(" Back Pressure happens, current segment in pool is " << availableSegments.size() << "for
        // channel "<< targetChannel)
        if (cancelled_.load()) {
            throw std::runtime_error("task has been cancelled");
        }
        // workaround sleep for a while
        // INFO_RELEASE("sleep requestMemorySegmentBlocking")
        // INFO_RELEASE("LocalMemoryBufferPool sleep time: " << std::to_string(1))
        //            int sleep_time = getWaitSizeFromEnv();
        //            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
        this->GetAvailableFuture()->get();
        // std::cout << "LocalMemoryBufferPool has requested segement" << std::endl;
    }
    return segment;
}

void LocalMemoryBufferPool::returnExcessSegments()
{
    void returnExcessMemorySegments();
};

void LocalMemoryBufferPool::returnExcessMemorySegments()
{
    while (hasExcessBuffers()) {
        if (availableSegments.empty()) {
            return;
        }

        MemorySegment* segment = reinterpret_cast<MemorySegment*>(availableSegments.front());
        availableSegments.pop_front();
        if (segment == nullptr) {
            return;
        }
        returnMemorySegment(segment);
    }
}

bool LocalMemoryBufferPool::hasExcessBuffers()
{
    return numberOfRequestedMemorySegments > currentPoolSize_;
}

bool LocalMemoryBufferPool::isRequestedSizeReached()
{
    return numberOfRequestedMemorySegments >= currentPoolSize_;
}

bool LocalMemoryBufferPool::requestSegmentFromGlobal()
{
    LOG("requestMemorySegmentFromGlobal in lock");
    if (isRequestedSizeReached()) {
        return false;
    }

    //        if (isDestroyed_) {
    //            throw std::runtime_error(
    //                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");
    //        }

    MemorySegment* segment = requestPooledMemorySegment();
    if (segment != nullptr) {
        availableSegments.push_back(segment);
        LOG_PART(
            "requestPooledMemorySegment from networkMemoryBufferPool_ , numberOfRequestedMemorySegments_  "
            << numberOfRequestedMemorySegments << " currentPoolSize_ :" << currentPoolSize_);
        return true;
    }
    return false;
}

std::string LocalMemoryBufferPool::toString() const
{
    return "[size: " + std::to_string(currentPoolSize_) + ", required: " + std::to_string(numberOfRequiredSegments_) +
           ", requested: " + std::to_string(numberOfRequestedMemorySegments) +
           ", available: " + std::to_string(availableSegments.size()) +
           ", max: " + std::to_string(maxNumberOfSegments) +
           ", listeners: " + std::to_string(registeredListeners_.size()) +
           ", subpartitions: " + std::to_string(subpartitionBuffersCount_.size()) +
           ", maxBuffersPerChannel: " + std::to_string(maxBuffersPerChannel_) +
           ", destroyed: " + (isDestroyed_ ? "true" : "false") + "]";
}

} // namespace omnistream::datastream
