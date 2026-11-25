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

#include "LocalBufferPool.h"

namespace omnistream {

const int LocalBufferPool::UNKNOWN_CHANNEL;

LocalBufferPool::LocalBufferPool(
    std::shared_ptr<NetworkBufferPool> networkBufferPool,
    int numberOfSubpartitions,
    int maxBuffersPerChannel,
    int currentPoolSize,
    int numberOfRequiredSegments,
    int maxNumberOfSegments,
    std::shared_ptr<AvailabilityHelper> availabilityHelper)
    : networkBufferPool(networkBufferPool),
      maxBuffersPerChannel_(maxBuffersPerChannel),
      currentPoolSize_(currentPoolSize),
      numberOfRequiredSegments_(numberOfRequiredSegments),
      maxNumberOfSegments(maxNumberOfSegments),
      availabilityHelper_(availabilityHelper),
      subpartitionBuffersCount_(numberOfSubpartitions, 0)
{
}

void LocalBufferPool::checkConsistentAvailability()
{
    LOG("checkConsistentAvailability in lock");
    bool shouldBeAvailableValue = shouldBeAvailable();
    if (availabilityHelper_->isApproximatelyAvailable() != shouldBeAvailableValue) {
        throw std::runtime_error("Inconsistent availability: expected " + std::to_string(shouldBeAvailableValue));
    }
}

const AvailabilityStatus& LocalBufferPool::checkAvailability()
{
    if (!availableSegments.empty()) {
        return AvailabilityStatus::from(shouldBeAvailable(), false);
    }
    if (isRequestedSizeReached()) {
        return AvailabilityStatus::UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION();
    }
    bool needRequestingNotificationOfGlobalPoolAvailable = false;
    // There aren't availableMemorySegments, and we continue to request new memory segment from
    // global pool.
    if (!requestSegmentFromGlobal()) {
        // If we can not get a buffer from global pool, we should request from it when it
        // becomes available. It should be noted that if we are already in this status, do not
        // need to repeat the request.
        needRequestingNotificationOfGlobalPoolAvailable = !requestingNotificationOfGlobalPoolAvailable;
    }
    return AvailabilityStatus::from(shouldBeAvailable(), needRequestingNotificationOfGlobalPoolAvailable);
}

std::shared_ptr<CompletableFuture> LocalBufferPool::checkAndUpdateAvailability()
{
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    const AvailabilityStatus& availabilityStatus = checkAvailability();
    if (availabilityStatus.isAvailable()) {
        toNotify = availabilityHelper_->getUnavailableToResetAvailable();
    } else {
        availabilityHelper_->resetUnavailable();
    }
    if (availabilityStatus.isNeedRequestingNotificationOfGlobalPoolAvailable()) {
        requestSegmentFromGlobalWhenAvailable();
    }

    checkConsistentAvailability();
    return toNotify;
}

bool LocalBufferPool::shouldBeAvailable()
{
    LOG("shouldBeAvailable in lock");
    return !availableSegments.empty() && unavailableSubpartitionsCount_ == 0;
}

void LocalBufferPool::recycle(Segment* segment)
{
    recycle(segment, UNKNOWN_CHANNEL);
}

void LocalBufferPool::recycle(Segment* segment, int channel)
{
    // INFO_RELEASE("recycle an object segment............. " << segment.get() << " for channel " << channel);
    std::shared_ptr<BufferListener> listener = nullptr;
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    do {
        std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
        if (channel != UNKNOWN_CHANNEL) {
            if (subpartitionBuffersCount_[channel]-- == maxBuffersPerChannel_) {
                unavailableSubpartitionsCount_--;
            }
        }

        if (isDestroyed_ || hasExcessBuffers()) {
            returnSegment(segment);
            return;
        } else {
            if (registeredListeners_.empty()) {
                // INFO_RELEASE(" Listeners is empty for segment to recycle " << segment.get());
                availableSegments.push_back(segment);
                if (!availabilityHelper_->isApproximatelyAvailable() && shouldBeAvailable()) {
                    toNotify = availabilityHelper_->getUnavailableToResetAvailable();
                }
                break;
            } else {
                // while not sure the detail machnism of listener, if it is not empty, ther must be some other consumer
                // using this buffer/segment  so that we can not recycle.
                listener = registeredListeners_.front();
                registeredListeners_.pop_front();
            }
        }

        checkConsistentAvailability();
    } while (!fireBufferAvailableNotification(listener, segment));

    mayNotifyAvailable(toNotify);
}

int LocalBufferPool::getNumberOfRequiredSegments() const
{
    return numberOfRequiredSegments_;
}

void LocalBufferPool::setNumBuffers(int numBuffers)
{
    std::shared_ptr<CompletableFuture> toNotify;
    {
        std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
        if (numBuffers < numberOfRequiredSegments_) {
            throw std::invalid_argument(
                "Buffer pool needs at least " + std::to_string(numberOfRequiredSegments_) +
                " buffers, but tried to set to " + std::to_string(numBuffers));
        }

        currentPoolSize_ = std::min(numBuffers, maxNumberOfSegments);

        returnExcessSegments();

        if (isDestroyed_) {
            return;
        }
        toNotify = checkAndUpdateAvailability();
    }

    mayNotifyAvailable(toNotify);
}

bool LocalBufferPool::addBufferListener(std::shared_ptr<BufferListener> listener)
{
    std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
    if (!availableSegments.empty() || isDestroyed_) {
        return false;
    }

    registeredListeners_.push_back(listener);
    return true;
}

void LocalBufferPool::onGlobalPoolAvailable()
{
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
        requestingNotificationOfGlobalPoolAvailable = false;
        if (isDestroyed_ || availabilityHelper_->isApproximatelyAvailable()) {
            return;
        }

        toNotify = checkAndUpdateAvailability();
    }
    mayNotifyAvailable(toNotify);
}

std::shared_ptr<CompletableFuture> LocalBufferPool::GetAvailableFuture()
{
    return availabilityHelper_->GetAvailableFuture();
}

void LocalBufferPool::requestSegmentFromGlobalWhenAvailable()
{
    if (requestingNotificationOfGlobalPoolAvailable) {
        throw std::runtime_error(
            "local buffer pool is already in the state of requesting memory segment from global "
            "when it is available.");
    }
    requestingNotificationOfGlobalPoolAvailable = true;
    class InnerRunnable : public Runnable {
    public:
        InnerRunnable(LocalBufferPool* self)
        {
            this->self = self;
        }
        ~InnerRunnable() = default;
        void run() override
        {
            self->onGlobalPoolAvailable();
        }

    private:
        LocalBufferPool* self;
    };
    auto runnable = std::make_shared<InnerRunnable>(this);
    networkBufferPool->GetAvailableFuture()->thenRun(runnable);
}

void LocalBufferPool::mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify)
{
    if (toNotify) {
        toNotify->complete();
    }
}

bool LocalBufferPool::fireBufferAvailableNotification(std::shared_ptr<BufferListener> listener, Segment* segment)
{
    // seems useless, because registeredListeners is empty
    // todo: need fix
    return listener->notifyBufferAvailable(nullptr);
}

void LocalBufferPool::cancel()
{
    cancelled_ = true;
    std::shared_ptr<CompletableFuture> toNotify = nullptr;
    {
        std::lock_guard<std::recursive_mutex> lock(availableSegmentsLock);
        toNotify = availabilityHelper_->getUnavailableToResetAvailable();
    }
    mayNotifyAvailable(toNotify);
}

LocalBufferPool::SubpartitionBufferRecycler::SubpartitionBufferRecycler(
    int channel, std::shared_ptr<LocalBufferPool> bufferPool)
    : channel_(channel),
      bufferPool_(bufferPool)
{
}

void LocalBufferPool::SubpartitionBufferRecycler::recycle(Segment* segment)
{
    bufferPool_->recycle(segment, channel_);
}
} // namespace omnistream
