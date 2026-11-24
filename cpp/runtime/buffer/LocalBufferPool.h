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

#ifndef LOCALBUFFERPOOL_H
#define LOCALBUFFERPOOL_H
#include <deque>

#include "BufferPool.h"
#include "io/AvailabilityHelper.h"
#include "AvailabilityStatus.h"
#include "NetworkBufferPool.h"

namespace omnistream {

class LocalBufferPool : public BufferPool, public std::enable_shared_from_this<LocalBufferPool> {
public:
    LocalBufferPool(
        std::shared_ptr<NetworkBufferPool> networkBufferPool,
        int numberOfSubpartitions,
        int maxBuffersPerChannel,
        int currentPoolSize,
        int numberOfRequiredSegments,
        int maxNumberOfSegments,
        std::shared_ptr<AvailabilityHelper> availabilityHelper);
    ~LocalBufferPool()
    {
        for (auto segment : availableSegments) {
            delete segment;
        }
        availableSegments.clear();
    }

    virtual bool isRequestedSizeReached() = 0;

    void checkConsistentAvailability();
    std::shared_ptr<CompletableFuture> checkAndUpdateAvailability();
    // void lazyDestroy() override;

    // std::shared_ptr<Segment> requestSegment() override;
    // std::shared_ptr<Segment> requestSegmentBlocking() override;

    virtual bool requestSegmentFromGlobal() = 0;
    void requestSegmentFromGlobalWhenAvailable();
    void onGlobalPoolAvailable();

    int getNumberOfRequiredSegments() const override;

    // std::shared_ptr<Segment> requestSegmentBlocking(int targetChannel) override = 0;
    bool addBufferListener(std::shared_ptr<BufferListener> listener) override;
    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;

    void recycle(Segment* segment) override;
    virtual bool hasExcessBuffers() = 0;

    virtual void returnSegment(Segment* segment) = 0;

    // virtual std::shared_ptr<ObjectBuffer> toObjectBuffer(std::shared_ptr<ObjectSegment> segment) = 0;
    // virtual std::shared_ptr<ObjectBufferBuilder> toObjectBufferBuilder(std::shared_ptr<ObjectSegment> segment, int
    // targetChannel) = 0;
    void mayNotifyAvailable(std::shared_ptr<CompletableFuture> toNotify);
    bool fireBufferAvailableNotification(std::shared_ptr<BufferListener> listener, Segment* segment);

    void recycle(Segment* segment, int channel);
    virtual void returnExcessSegments() = 0;
    void setNumBuffers(int numBuffers) override;
    void cancel() override;

protected:
    class SubpartitionBufferRecycler : public BufferRecycler {
    public:
        SubpartitionBufferRecycler(int channel, std::shared_ptr<LocalBufferPool> bufferPool);

        void recycle(Segment* segment) override;

        std::string toString() const override
        {
            return "MemoryBufferRecycler";
        };

    protected:
        int channel_;
        std::shared_ptr<LocalBufferPool> bufferPool_;
    };

    static const int UNKNOWN_CHANNEL = -1;
    std::recursive_mutex memoryMutex;
    std::recursive_mutex availableSegmentsLock;
    std::deque<std::shared_ptr<BufferListener>> registeredListeners_;
    std::deque<Segment*> availableSegments;
    std::shared_ptr<NetworkBufferPool> networkBufferPool;

    int maxBuffersPerChannel_;
    int currentPoolSize_;
    int numberOfRequiredSegments_;
    int maxNumberOfSegments;
    std::shared_ptr<AvailabilityHelper> availabilityHelper_;
    std::vector<int> subpartitionBuffersCount_;

    bool isDestroyed_ = false;
    int unavailableSubpartitionsCount_ = 0;
    // std::shared_ptr<NetworkObjectBufferPool> networkObjBufferPool_;
    bool shouldBeAvailable();
    const AvailabilityStatus& checkAvailability();
    // used for job cancellation to avoid back pressure
    std::atomic<bool> cancelled_{false};

    bool requestingNotificationOfGlobalPoolAvailable = false;
};
} // namespace omnistream

#endif // LOCALBUFFERPOOL_H
