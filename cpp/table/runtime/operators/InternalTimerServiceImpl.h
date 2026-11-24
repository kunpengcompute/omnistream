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

#pragma once

#include "runtime/state/KeyGroupRange.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "InternalTimerService.h"
#include "runtime/state/heap/HeapPriorityQueueSet.h"
#include "streaming/api/operators/Triggerable.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "streaming/api/operators/KeyContext.h"
#include "InternalTimersSnapshot.h"
#include <cstdint>
#include <functional>

template <typename K, typename N>
class AggregateWindowOperator;

template <typename K, typename N>
class InternalTimerServiceImpl : public InternalTimerService<N>, public ProcessingTimeCallback {
public:
    using ProcessingTimeTimersQueueType =
        KeyGroupedInternalPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>>;
    using EventTimeTimersQueueType = KeyGroupedInternalPriorityQueue<std::shared_ptr<TimerHeapInternalTimer<K, N>>>;

    InternalTimerServiceImpl(
        KeyGroupRange* localKeyGroupRange,
        KeyContext<K>* keyContext,
        ProcessingTimeService* processingTimeService,
        std::shared_ptr<ProcessingTimeTimersQueueType> processingTimeTimersQueue,
        std::shared_ptr<EventTimeTimersQueueType> eventTimeTimersQueue);

    ~InternalTimerServiceImpl() override;
    int64_t currentProcessingTime() override;
    int64_t currentWatermark() override;
    void advanceWatermark(int64_t time) override;
    void startTimerService(
        TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer, Triggerable<K, N>* triggerTarget);
    void registerProcessingTimeTimer(N nameSpace, int64_t time);
    void deleteProcessingTimeTimer(N nameSpace, int64_t time);
    void registerEventTimeTimer(N nameSpace, int64_t time);
    void deleteEventTimeTimer(N nameSpace, int64_t time);

    // temp fix for too many timers in priority queue
    // this function should to be deleted when RocksDBCachingPriorityQueueSet is implemented in the future
    void deleteFirstEventTimeTimer();

    InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int32_t keyGroupId);

    void restoreTimersForKeyGroup(const InternalTimersSnapshot<K, N>& timersSnapshot, int32_t keyGroupId);

    TypeSerializer* getKeySerializer()
    {
        return keySerializer;
    }

    TypeSerializer* getNamespaceSerializer()
    {
        return namespaceSerializer;
    }

private:
    KeyContext<K>* keyContext = nullptr;
    ProcessingTimeService* processingTimeService = nullptr;
    KeyGroupRange* localKeyGroupRange = nullptr;
    std::shared_ptr<ProcessingTimeTimersQueueType> processingTimeTimersQueue;
    std::shared_ptr<EventTimeTimersQueueType> eventTimeTimersQueue;

    InternalTimersSnapshot<K, N> restoredTimersSnapshot;
    bool hasRestoredTimersSnapshot = false;
    int32_t localKeyGroupRangeStartIndex{};
    int64_t currentWatermarkValue = INT64_MIN;

    Triggerable<K, N>* triggerTarget = nullptr;
    TypeSerializer* keySerializer = nullptr;
    TypeSerializer* namespaceSerializer = nullptr;
    bool isInitialized{};
    void OnProcessingTime(int64_t time) override;
};

template <typename K, typename N>
InternalTimerServiceImpl<K, N>::InternalTimerServiceImpl(
    KeyGroupRange* localKeyGroupRange,
    KeyContext<K>* keyContext,
    ProcessingTimeService* processingTimeService,
    std::shared_ptr<ProcessingTimeTimersQueueType> processingTimeTimersQueue,
    std::shared_ptr<EventTimeTimersQueueType> eventTimeTimersQueue)
    : keyContext(keyContext),
      processingTimeService(processingTimeService),
      localKeyGroupRange(localKeyGroupRange),
      processingTimeTimersQueue(processingTimeTimersQueue),
      eventTimeTimersQueue(eventTimeTimersQueue),
      isInitialized(false)
{
    this->localKeyGroupRangeStartIndex =
        localKeyGroupRange->getEndKeyGroup() == -1 ? -1 : localKeyGroupRange->getStartKeyGroup();
    this->keySerializer = nullptr;
    this->namespaceSerializer = nullptr;
    this->triggerTarget = nullptr;
}

template <typename K, typename N>
InternalTimerServiceImpl<K, N>::~InternalTimerServiceImpl()
{
    // TODO: Serializer::INSTANCE cannot be deleted here
    // if (namespaceSerializer != nullptr) {
    //     delete namespaceSerializer;
    //     namespaceSerializer = nullptr;
    // }
}

template <typename K, typename N>
inline int64_t InternalTimerServiceImpl<K, N>::currentProcessingTime()
{
    return processingTimeService->getCurrentProcessingTime();
}

template <typename K, typename N>
int64_t InternalTimerServiceImpl<K, N>::currentWatermark()
{
    return currentWatermarkValue;
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::advanceWatermark(int64_t time)
{
    currentWatermarkValue = time;

    auto timer = eventTimeTimersQueue->peek();

    while (!eventTimeTimersQueue->isEmpty() && timer->getTimestamp() <= time) {
        eventTimeTimersQueue->poll();
        keyContext->setCurrentKey(timer->getKey());
        triggerTarget->onEventTime(timer.get());
        timer = eventTimeTimersQueue->peek();
    }
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::startTimerService(
    TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer, Triggerable<K, N>* triggerTarget)
{
    if (!isInitialized) {
        if (keySerializer == nullptr || namespaceSerializer == nullptr) {
            THROW_LOGIC_EXCEPTION("The TimersService serializers cannot be null.");
        }

        if (this->keySerializer != nullptr || this->namespaceSerializer != nullptr || this->triggerTarget != nullptr) {
            THROW_LOGIC_EXCEPTION("The TimersService has already been initialized.");
        }

        if (hasRestoredTimersSnapshot) {
            auto restoredKeySerializer = restoredTimersSnapshot.getKeySerializer();
            auto restoredNamespaceSerializer = restoredTimersSnapshot.getNamespaceSerializer();
            if (restoredKeySerializer != nullptr &&
                restoredKeySerializer->getBackendId() != BackendDataType::INVALID_BK &&
                restoredKeySerializer->getBackendId() != keySerializer->getBackendId()) {
                INFO_RELEASE(
                    "Error: startTimerService Restored timer key serializer is incompatible with requested "
                    "timer service.");
                THROW_LOGIC_EXCEPTION("Restored timer key serializer is incompatible with requested timer service.");
            }
            if (restoredNamespaceSerializer != nullptr &&
                restoredNamespaceSerializer->getBackendId() != BackendDataType::INVALID_BK &&
                restoredNamespaceSerializer->getBackendId() != namespaceSerializer->getBackendId()) {
                INFO_RELEASE(
                    "Error: startTimerService Restored timer namespace serializer is incompatible with "
                    "requested timer service");
                THROW_LOGIC_EXCEPTION(
                    "Restored timer namespace serializer is incompatible with requested timer service.");
            }
        }

        this->keySerializer = keySerializer;
        this->namespaceSerializer = namespaceSerializer;
        this->triggerTarget = triggerTarget;

        auto headTimer = processingTimeTimersQueue->peek();
        if (headTimer != nullptr) {
            processingTimeService->registerTimer(headTimer->getTimestamp(), this);
        }

        this->isInitialized = true;
    }
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::registerProcessingTimeTimer(N nameSpace, int64_t time)
{
    auto oldHead = processingTimeTimersQueue->peek();
    bool newHead = processingTimeTimersQueue->add(
        std::make_shared<TimerHeapInternalTimer<K, N>>(time, keyContext->getCurrentKey(), nameSpace));
    if (newHead) {
        int64_t nextTriggerTime = oldHead != nullptr ? oldHead->getTimestamp() : INT64_MAX;
        if (time < nextTriggerTime) {
            processingTimeService->registerTimer(time, this);
        }
    }
}

// todo ScheduledFuture should be accomplished
template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::OnProcessingTime(int64_t time)
{
    auto timer = processingTimeTimersQueue->peek();
    while (timer != nullptr && timer->getTimestamp() <= time) {
        keyContext->setCurrentKey(timer->getKey());
        processingTimeTimersQueue->poll();
        triggerTarget->onProcessingTime(timer.get());
        timer = processingTimeTimersQueue->peek();
    }
    if (timer != nullptr) {
        processingTimeService->registerTimer(timer->getTimestamp(), this);
    }
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::deleteProcessingTimeTimer(N nameSpace, int64_t time)
{
    auto toRemove = std::make_shared<TimerHeapInternalTimer<K, N>>(time, keyContext->getCurrentKey(), nameSpace);
    processingTimeTimersQueue->remove(toRemove);
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::registerEventTimeTimer(N nameSpace, int64_t time)
{
    eventTimeTimersQueue->add(
        std::make_shared<TimerHeapInternalTimer<K, N>>(time, keyContext->getCurrentKey(), nameSpace));
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::deleteEventTimeTimer(N nameSpace, int64_t time)
{
    auto toRemove = std::make_shared<TimerHeapInternalTimer<K, N>>(time, keyContext->getCurrentKey(), nameSpace);
    eventTimeTimersQueue->remove(toRemove);
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::deleteFirstEventTimeTimer()
{
    eventTimeTimersQueue->poll();
}
template <typename K, typename N>
InternalTimersSnapshot<K, N> InternalTimerServiceImpl<K, N>::snapshotTimersForKeyGroup(int32_t keyGroupId)
{
    InternalTimersSnapshot<K, N> snapshot;
    snapshot.setKeySerializer(keySerializer);
    snapshot.setNamespaceSerializer(namespaceSerializer);

    auto eventSubset = eventTimeTimersQueue->getSubsetForKeyGroup(keyGroupId);
    if (eventSubset != nullptr) {
        for (const auto& timer : *eventSubset) {
            snapshot.addEventTimeTimer(timer);
        }
    }

    auto processingSubset = processingTimeTimersQueue->getSubsetForKeyGroup(keyGroupId);
    if (processingSubset != nullptr) {
        for (const auto& timer : *processingSubset) {
            snapshot.addProcessingTimeTimer(timer);
        }
    }

    return snapshot;
}

template <typename K, typename N>
void InternalTimerServiceImpl<K, N>::restoreTimersForKeyGroup(
    const InternalTimersSnapshot<K, N>& timersSnapshot, int32_t keyGroupId)
{
    if (localKeyGroupRange != nullptr && !localKeyGroupRange->contains(keyGroupId)) {
        INFO_RELEASE(
            "Error: restoreTimersForKeyGroup Timer key-group " << keyGroupId << " is outside local key-group range "
                                                               << localKeyGroupRange->ToString());
        THROW_LOGIC_EXCEPTION(
            "Timer key-group " << keyGroupId << " is outside local key-group range " << localKeyGroupRange->ToString());
    }

    restoredTimersSnapshot = timersSnapshot;
    hasRestoredTimersSnapshot = true;

    for (const auto& timer : timersSnapshot.getEventTimeTimers()) {
        eventTimeTimersQueue->add(timer);
    }

    for (const auto& timer : timersSnapshot.getProcessingTimeTimers()) {
        processingTimeTimersQueue->add(timer);
    }
}
