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

#include "streaming/api/watermark/Watermark.h"
#include "InternalTimerService.h"
#include "core/typeutils/TypeSerializer.h"
#include "InternalTimerServiceImpl.h"
#include "streaming/api/operators/KeyContext.h"
#include "TimerSerializer.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "runtime/state/KeyedStateCheckpointOutputStream.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/TimerConsistencyCheckControl.h"
#include "runtime/state/restore/RawKeyedStateInputStreamProxy.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "InternalTimerServiceSerializationProxy.h"
#include "InternalTimersSnapshotReaderWriters.h"
#include "core/utils/type_traits_ext.h"
#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <utility>

template <typename K>
class InternalTimeServiceManager {
public:
    inline static std::string TIMER_STATE_PREFIX = "_timer_state";
    inline static std::string PROCESSING_TIMER_PREFIX = TIMER_STATE_PREFIX + "/processing_";
    inline static std::string EVENT_TIMER_PREFIX = TIMER_STATE_PREFIX + "/event_";

    InternalTimeServiceManager(
        KeyGroupRange* localKeyGroupRange,
        KeyContext<K>* keyContext,
        PriorityQueueSetFactory* priorityQueueSetFactory,
        ProcessingTimeService* processingTimeService,
        int maxNumberOfSubtasks,
        const std::vector<std::shared_ptr<KeyedStateHandle>>& rawKeyedStateHandles = {},
        std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge = nullptr)
        : localKeyGroupRange(localKeyGroupRange),
          keyContext(keyContext),
          priorityQueueSetFactory(priorityQueueSetFactory),
          processingTimeService(processingTimeService),
          maxNumberOfSubtasks(maxNumberOfSubtasks),
          omniTaskBridge(std::move(omniTaskBridge))
    {
        if (auto keyedBackend = dynamic_cast<AbstractKeyedStateBackend<K>*>(priorityQueueSetFactory)) {
            keySerializerForRestore = keyedBackend->getKeySerializer();
        }
        restoreRawKeyedState(rawKeyedStateHandles);
    }

    ~InternalTimeServiceManager()
    {
        for (const auto& it : timerServicesInt64) {
            delete it.second;
        }
        timerServicesInt64.clear();

        for (const auto& it : timerServicesWindow) {
            delete it.second;
        }
        timerServicesWindow.clear();

        for (const auto& it : timerServicesVoidNameSpace) {
            delete it.second;
        }
        timerServicesVoidNameSpace.clear();
    }

    template <typename N>
    InternalTimerServiceImpl<K, N>* getInternalTimerService(
        std::string name,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer,
        Triggerable<K, N>* triggerable);

    void advanceWatermark(Watermark* watermark);

    void snapshotToRawKeyedState(
        KeyedStateCheckpointOutputStream* stateCheckpointOutputStream,
        std::string operatorName,
        int64_t checkpointId = -1);

    int32_t getRegisteredTimerServiceCount() const;

    void writeTimersForKeyGroup(KeyedStateCheckpointOutputStream* out, int32_t keyGroupIdx);

    void readTimersForKeyGroup(RawKeyedStateInputStreamProxy* in, int32_t keyGroupIdx, int32_t version);

private:
    enum class TimerNamespaceKind {
        INT64,
        TIME_WINDOW,
        VOID_NAMESPACE
    };
    struct TimerDataSample {
        bool present = false;
        bool eventTimer = false;
        int32_t keyGroupId = -1;
        int64_t timestamp = -1;
        uint64_t sampleRankHash = std::numeric_limits<uint64_t>::max();
        uint64_t timerHash = 0;
        uint64_t keyHash = 0;
        uint64_t namespaceHash = 0;
        int32_t keyBytesLength = 0;
        int32_t namespaceBytesLength = 0;
        std::string keyPreviewHex;
        std::string namespacePreviewHex;
    };

    struct TimerDataDigest {
        int32_t keyGroupCount = 0;
        int32_t nonEmptyKeyGroupCount = 0;
        int64_t eventTimerCount = 0;
        int64_t processingTimerCount = 0;
        uint64_t eventXorHash = 0;
        uint64_t eventSumHash = 0;
        uint64_t processingXorHash = 0;
        uint64_t processingSumHash = 0;
        uint64_t keyGroupXorHash = 0;
        uint64_t keyGroupSumHash = 0;
        int64_t eventMinTimestamp = std::numeric_limits<int64_t>::max();
        int64_t eventMaxTimestamp = std::numeric_limits<int64_t>::min();
        int64_t processingMinTimestamp = std::numeric_limits<int64_t>::max();
        int64_t processingMaxTimestamp = std::numeric_limits<int64_t>::min();
        TimerDataSample sample;

        int64_t totalTimerCount() const
        {
            return eventTimerCount + processingTimerCount;
        }

        void addTimer(
            bool eventTimer,
            int64_t timestamp,
            uint64_t timerHash,
            uint64_t keyHash,
            uint64_t namespaceHash,
            int32_t keyBytesLength,
            int32_t namespaceBytesLength,
            std::string keyPreviewHex,
            std::string namespacePreviewHex)
        {
            if (eventTimer) {
                ++eventTimerCount;
                eventXorHash ^= mixHash(timerHash);
                eventSumHash += timerHash;
                eventMinTimestamp = std::min(eventMinTimestamp, timestamp);
                eventMaxTimestamp = std::max(eventMaxTimestamp, timestamp);
            } else {
                ++processingTimerCount;
                processingXorHash ^= mixHash(timerHash);
                processingSumHash += timerHash;
                processingMinTimestamp = std::min(processingMinTimestamp, timestamp);
                processingMaxTimestamp = std::max(processingMaxTimestamp, timestamp);
            }
            TimerDataSample candidate;
            candidate.present = true;
            candidate.eventTimer = eventTimer;
            candidate.timestamp = timestamp;
            candidate.sampleRankHash = mixHash(timerHash);
            candidate.timerHash = timerHash;
            candidate.keyHash = keyHash;
            candidate.namespaceHash = namespaceHash;
            candidate.keyBytesLength = keyBytesLength;
            candidate.namespaceBytesLength = namespaceBytesLength;
            candidate.keyPreviewHex = std::move(keyPreviewHex);
            candidate.namespacePreviewHex = std::move(namespacePreviewHex);
            considerSample(std::move(candidate));
        }

        void mergeKeyGroupDigest(int32_t keyGroupId, const TimerDataDigest& keyGroupDigest)
        {
            ++keyGroupCount;
            if (keyGroupDigest.totalTimerCount() > 0) {
                ++nonEmptyKeyGroupCount;
            }
            uint64_t keyGroupHash = hashKeyGroupDigest(keyGroupId, keyGroupDigest);
            keyGroupXorHash ^= mixHash(keyGroupHash);
            keyGroupSumHash += keyGroupHash;
            eventTimerCount += keyGroupDigest.eventTimerCount;
            processingTimerCount += keyGroupDigest.processingTimerCount;
            eventXorHash ^= keyGroupDigest.eventXorHash;
            eventSumHash += keyGroupDigest.eventSumHash;
            processingXorHash ^= keyGroupDigest.processingXorHash;
            processingSumHash += keyGroupDigest.processingSumHash;
            if (keyGroupDigest.eventTimerCount > 0) {
                eventMinTimestamp = std::min(eventMinTimestamp, keyGroupDigest.eventMinTimestamp);
                eventMaxTimestamp = std::max(eventMaxTimestamp, keyGroupDigest.eventMaxTimestamp);
            }
            if (keyGroupDigest.processingTimerCount > 0) {
                processingMinTimestamp = std::min(processingMinTimestamp, keyGroupDigest.processingMinTimestamp);
                processingMaxTimestamp = std::max(processingMaxTimestamp, keyGroupDigest.processingMaxTimestamp);
            }
            if (keyGroupDigest.sample.present) {
                TimerDataSample candidate = keyGroupDigest.sample;
                candidate.keyGroupId = keyGroupId;
                considerSample(std::move(candidate));
            }
        }

    private:
        void considerSample(TimerDataSample candidate)
        {
            if (!candidate.present) {
                return;
            }
            if (!sample.present || candidate.sampleRankHash < sample.sampleRankHash) {
                sample = std::move(candidate);
            }
        }

        static uint64_t mixHash(uint64_t value)
        {
            value ^= value >> 33U;
            value *= 0xff51afd7ed558ccdULL;
            value ^= value >> 33U;
            value *= 0xc4ceb9fe1a85ec53ULL;
            value ^= value >> 33U;
            return value;
        }

        static uint64_t hashKeyGroupDigest(int32_t keyGroupId, const TimerDataDigest& digest)
        {
            uint64_t value = mixHash(static_cast<uint64_t>(static_cast<uint32_t>(keyGroupId)));
            value ^= mixHash(static_cast<uint64_t>(digest.eventTimerCount));
            value ^= mixHash(static_cast<uint64_t>(digest.processingTimerCount));
            value ^= mixHash(digest.eventXorHash);
            value ^= mixHash(digest.eventSumHash);
            value ^= mixHash(digest.processingXorHash);
            value ^= mixHash(digest.processingSumHash);
            return value;
        }
    };
    emhash7::HashMap<std::string, InternalTimerService<int64_t>*> timerServicesInt64;
    emhash7::HashMap<std::string, InternalTimerService<TimeWindow>*> timerServicesWindow;
    emhash7::HashMap<std::string, InternalTimerService<VoidNamespace>*> timerServicesVoidNameSpace;
    KeyGroupRange* localKeyGroupRange;
    KeyContext<K>* keyContext;
    PriorityQueueSetFactory* priorityQueueSetFactory;
    ProcessingTimeService* processingTimeService;
    int maxNumberOfSubtasks;
    std::shared_ptr<omnistream::OmniTaskBridge> omniTaskBridge;
    TypeSerializer* keySerializerForRestore = nullptr;
    int64_t activeSnapshotCheckpointId = -1;
    int64_t snapshotEventTimerCount = 0;
    int64_t snapshotProcessingTimerCount = 0;
    int32_t snapshotWrittenKeyGroupCount = 0;
    int32_t snapshotNonEmptyKeyGroupCount = 0;
    int32_t snapshotNonEmptyServiceKeyGroupCount = 0;
    int64_t restoreEventTimerCount = 0;
    int64_t restoreProcessingTimerCount = 0;
    int32_t restoreReadKeyGroupCount = 0;
    int32_t restoreNonEmptyKeyGroupCount = 0;
    int32_t restoreNonEmptyServiceKeyGroupCount = 0;
    int32_t restoreSkippedHandleCount = 0;
    int32_t activeSnapshotKeyGroupId = -1;
    std::unordered_map<std::string, TimerDataDigest> snapshotTimerDataDigests;
    std::unordered_map<std::string, TimerDataDigest> restoreTimerDataDigests;
    static constexpr int MIN_CAPACITY_OF_TIMER_QUEUE = 128;

    template <typename N>
    InternalTimerServiceImpl<K, N>* registerOrGetTimerService(std::string name, TimerSerializer<K, N>* timerSerializer);

    template <typename N>
    InternalTimerServiceImpl<K, N>* registerOrGetTimerServiceForRestore(
        const std::string& name, TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer);

    template <typename N>
    void writeTimerServiceMap(
        KeyedStateCheckpointOutputStream* out,
        emhash7::HashMap<std::string, InternalTimerService<N>*>& services,
        int32_t keyGroupIdx);

    void restoreRawKeyedState(const std::vector<std::shared_ptr<KeyedStateHandle>>& rawKeyedStateHandles);

    void restoreRawKeyGroupState(const std::shared_ptr<KeyGroupsStateHandle>& keyGroupsStateHandle);

    void restoreStateForKeyGroup(RawKeyedStateInputStreamProxy* in, int32_t keyGroupIdx);

    TimerNamespaceKind inferNamespaceKind(
        const FlinkTimerSerializerSnapshots::SnapshotDescriptor& namespaceSerializerSnapshot);

    template <typename N>
    void readTimerServiceForNamespace(
        RawKeyedStateInputStreamProxy* in,
        const std::string& serviceName,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot,
        FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot,
        int32_t keyGroupIdx,
        int32_t version);

    template <typename N>
    TimerDataDigest buildTimerDataDigest(const InternalTimersSnapshot<K, N>& timersSnapshot);

    template <typename N>
    void addTimersToDigest(
        TimerDataDigest& digest,
        const std::vector<typename InternalTimersSnapshot<K, N>::TimerPtr>& timers,
        TypeSerializer* keySerializer,
        TypeSerializer* namespaceSerializer,
        bool eventTimer);

    template <typename T>
    static std::vector<uint8_t> serializeTimerValueForDigest(TypeSerializer* serializer, T value);

    static uint64_t hashTimerBytes(
        bool eventTimer,
        int64_t timestamp,
        const std::vector<uint8_t>& keyBytes,
        const std::vector<uint8_t>& namespaceBytes);

    static uint64_t hashBytesForDigestLog(const std::vector<uint8_t>& bytes);

    static std::string bytesPreviewHex(const std::vector<uint8_t>& bytes, size_t maxBytes);

    static uint64_t fnv1aUpdate(uint64_t hash, const uint8_t* data, size_t length);

    static uint64_t fnv1aUpdateInt64(uint64_t hash, int64_t value);

    static uint64_t fnv1aUpdateInt32(uint64_t hash, int32_t value);

    static std::string namespaceKindName(TimerNamespaceKind namespaceKind);

    static std::string digestMapKey(const std::string& serviceName, TimerNamespaceKind namespaceKind);

    static int64_t printableMinTimestamp(int64_t count, int64_t value);

    static int64_t printableMaxTimestamp(int64_t count, int64_t value);

    void mergeTimerDataDigest(
        std::unordered_map<std::string, TimerDataDigest>& target,
        const std::string& serviceName,
        TimerNamespaceKind namespaceKind,
        int32_t keyGroupIdx,
        const TimerDataDigest& keyGroupDigest);

    void logTimerDataDigest(
        const char* phase,
        const std::string& operatorName,
        int64_t checkpointId,
        const std::string& serviceName,
        TimerNamespaceKind namespaceKind,
        const TimerDataDigest& digest);

    void logTimerDataDigests(
        const char* phase,
        const std::string& operatorName,
        int64_t checkpointId,
        const std::unordered_map<std::string, TimerDataDigest>& digests);

    void logTimerDataSample(
        const char* phase,
        const std::string& operatorName,
        int64_t checkpointId,
        const std::string& serviceName,
        const std::string& namespaceKind,
        const TimerDataDigest& digest);
};

template <typename K>
void InternalTimeServiceManager<K>::advanceWatermark(Watermark* watermark)
{
    for (auto it = timerServicesWindow.begin(); it != timerServicesWindow.end(); it++) {
        it->second->advanceWatermark(watermark->getTimestamp());
    }
    for (auto it = timerServicesVoidNameSpace.begin(); it != timerServicesVoidNameSpace.end(); it++) {
        it->second->advanceWatermark(watermark->getTimestamp());
    }
    for (auto it = timerServicesInt64.begin(); it != timerServicesInt64.end(); it++) {
        it->second->advanceWatermark(watermark->getTimestamp());
    }
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N>* InternalTimeServiceManager<K>::getInternalTimerService(
    std::string name,
    TypeSerializer* keySerializer,
    TypeSerializer* namespaceSerializer,
    Triggerable<K, N>* triggerable)
{
    // TODO: delete？
    auto timerSerializer = new TimerSerializer<K, N>(keySerializer, namespaceSerializer);
    InternalTimerServiceImpl<K, N>* timerService = this->template registerOrGetTimerService<N>(name, timerSerializer);
    timerService->startTimerService(keySerializer, namespaceSerializer, triggerable);
    return timerService;
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N>* InternalTimeServiceManager<K>::registerOrGetTimerService(
    std::string name, TimerSerializer<K, N>* timerSerializer)
{
    auto& timerServicesMap = [&]() -> auto& {
        if constexpr (std::is_same_v<N, int64_t>) {
            return timerServicesInt64;
        } else if constexpr (std::is_same_v<N, VoidNamespace>) {
            return timerServicesVoidNameSpace;
        } else if constexpr (std::is_same_v<N, TimeWindow>) {
            return timerServicesWindow;
        } else {
            THROW_LOGIC_EXCEPTION("Unsupported namespace type, timerService name: " << name);
        }
    }();

    InternalTimerServiceImpl<K, N>* timerService;
    using TimerType = TimerHeapInternalTimer<K, N>;
    using TimerComparator = typename TimerType::MinHeapComparator;

    auto it = timerServicesMap.find(name);
    if (it == timerServicesMap.end()) {
        if (auto factory = dynamic_cast<HeapKeyedStateBackend<K>*>(priorityQueueSetFactory)) {
            auto processingTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                PROCESSING_TIMER_PREFIX + name, timerSerializer);
            auto eventTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                EVENT_TIMER_PREFIX + name, timerSerializer);

            timerService = new InternalTimerServiceImpl<K, N>(
                localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesMap.emplace(name, timerService);
            INFO_RELEASE(
                "TIMER_CP_QUEUE_CREATE backend=HEAP_KEYED"
                << ", managerPtr=" << reinterpret_cast<uintptr_t>(this)
                << ", timerServicePtr=" << reinterpret_cast<uintptr_t>(timerService) << ", timerServiceName=" << name
                << ", processingStateName=" << PROCESSING_TIMER_PREFIX + name
                << ", eventStateName=" << EVENT_TIMER_PREFIX + name
                << ", registeredTimerServiceCountNow=" << getRegisteredTimerServiceCount());
        } else if (auto factory = dynamic_cast<RocksdbKeyedStateBackend<K>*>(priorityQueueSetFactory)) {
            auto processingTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                PROCESSING_TIMER_PREFIX + name, timerSerializer);
            auto eventTimerQueue = factory->template create<std::shared_ptr<TimerType>, TimerComparator>(
                EVENT_TIMER_PREFIX + name, timerSerializer);

            timerService = new InternalTimerServiceImpl<K, N>(
                localKeyGroupRange, keyContext, processingTimeService, processingTimerQueue, eventTimerQueue);
            timerServicesMap.emplace(name, timerService);
            INFO_RELEASE(
                "TIMER_CP_QUEUE_CREATE backend=ROCKSDB_KEYED"
                << ", managerPtr=" << reinterpret_cast<uintptr_t>(this)
                << ", timerServicePtr=" << reinterpret_cast<uintptr_t>(timerService) << ", timerServiceName=" << name
                << ", processingStateName=" << PROCESSING_TIMER_PREFIX + name
                << ", eventStateName=" << EVENT_TIMER_PREFIX + name
                << ", registeredTimerServiceCountNow=" << getRegisteredTimerServiceCount());
        } else {
            THROW_LOGIC_EXCEPTION("Unsupported priorityQueueSetFactory");
        }
    } else {
        timerService = static_cast<InternalTimerServiceImpl<K, N>*>(it->second);
    }
    return timerService;
}

template <typename K>
template <typename N>
InternalTimerServiceImpl<K, N>* InternalTimeServiceManager<K>::registerOrGetTimerServiceForRestore(
    const std::string& name, TypeSerializer* keySerializer, TypeSerializer* namespaceSerializer)
{
    auto timerSerializer = new TimerSerializer<K, N>(keySerializer, namespaceSerializer);
    return this->template registerOrGetTimerService<N>(name, timerSerializer);
}

template <typename K>
void InternalTimeServiceManager<K>::restoreRawKeyedState(
    const std::vector<std::shared_ptr<KeyedStateHandle>>& rawKeyedStateHandles)
{
    restoreEventTimerCount = 0;
    restoreProcessingTimerCount = 0;
    restoreReadKeyGroupCount = 0;
    restoreNonEmptyKeyGroupCount = 0;
    restoreNonEmptyServiceKeyGroupCount = 0;
    restoreSkippedHandleCount = 0;
    restoreTimerDataDigests.clear();

    if (rawKeyedStateHandles.empty()) {
        INFO_RELEASE(
            "TIMER_CP_RESTORE_SUMMARY rawHandleCount=0" << ", managerPtr=" << reinterpret_cast<uintptr_t>(this)
                                                        << ", skippedHandleCount=0"
                                                        << ", restoredKeyGroupCount=0"
                                                        << ", nonEmptyKeyGroupCount=0"
                                                        << ", nonEmptyServiceKeyGroupCount=0"
                                                        << ", eventTimerCount=0"
                                                        << ", processingTimerCount=0"
                                                        << ", totalTimerCount=0"
                                                        << ", localKeyGroupRange=" << localKeyGroupRange->ToString());
        return;
    }

    for (const auto& handle : rawKeyedStateHandles) {
        if (handle == nullptr) {
            ++restoreSkippedHandleCount;
            continue;
        }

        auto intersection = handle->GetIntersection(*localKeyGroupRange);
        if (intersection == nullptr) {
            ++restoreSkippedHandleCount;
            continue;
        }

        auto keyGroupsStateHandle = std::dynamic_pointer_cast<KeyGroupsStateHandle>(intersection);
        if (keyGroupsStateHandle == nullptr) {
            INFO_RELEASE("Error: restoreRawKeyedState Raw keyed timer restore only supports KeyGroupsStateHandle.");
            THROW_LOGIC_EXCEPTION("Raw keyed timer restore only supports KeyGroupsStateHandle.");
        }

        restoreRawKeyGroupState(keyGroupsStateHandle);
    }

    INFO_RELEASE(
        "TIMER_CP_RESTORE_SUMMARY rawHandleCount="
        << rawKeyedStateHandles.size() << ", managerPtr=" << reinterpret_cast<uintptr_t>(this)
        << ", skippedHandleCount=" << restoreSkippedHandleCount << ", restoredKeyGroupCount="
        << restoreReadKeyGroupCount << ", nonEmptyKeyGroupCount=" << restoreNonEmptyKeyGroupCount
        << ", nonEmptyServiceKeyGroupCount=" << restoreNonEmptyServiceKeyGroupCount
        << ", eventTimerCount=" << restoreEventTimerCount << ", processingTimerCount=" << restoreProcessingTimerCount
        << ", totalTimerCount=" << (restoreEventTimerCount + restoreProcessingTimerCount)
        << ", localKeyGroupRange=" << localKeyGroupRange->ToString());
    if (TimerConsistencyCheckControl::timerConsistencyCheckEnabled) {
        logTimerDataDigests("restore", "", -1, restoreTimerDataDigests);
    }
}

template <typename K>
void InternalTimeServiceManager<K>::restoreRawKeyGroupState(
    const std::shared_ptr<KeyGroupsStateHandle>& keyGroupsStateHandle)
{
    KeyGroupRange keyGroupRange = keyGroupsStateHandle->GetKeyGroupRange();
    RawKeyedStateInputStreamProxy input(omniTaskBridge, keyGroupsStateHandle);

    for (int32_t keyGroupIdx = keyGroupRange.getStartKeyGroup(); keyGroupIdx <= keyGroupRange.getEndKeyGroup();
         ++keyGroupIdx) {
        int64_t offset = keyGroupsStateHandle->getOffsetForKeyGroup(keyGroupIdx);
        if (offset < 0) {
            continue;
        }

        input.seek(offset);
        restoreStateForKeyGroup(&input, keyGroupIdx);
    }
}

template <typename K>
void InternalTimeServiceManager<K>::restoreStateForKeyGroup(RawKeyedStateInputStreamProxy* in, int32_t keyGroupIdx)
{
    InternalTimerServiceSerializationProxy<K> proxy(this, keyGroupIdx);
    proxy.read(in);
}

template <typename K>
typename InternalTimeServiceManager<K>::TimerNamespaceKind InternalTimeServiceManager<K>::inferNamespaceKind(
    const FlinkTimerSerializerSnapshots::SnapshotDescriptor& namespaceSerializerSnapshot)
{
    const std::string& className = namespaceSerializerSnapshot.className;
    if (className == FlinkTimerSerializerSnapshots::VOID_NAMESPACE_SERIALIZER_SNAPSHOT) {
        return TimerNamespaceKind::VOID_NAMESPACE;
    }
    if (className == FlinkTimerSerializerSnapshots::TIME_WINDOW_SERIALIZER_SNAPSHOT) {
        return TimerNamespaceKind::TIME_WINDOW;
    }
    if (className == FlinkTimerSerializerSnapshots::LONG_SERIALIZER_SNAPSHOT) {
        return TimerNamespaceKind::INT64;
    }

    INFO_RELEASE("Error: inferNamespaceKind Unsupported timer namespace serializer snapshot class: " << className);
    THROW_LOGIC_EXCEPTION("Unsupported timer namespace serializer snapshot class: " << className);
}

template <typename K>
void InternalTimeServiceManager<K>::readTimersForKeyGroup(
    RawKeyedStateInputStreamProxy* in, int32_t keyGroupIdx, int32_t version)
{
    int32_t serviceCount = in->readInt();
    ++restoreReadKeyGroupCount;
    int64_t keyGroupEventTimerCountBefore = restoreEventTimerCount;
    int64_t keyGroupProcessingTimerCountBefore = restoreProcessingTimerCount;

    for (int32_t i = 0; i < serviceCount; ++i) {
        std::string serviceName = in->readUTF();
        auto keySerializerSnapshot = FlinkTimerSerializerSnapshots::readVersionedSnapshot(in);
        auto namespaceSerializerSnapshot = FlinkTimerSerializerSnapshots::readVersionedSnapshot(in);

        switch (inferNamespaceKind(namespaceSerializerSnapshot)) {
            case TimerNamespaceKind::INT64:
                readTimerServiceForNamespace<int64_t>(
                    in,
                    serviceName,
                    std::move(keySerializerSnapshot),
                    std::move(namespaceSerializerSnapshot),
                    keyGroupIdx,
                    version);
                break;
            case TimerNamespaceKind::TIME_WINDOW:
                readTimerServiceForNamespace<TimeWindow>(
                    in,
                    serviceName,
                    std::move(keySerializerSnapshot),
                    std::move(namespaceSerializerSnapshot),
                    keyGroupIdx,
                    version);
                break;
            case TimerNamespaceKind::VOID_NAMESPACE:
                readTimerServiceForNamespace<VoidNamespace>(
                    in,
                    serviceName,
                    std::move(keySerializerSnapshot),
                    std::move(namespaceSerializerSnapshot),
                    keyGroupIdx,
                    version);
                break;
        }
    }

    int64_t keyGroupEventTimerCount = restoreEventTimerCount - keyGroupEventTimerCountBefore;
    int64_t keyGroupProcessingTimerCount = restoreProcessingTimerCount - keyGroupProcessingTimerCountBefore;
    if (keyGroupEventTimerCount + keyGroupProcessingTimerCount > 0) {
        ++restoreNonEmptyKeyGroupCount;
    }
}

template <typename K>
template <typename N>
void InternalTimeServiceManager<K>::readTimerServiceForNamespace(
    RawKeyedStateInputStreamProxy* in,
    const std::string& serviceName,
    FlinkTimerSerializerSnapshots::SnapshotDescriptor keySerializerSnapshot,
    FlinkTimerSerializerSnapshots::SnapshotDescriptor namespaceSerializerSnapshot,
    int32_t keyGroupIdx,
    int32_t version)
{
    auto reader = InternalTimersSnapshotReaderWriters<K, N>::getReaderForVersion(
        version, std::move(keySerializerSnapshot), std::move(namespaceSerializerSnapshot), keySerializerForRestore);
    InternalTimersSnapshot<K, N> timersSnapshot = reader->readTimersSnapshot(in);
    int64_t eventTimerCount = static_cast<int64_t>(timersSnapshot.getEventTimeTimers().size());
    int64_t processingTimerCount = static_cast<int64_t>(timersSnapshot.getProcessingTimeTimers().size());
    restoreEventTimerCount += eventTimerCount;
    restoreProcessingTimerCount += processingTimerCount;
    if (eventTimerCount + processingTimerCount > 0) {
        ++restoreNonEmptyServiceKeyGroupCount;
    }
    if (TimerConsistencyCheckControl::timerConsistencyCheckEnabled) {
        TimerNamespaceKind namespaceKind = []() {
            if constexpr (std::is_same_v<N, int64_t>) {
                return TimerNamespaceKind::INT64;
            } else if constexpr (std::is_same_v<N, TimeWindow>) {
                return TimerNamespaceKind::TIME_WINDOW;
            } else {
                return TimerNamespaceKind::VOID_NAMESPACE;
            }
        }();
        TimerDataDigest keyGroupDigest = buildTimerDataDigest<N>(timersSnapshot);
        mergeTimerDataDigest(restoreTimerDataDigests, serviceName, namespaceKind, keyGroupIdx, keyGroupDigest);
    }

    auto* timerService = registerOrGetTimerServiceForRestore<N>(
        serviceName, timersSnapshot.getKeySerializer(), timersSnapshot.getNamespaceSerializer());
    timerService->restoreTimersForKeyGroup(timersSnapshot, keyGroupIdx);
}

template <typename K>
inline void InternalTimeServiceManager<K>::snapshotToRawKeyedState(
    KeyedStateCheckpointOutputStream* stateCheckpointOutputStream, std::string operatorName, int64_t checkpointId)
{
    if (stateCheckpointOutputStream == nullptr) {
        INFO_RELEASE(
            "Error: snapshotToRawKeyedState Raw keyed state output stream is null for operator " << operatorName);
        THROW_LOGIC_EXCEPTION("Raw keyed state output stream is null for operator " << operatorName);
    }

    try {
        activeSnapshotCheckpointId = checkpointId;
        snapshotEventTimerCount = 0;
        snapshotProcessingTimerCount = 0;
        snapshotWrittenKeyGroupCount = 0;
        snapshotNonEmptyKeyGroupCount = 0;
        snapshotNonEmptyServiceKeyGroupCount = 0;
        snapshotTimerDataDigests.clear();
        for (int32_t keyGroupIdx : stateCheckpointOutputStream->getKeyGroupList()) {
            activeSnapshotKeyGroupId = keyGroupIdx;
            stateCheckpointOutputStream->startNewKeyGroup(keyGroupIdx);
            InternalTimerServiceSerializationProxy<K> proxy(this, keyGroupIdx);
            proxy.write(stateCheckpointOutputStream);
        }
    } catch (const std::exception& e) {
        INFO_RELEASE(
            "TIMER_CP_SNAPSHOT_ABORT checkpointId="
            << checkpointId << ", managerPtr=" << reinterpret_cast<uintptr_t>(this) << ", operatorName=" << operatorName
            << ", phase=writeKeyGroups"
            << ", activeKeyGroupId=" << activeSnapshotKeyGroupId << ", writtenKeyGroupCount="
            << snapshotWrittenKeyGroupCount << ", registeredTimerServiceCount=" << getRegisteredTimerServiceCount()
            << ", int64TimerServiceCount=" << timerServicesInt64.size()
            << ", windowTimerServiceCount=" << timerServicesWindow.size()
            << ", voidTimerServiceCount=" << timerServicesVoidNameSpace.size() << ", error=" << e.what());
        activeSnapshotCheckpointId = -1;
        activeSnapshotKeyGroupId = -1;
        INFO_RELEASE(
            "Error: snapshotToRawKeyedState Could not write timer service of operator "
            << operatorName << " to raw keyed checkpoint state stream.");
        THROW_LOGIC_EXCEPTION(
            "Could not write timer service of operator "
            << operatorName << " to raw keyed checkpoint state stream. Root cause: " << e.what());
    }

    try {
        stateCheckpointOutputStream->close();
    } catch (const std::exception& e) {
        INFO_RELEASE(
            "TIMER_CP_SNAPSHOT_ABORT checkpointId="
            << checkpointId << ", managerPtr=" << reinterpret_cast<uintptr_t>(this) << ", operatorName=" << operatorName
            << ", phase=closeRawKeyedStateStream"
            << ", activeKeyGroupId=" << activeSnapshotKeyGroupId << ", writtenKeyGroupCount="
            << snapshotWrittenKeyGroupCount << ", registeredTimerServiceCount=" << getRegisteredTimerServiceCount()
            << ", int64TimerServiceCount=" << timerServicesInt64.size()
            << ", windowTimerServiceCount=" << timerServicesWindow.size()
            << ", voidTimerServiceCount=" << timerServicesVoidNameSpace.size() << ", error=" << e.what());
        activeSnapshotCheckpointId = -1;
        activeSnapshotKeyGroupId = -1;
        throw;
    }
    INFO_RELEASE(
        "TIMER_CP_SNAPSHOT_SUMMARY checkpointId="
        << checkpointId << ", managerPtr=" << reinterpret_cast<uintptr_t>(this) << ", operatorName=" << operatorName
        << ", keyGroupCount=" << snapshotWrittenKeyGroupCount << ", nonEmptyKeyGroupCount="
        << snapshotNonEmptyKeyGroupCount << ", registeredTimerServiceCount=" << getRegisteredTimerServiceCount()
        << ", int64TimerServiceCount=" << timerServicesInt64.size() << ", windowTimerServiceCount="
        << timerServicesWindow.size() << ", voidTimerServiceCount=" << timerServicesVoidNameSpace.size()
        << ", nonEmptyServiceKeyGroupCount=" << snapshotNonEmptyServiceKeyGroupCount
        << ", eventTimerCount=" << snapshotEventTimerCount << ", processingTimerCount=" << snapshotProcessingTimerCount
        << ", totalTimerCount=" << (snapshotEventTimerCount + snapshotProcessingTimerCount));
    if (TimerConsistencyCheckControl::timerConsistencyCheckEnabled) {
        logTimerDataDigests("snapshot", operatorName, checkpointId, snapshotTimerDataDigests);
    }
    activeSnapshotCheckpointId = -1;
    activeSnapshotKeyGroupId = -1;
}

template <typename K>
inline int32_t InternalTimeServiceManager<K>::getRegisteredTimerServiceCount() const
{
    return static_cast<int32_t>(
        timerServicesInt64.size() + timerServicesWindow.size() + timerServicesVoidNameSpace.size());
}

template <typename K>
inline void InternalTimeServiceManager<K>::writeTimersForKeyGroup(
    KeyedStateCheckpointOutputStream* out, int32_t keyGroupIdx)
{
    ++snapshotWrittenKeyGroupCount;
    int64_t keyGroupEventTimerCountBefore = snapshotEventTimerCount;
    int64_t keyGroupProcessingTimerCountBefore = snapshotProcessingTimerCount;

    out->writeInt(getRegisteredTimerServiceCount());
    writeTimerServiceMap<int64_t>(out, timerServicesInt64, keyGroupIdx);
    writeTimerServiceMap<TimeWindow>(out, timerServicesWindow, keyGroupIdx);
    writeTimerServiceMap<VoidNamespace>(out, timerServicesVoidNameSpace, keyGroupIdx);

    int64_t keyGroupEventTimerCount = snapshotEventTimerCount - keyGroupEventTimerCountBefore;
    int64_t keyGroupProcessingTimerCount = snapshotProcessingTimerCount - keyGroupProcessingTimerCountBefore;
    if (keyGroupEventTimerCount + keyGroupProcessingTimerCount > 0) {
        ++snapshotNonEmptyKeyGroupCount;
    }
}

template <typename K>
template <typename N>
inline void InternalTimeServiceManager<K>::writeTimerServiceMap(
    KeyedStateCheckpointOutputStream* out,
    emhash7::HashMap<std::string, InternalTimerService<N>*>& services,
    int32_t keyGroupIdx)
{
    for (auto& entry : services) {
        const std::string& serviceName = entry.first;
        auto* timerService = static_cast<InternalTimerServiceImpl<K, N>*>(entry.second);
        out->writeUTF(serviceName);

        auto timersSnapshot = timerService->snapshotTimersForKeyGroup(keyGroupIdx);
        int64_t eventTimerCount = static_cast<int64_t>(timersSnapshot.getEventTimeTimers().size());
        int64_t processingTimerCount = static_cast<int64_t>(timersSnapshot.getProcessingTimeTimers().size());
        snapshotEventTimerCount += eventTimerCount;
        snapshotProcessingTimerCount += processingTimerCount;
        if (eventTimerCount + processingTimerCount > 0) {
            ++snapshotNonEmptyServiceKeyGroupCount;
        }
        if (TimerConsistencyCheckControl::timerConsistencyCheckEnabled) {
            TimerNamespaceKind namespaceKind = [&]() {
                if constexpr (std::is_same_v<N, int64_t>) {
                    return TimerNamespaceKind::INT64;
                } else if constexpr (std::is_same_v<N, TimeWindow>) {
                    return TimerNamespaceKind::TIME_WINDOW;
                } else {
                    return TimerNamespaceKind::VOID_NAMESPACE;
                }
            }();
            TimerDataDigest keyGroupDigest = buildTimerDataDigest<N>(timersSnapshot);
            mergeTimerDataDigest(snapshotTimerDataDigests, serviceName, namespaceKind, keyGroupIdx, keyGroupDigest);
        }
        auto writer = InternalTimersSnapshotReaderWriters<K, N>::getWriterForVersion(
            InternalTimerServiceSerializationProxy<K>::VERSION,
            std::move(timersSnapshot),
            timerService->getKeySerializer(),
            timerService->getNamespaceSerializer());
        writer->writeTimersSnapshot(out);
    }
}

template <typename K>
template <typename N>
typename InternalTimeServiceManager<K>::TimerDataDigest InternalTimeServiceManager<K>::buildTimerDataDigest(
    const InternalTimersSnapshot<K, N>& timersSnapshot)
{
    TimerDataDigest digest;
    addTimersToDigest<N>(
        digest,
        timersSnapshot.getEventTimeTimers(),
        timersSnapshot.getKeySerializer(),
        timersSnapshot.getNamespaceSerializer(),
        true);
    addTimersToDigest<N>(
        digest,
        timersSnapshot.getProcessingTimeTimers(),
        timersSnapshot.getKeySerializer(),
        timersSnapshot.getNamespaceSerializer(),
        false);
    return digest;
}

template <typename K>
template <typename N>
void InternalTimeServiceManager<K>::addTimersToDigest(
    TimerDataDigest& digest,
    const std::vector<typename InternalTimersSnapshot<K, N>::TimerPtr>& timers,
    TypeSerializer* keySerializer,
    TypeSerializer* namespaceSerializer,
    bool eventTimer)
{
    for (const auto& timer : timers) {
        if (timer == nullptr) {
            continue;
        }
        std::vector<uint8_t> keyBytes =
            InternalTimeServiceManager<K>::template serializeTimerValueForDigest<K>(keySerializer, timer->getKey());
        std::vector<uint8_t> namespaceBytes = InternalTimeServiceManager<K>::template serializeTimerValueForDigest<N>(
            namespaceSerializer, timer->getNamespace());
        uint64_t timerHash = hashTimerBytes(eventTimer, timer->getTimestamp(), keyBytes, namespaceBytes);
        digest.addTimer(
            eventTimer,
            timer->getTimestamp(),
            timerHash,
            hashBytesForDigestLog(keyBytes),
            hashBytesForDigestLog(namespaceBytes),
            static_cast<int32_t>(keyBytes.size()),
            static_cast<int32_t>(namespaceBytes.size()),
            bytesPreviewHex(keyBytes, 32),
            bytesPreviewHex(namespaceBytes, 32));
    }
}

template <typename K>
template <typename T>
std::vector<uint8_t> InternalTimeServiceManager<K>::serializeTimerValueForDigest(TypeSerializer* serializer, T value)
{
    if (serializer == nullptr) {
        INFO_RELEASE("Error: serializeTimerValueForDigest Timer serializer is null.");
        THROW_LOGIC_EXCEPTION("Timer serializer is null while building timer digest.");
    }

    DataOutputSerializer target(128);
    if constexpr (std::is_same_v<T, int64_t>) {
        Long boxed(value);
        serializer->serialize(&boxed, target);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        Integer boxed(value);
        serializer->serialize(&boxed, target);
    } else if constexpr (std::is_same_v<T, Object*>) {
        serializer->serialize(value, target);
    } else if constexpr (is_shared_ptr_v<T>) {
        if (value == nullptr) {
            INFO_RELEASE("Error: serializeTimerValueForDigest Timer shared_ptr value is null.");
            THROW_LOGIC_EXCEPTION("Timer shared_ptr value is null while building timer digest.");
        }
        serializer->serialize(value.get(), target);
    } else if constexpr (std::is_pointer_v<T>) {
        serializer->serialize(static_cast<void*>(value), target);
    } else {
        T copy = value;
        serializer->serialize(static_cast<void*>(&copy), target);
    }

    int32_t length = target.getPosition();
    if (length <= 0 || target.getData() == nullptr) {
        return {};
    }
    return std::vector<uint8_t>(target.getData(), target.getData() + length);
}

template <typename K>
uint64_t InternalTimeServiceManager<K>::hashTimerBytes(
    bool eventTimer,
    int64_t timestamp,
    const std::vector<uint8_t>& keyBytes,
    const std::vector<uint8_t>& namespaceBytes)
{
    static constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
    uint64_t hash = FNV_OFFSET_BASIS;
    uint8_t queueType = eventTimer ? 1U : 2U;
    hash = fnv1aUpdate(hash, &queueType, 1);
    hash = fnv1aUpdateInt64(hash, timestamp);
    hash = fnv1aUpdateInt32(hash, static_cast<int32_t>(keyBytes.size()));
    hash = fnv1aUpdate(hash, keyBytes.data(), keyBytes.size());
    hash = fnv1aUpdateInt32(hash, static_cast<int32_t>(namespaceBytes.size()));
    hash = fnv1aUpdate(hash, namespaceBytes.data(), namespaceBytes.size());
    return hash;
}

template <typename K>
uint64_t InternalTimeServiceManager<K>::hashBytesForDigestLog(const std::vector<uint8_t>& bytes)
{
    static constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
    uint64_t hash = FNV_OFFSET_BASIS;
    hash = fnv1aUpdateInt32(hash, static_cast<int32_t>(bytes.size()));
    return fnv1aUpdate(hash, bytes.data(), bytes.size());
}

template <typename K>
std::string InternalTimeServiceManager<K>::bytesPreviewHex(const std::vector<uint8_t>& bytes, size_t maxBytes)
{
    static constexpr char HEX[] = "0123456789abcdef";
    size_t previewLength = std::min(bytes.size(), maxBytes);
    std::string result;
    result.reserve(previewLength * 2);
    for (size_t i = 0; i < previewLength; ++i) {
        uint8_t value = bytes[i];
        result.push_back(HEX[(value >> 4U) & 0x0fU]);
        result.push_back(HEX[value & 0x0fU]);
    }
    return result;
}

template <typename K>
uint64_t InternalTimeServiceManager<K>::fnv1aUpdate(uint64_t hash, const uint8_t* data, size_t length)
{
    static constexpr uint64_t FNV_PRIME = 1099511628211ULL;
    if (data == nullptr || length == 0) {
        return hash;
    }
    for (size_t i = 0; i < length; ++i) {
        hash ^= static_cast<uint64_t>(data[i]);
        hash *= FNV_PRIME;
    }
    return hash;
}

template <typename K>
uint64_t InternalTimeServiceManager<K>::fnv1aUpdateInt64(uint64_t hash, int64_t value)
{
    uint64_t unsignedValue = static_cast<uint64_t>(value);
    uint8_t bytes[8];
    for (int32_t i = 0; i < 8; ++i) {
        bytes[i] = static_cast<uint8_t>((unsignedValue >> ((7 - i) * 8)) & 0xffU);
    }
    return fnv1aUpdate(hash, bytes, sizeof(bytes));
}

template <typename K>
uint64_t InternalTimeServiceManager<K>::fnv1aUpdateInt32(uint64_t hash, int32_t value)
{
    uint32_t unsignedValue = static_cast<uint32_t>(value);
    uint8_t bytes[4];
    for (int32_t i = 0; i < 4; ++i) {
        bytes[i] = static_cast<uint8_t>((unsignedValue >> ((3 - i) * 8)) & 0xffU);
    }
    return fnv1aUpdate(hash, bytes, sizeof(bytes));
}

template <typename K>
std::string InternalTimeServiceManager<K>::namespaceKindName(TimerNamespaceKind namespaceKind)
{
    switch (namespaceKind) {
        case TimerNamespaceKind::INT64: return "INT64";
        case TimerNamespaceKind::TIME_WINDOW: return "TIME_WINDOW";
        case TimerNamespaceKind::VOID_NAMESPACE: return "VOID_NAMESPACE";
        default: return "UNKNOWN";
    }
}

template <typename K>
std::string InternalTimeServiceManager<K>::digestMapKey(
    const std::string& serviceName, TimerNamespaceKind namespaceKind)
{
    return namespaceKindName(namespaceKind) + "|" + serviceName;
}

template <typename K>
int64_t InternalTimeServiceManager<K>::printableMinTimestamp(int64_t count, int64_t value)
{
    return count > 0 ? value : -1;
}

template <typename K>
int64_t InternalTimeServiceManager<K>::printableMaxTimestamp(int64_t count, int64_t value)
{
    return count > 0 ? value : -1;
}

template <typename K>
void InternalTimeServiceManager<K>::mergeTimerDataDigest(
    std::unordered_map<std::string, TimerDataDigest>& target,
    const std::string& serviceName,
    TimerNamespaceKind namespaceKind,
    int32_t keyGroupIdx,
    const TimerDataDigest& keyGroupDigest)
{
    target[digestMapKey(serviceName, namespaceKind)].mergeKeyGroupDigest(keyGroupIdx, keyGroupDigest);
}

template <typename K>
void InternalTimeServiceManager<K>::logTimerDataDigest(
    const char* phase,
    const std::string& operatorName,
    int64_t checkpointId,
    const std::string& serviceName,
    TimerNamespaceKind namespaceKind,
    const TimerDataDigest& digest)
{
    INFO_RELEASE(
        "TIMER_CP_HEAP_PQ_DATA_DIGEST phase="
        << phase << ", checkpointId=" << checkpointId << ", backend=ROCKSDB_KEYED"
        << ", pqStorage=HEAP"
        << ", managerPtr=" << reinterpret_cast<uintptr_t>(this) << ", operatorName=" << operatorName
        << ", serviceName=" << serviceName << ", namespaceKind=" << namespaceKindName(namespaceKind)
        << ", keyGroupCount=" << digest.keyGroupCount << ", nonEmptyKeyGroupCount=" << digest.nonEmptyKeyGroupCount
        << ", eventTimerCount=" << digest.eventTimerCount << ", processingTimerCount=" << digest.processingTimerCount
        << ", totalTimerCount=" << digest.totalTimerCount() << ", eventXorHash=" << digest.eventXorHash
        << ", eventSumHash=" << digest.eventSumHash << ", processingXorHash=" << digest.processingXorHash
        << ", processingSumHash=" << digest.processingSumHash << ", keyGroupXorHash=" << digest.keyGroupXorHash
        << ", keyGroupSumHash=" << digest.keyGroupSumHash
        << ", eventMinTimestamp=" << printableMinTimestamp(digest.eventTimerCount, digest.eventMinTimestamp)
        << ", eventMaxTimestamp=" << printableMaxTimestamp(digest.eventTimerCount, digest.eventMaxTimestamp)
        << ", processingMinTimestamp="
        << printableMinTimestamp(digest.processingTimerCount, digest.processingMinTimestamp)
        << ", processingMaxTimestamp="
        << printableMaxTimestamp(digest.processingTimerCount, digest.processingMaxTimestamp));
    logTimerDataSample(phase, operatorName, checkpointId, serviceName, namespaceKindName(namespaceKind), digest);
}

template <typename K>
void InternalTimeServiceManager<K>::logTimerDataDigests(
    const char* phase,
    const std::string& operatorName,
    int64_t checkpointId,
    const std::unordered_map<std::string, TimerDataDigest>& digests)
{
    for (const auto& entry : digests) {
        const std::string& key = entry.first;
        size_t separator = key.find('|');
        std::string namespaceKind = separator == std::string::npos ? "UNKNOWN" : key.substr(0, separator);
        std::string serviceName = separator == std::string::npos ? key : key.substr(separator + 1);
        const TimerDataDigest& digest = entry.second;
        INFO_RELEASE(
            "TIMER_CP_HEAP_PQ_DATA_DIGEST phase="
            << phase << ", checkpointId=" << checkpointId << ", backend=ROCKSDB_KEYED"
            << ", pqStorage=HEAP"
            << ", managerPtr=" << reinterpret_cast<uintptr_t>(this) << ", operatorName=" << operatorName
            << ", serviceName=" << serviceName << ", namespaceKind=" << namespaceKind
            << ", keyGroupCount=" << digest.keyGroupCount << ", nonEmptyKeyGroupCount=" << digest.nonEmptyKeyGroupCount
            << ", eventTimerCount=" << digest.eventTimerCount << ", processingTimerCount="
            << digest.processingTimerCount << ", totalTimerCount=" << digest.totalTimerCount()
            << ", eventXorHash=" << digest.eventXorHash << ", eventSumHash=" << digest.eventSumHash
            << ", processingXorHash=" << digest.processingXorHash << ", processingSumHash=" << digest.processingSumHash
            << ", keyGroupXorHash=" << digest.keyGroupXorHash << ", keyGroupSumHash=" << digest.keyGroupSumHash
            << ", eventMinTimestamp=" << printableMinTimestamp(digest.eventTimerCount, digest.eventMinTimestamp)
            << ", eventMaxTimestamp=" << printableMaxTimestamp(digest.eventTimerCount, digest.eventMaxTimestamp)
            << ", processingMinTimestamp="
            << printableMinTimestamp(digest.processingTimerCount, digest.processingMinTimestamp)
            << ", processingMaxTimestamp="
            << printableMaxTimestamp(digest.processingTimerCount, digest.processingMaxTimestamp));
        logTimerDataSample(phase, operatorName, checkpointId, serviceName, namespaceKind, digest);
    }
}

template <typename K>
void InternalTimeServiceManager<K>::logTimerDataSample(
    const char* phase,
    const std::string& operatorName,
    int64_t checkpointId,
    const std::string& serviceName,
    const std::string& namespaceKind,
    const TimerDataDigest& digest)
{
    if (!digest.sample.present) {
        return;
    }
    INFO_RELEASE(
        "TIMER_CP_HEAP_PQ_DATA_SAMPLE phase="
        << phase << ", checkpointId=" << checkpointId << ", backend=ROCKSDB_KEYED"
        << ", pqStorage=HEAP"
        << ", managerPtr=" << reinterpret_cast<uintptr_t>(this) << ", operatorName=" << operatorName << ", serviceName="
        << serviceName << ", namespaceKind=" << namespaceKind << ", keyGroupId=" << digest.sample.keyGroupId
        << ", queue=" << (digest.sample.eventTimer ? "event" : "processing")
        << ", timestamp=" << digest.sample.timestamp << ", timerHash=" << digest.sample.timerHash
        << ", sampleRankHash=" << digest.sample.sampleRankHash << ", keyHash=" << digest.sample.keyHash
        << ", namespaceHash=" << digest.sample.namespaceHash << ", keyBytesLen=" << digest.sample.keyBytesLength
        << ", namespaceBytesLen=" << digest.sample.namespaceBytesLength << ", keyPreviewHex="
        << digest.sample.keyPreviewHex << ", namespacePreviewHex=" << digest.sample.namespacePreviewHex);
}
