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
#include <memory>
#include <string>
#include <unordered_map>

#include "KeyGroupRange.h"
#include "TimerConsistencyCheckControl.h"
#include "heap/HeapPriorityQueueDataDigest.h"
#include "heap/HeapPriorityQueueSetFactory.h"
#include "heap/HeapPriorityQueueSnapshotRestoreWrapper.h"
#include "heap/RestoredHeapPriorityQueueSnapshotRestoreWrapper.h"

class HeapPriorityQueuesManager {
public:
    HeapPriorityQueuesManager(
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
            registeredPQStates,
        std::shared_ptr<HeapPriorityQueueSetFactory> priorityQueueSetFactory,
        KeyGroupRange* keyGroupRange,
        int32_t numberOfKeyGroups)
        : registeredPQStates_(registeredPQStates),
          priorityQueueSetFactory_(priorityQueueSetFactory),
          keyGroupRange_(keyGroupRange),
          numberOfKeyGroups_(numberOfKeyGroups)
    {
    }

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> createOrUpdate(
        std::string stateName, TypeSerializer* byteOrderedElementSerializer)
    {
        return createOrUpdate<K, T, Comparator>(stateName, byteOrderedElementSerializer, false);
    }

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> createOrUpdate(
        std::string stateName, TypeSerializer* byteOrderedElementSerializer, bool allowFutureMetadataUpdates)
    {
        auto iter = registeredPQStates_->find(stateName);
        if (iter != registeredPQStates_->end()) {
            auto existingState =
                std::dynamic_pointer_cast<HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>>(iter->second);
            if (existingState != nullptr) {
                // todo: TypeSerializerSchemaCompatibility
                return existingState->getHeapPriorityQueueSet();
            }

            auto pendingRestoredState =
                std::dynamic_pointer_cast<RestoredHeapPriorityQueueSnapshotRestoreWrapper>(iter->second);
            if (pendingRestoredState != nullptr) {
                auto metaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(
                    stateName, byteOrderedElementSerializer);
                auto restoredState = createInternal<K, T, Comparator>(metaInfo);
                const size_t pendingEntryCount = pendingRestoredState->size();
                pendingRestoredState->template drainTo<K, T, Comparator>(restoredState, getKeyGroupPrefixBytes());
                if (TimerConsistencyCheckControl::timerConsistencyCheckEnabled) {
                    logRestoredQueueDigest<K, T, Comparator>(stateName, restoredState);
                }
                INFO_RELEASE(
                    "HeapPriorityQueuesManager: drained "
                    << pendingEntryCount << " pending restored entries for priority queue state '" << stateName << "'");
                return restoredState->getHeapPriorityQueueSet();
            }

            INFO_RELEASE(
                "Error: createOrUpdate Priority queue type is not supported for restored HEAP PQ state:" << stateName);
            THROW_LOGIC_EXCEPTION("Priority queue type is not supported for restored HEAP PQ state: " << stateName);
        }
        auto metaInfo =
            std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(stateName, byteOrderedElementSerializer);

        // todo: withSerializerUpgradesAllowed
        return createInternal<K, T, Comparator>(metaInfo)->getHeapPriorityQueueSet();
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
    getRegisteredPQStates()
    {
        return registeredPQStates_;
    }

private:
    int getKeyGroupPrefixBytes() const
    {
        return numberOfKeyGroups_ > 128 ? 2 : 1;
    }

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>> createInternal(
        std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo)
    {
        const std::string& stateName = metaInfo->getName();

        std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> priorityQueue =
            priorityQueueSetFactory_->create<K, T, Comparator>(stateName, metaInfo->getElementSerializer());

        using WrapperType = HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>;
        std::shared_ptr<WrapperType> wrapper = std::make_shared<WrapperType>(
            std::static_pointer_cast<HeapPriorityQueueSet<K, T, Comparator>>(priorityQueue),
            metaInfo,
            keyGroupRange_,
            numberOfKeyGroups_);

        (*registeredPQStates_)[stateName] = wrapper;

        return wrapper;
    }

    template <typename K, typename T, typename Comparator>
    void logRestoredQueueDigest(
        const std::string& stateName,
        const std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>>& wrapper)
    {
        auto summary = HeapPriorityQueueDataDigest::createSummary(
            "restore",
            -1,
            stateName,
            keyGroupRange_ == nullptr ? numberOfKeyGroups_ : keyGroupRange_->getNumberOfKeyGroups());

        if (wrapper != nullptr) {
            auto iterator = wrapper->createSnapshotIterator(-1, getKeyGroupPrefixBytes());
            while (iterator != nullptr && iterator->isValid()) {
                HeapPriorityQueueDataDigest::addSerializedEntry(
                    summary, iterator->key(), iterator->value(), getKeyGroupPrefixBytes());
                iterator->next();
            }
            if (iterator != nullptr) {
                iterator->close();
            }
        }

        HeapPriorityQueueDataDigest::logSummary(summary);
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
        registeredPQStates_;
    std::shared_ptr<HeapPriorityQueueSetFactory> priorityQueueSetFactory_;
    KeyGroupRange* keyGroupRange_;
    int32_t numberOfKeyGroups_;
};
