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
#include "HeapPriorityQueueSet.h"
#include "HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "HeapPriorityQueueSingleStateIterator.h"
#include "core/memory/DataInputDeserializer.h"
#include "state/RegisteredPriorityQueueStateBackendMetaInfo.h"

template <typename K, typename T, typename Comparator>
class HeapPriorityQueueSnapshotRestoreWrapper : public HeapPriorityQueueSnapshotRestoreWrapperBase {
public:
    HeapPriorityQueueSnapshotRestoreWrapper(
        std::shared_ptr<HeapPriorityQueueSet<K, T, Comparator>> heapPriorityQueueSet,
        std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo,
        KeyGroupRange* keyGroupRange,
        int32_t totalKeyGroupSize)
        : heapPriorityQueueSet_(heapPriorityQueueSet),
          metaInfo_(metaInfo),
          keyGroupRange_(keyGroupRange),
          totalKeyGroupSize_(totalKeyGroupSize)
    {
    }

    std::shared_ptr<HeapPriorityQueueSet<K, T, Comparator>> getHeapPriorityQueueSet()
    {
        return heapPriorityQueueSet_;
    }

    std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> getMetaInfo()
    {
        return metaInfo_;
    }

    std::shared_ptr<StateMetaInfoSnapshot> snapshotMetaInfo() override
    {
        return metaInfo_->snapshot();
    }

    const std::string& getStateName() const override
    {
        static const std::string emptyName;
        return metaInfo_ == nullptr ? emptyName : metaInfo_->getName();
    }

    std::unique_ptr<SingleStateIterator> createSnapshotIterator(int kvStateId, int keyGroupPrefixBytes) override
    {
        return std::make_unique<HeapPriorityQueueSingleStateIterator<K, T, Comparator>>(
            heapPriorityQueueSet_.get(),
            metaInfo_->getElementSerializer(),
            kvStateId,
            keyGroupPrefixBytes,
            totalKeyGroupSize_);
    }

    void restoreSerializedElement(const std::vector<int8_t>& serializedKey, int keyGroupPrefixBytes) override
    {
        if (serializedKey.empty()) {
            return;
        }
        if (keyGroupPrefixBytes < 0 || static_cast<size_t>(keyGroupPrefixBytes) > serializedKey.size()) {
            INFO_RELEASE("Error: restoreSerializedElement Invalid PRIORITY_QUEUE serialized key prefix length");
            THROW_LOGIC_EXCEPTION("Invalid PRIORITY_QUEUE serialized key prefix length");
        }
        if (metaInfo_ == nullptr || metaInfo_->getElementSerializer() == nullptr) {
            INFO_RELEASE("Error: restoreSerializedElement Priority queue element serializer is null during restore");
            THROW_LOGIC_EXCEPTION("Priority queue element serializer is null during restore");
        }
        if (heapPriorityQueueSet_ == nullptr) {
            INFO_RELEASE("Error: restoreSerializedElement Priority queue set is null during restore");
            THROW_LOGIC_EXCEPTION("Priority queue set is null during restore");
        }

        DataInputDeserializer input(
            reinterpret_cast<const uint8_t*>(serializedKey.data()),
            static_cast<int>(serializedKey.size()),
            keyGroupPrefixBytes);

        using ElementType = typename T::element_type;
        void* rawElement = metaInfo_->getElementSerializer()->deserialize(input);
        if (rawElement == nullptr) {
            return;
        }

        T restoredElement(static_cast<ElementType*>(rawElement));
        heapPriorityQueueSet_->add(restoredElement);
    }

private:
    std::shared_ptr<HeapPriorityQueueSet<K, T, Comparator>> heapPriorityQueueSet_;
    std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo_;
    KeyGroupRange* keyGroupRange_;
    int32_t totalKeyGroupSize_;
};
