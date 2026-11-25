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

#ifndef OMNISTREAM_KEYGROUPITERATOR_H
#define OMNISTREAM_KEYGROUPITERATOR_H
#include "KeyGroup.h"
#include "KeyGroupEntry.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"

class KeyGroupIterator {
public:
    KeyGroupIterator(
        std::shared_ptr<KeyGroupsStateHandle> keyGroupsStateHandle, std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : keyGroupsStateHandle_(keyGroupsStateHandle),
          omniTaskBridge_(omniTaskBridge),
          keyGroupRangeOffset_(keyGroupsStateHandle->getGroupRangeOffsets()),
          keyGroupRange_(keyGroupsStateHandle->getGroupRangeOffsets().getKeyGroupRange()),
          currentIndex_(0)
    {
        auto serializerStr = TaskStateSnapshotSerializer::parseKeyGroupsStateHandle(keyGroupsStateHandle_);
        inputStream_ = omniTaskBridge_->getSavepointInputStream(to_string(serializerStr));
        isUsingKeyGroupCompression_ = omniTaskBridge_->isUsingKeyGroupCompression(inputStream_);
    }

    ~KeyGroupIterator()
    {
        if (inputStream_ != nullptr) {
            omniTaskBridge_->closeSavepointInputStream(inputStream_);
        }
    }

    bool hasNext()
    {
        return currentIndex_ < keyGroupRange_.getNumberOfKeyGroups();
    }

    std::unique_ptr<KeyGroup> next()
    {
        if (!hasNext()) {
            throw std::out_of_range("No more elements in KeyGroupIterator");
        }

        int keyGroup = keyGroupRange_.getKeyGroupId(currentIndex_++);
        int64_t offset = keyGroupRangeOffset_.getKeyGroupOffset(keyGroup);
        omniTaskBridge_->setSavepointInputStreamOffset(inputStream_, offset);
        return std::make_unique<KeyGroup>(KeyGroup(
            keyGroup,
            std::make_unique<KeyGroupEntryIterator>(
                offset, keyGroupsStateHandle_, omniTaskBridge_, inputStream_, isUsingKeyGroupCompression_)));
    }

private:
    std::shared_ptr<KeyGroupsStateHandle> keyGroupsStateHandle_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    KeyGroupRangeOffsets keyGroupRangeOffset_;
    KeyGroupRange keyGroupRange_;
    size_t currentIndex_;
    jobject inputStream_;
    bool isUsingKeyGroupCompression_;
};

#endif // OMNISTREAM_KEYGROUPITERATOR_H
