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

#ifndef OMNISTREAM_KEYGROUPENTRYITERATOR_H
#define OMNISTREAM_KEYGROUPENTRYITERATOR_H

#include <stdexcept>
#include "KeyGroupEntry.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"

class KeyGroupEntryIterator {
public:
    KeyGroupEntryIterator(
        int64_t offset,
        std::shared_ptr<KeyGroupsStateHandle> keyGroupsStateHandle,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge,
        jobject inputStream,
        bool isUsingKeyGroupCompression)
        : offset_(offset),
          keyGroupsStateHandle_(keyGroupsStateHandle),
          omniTaskBridge_(omniTaskBridge),
          inputStream_(inputStream),
          isUsingKeyGroupCompression_(isUsingKeyGroupCompression),
          currentKvStateId_(-1),
          currentIndex_(0)
    {
    }

    bool hasNext()
    {
        if (currentIndex_ < entries_.size()) {
            return true;
        }
        bool shouldReadNextBatch =
            currentKvStateId_ == -1 || ((END_OF_KEY_GROUP_MARK & currentKvStateId_) != END_OF_KEY_GROUP_MARK);
        if ((offset_ != 0) && shouldReadNextBatch) {
            if (currentIndex_ >= entries_.size()) {
                // jni read
                currentIndex_ = 0;
                omniTaskBridge_->getKeyGroupEntries(
                    inputStream_, currentKvStateId_, isUsingKeyGroupCompression_, entries_);
                // if entries_ is empty, it means jni has exceptions, return false to forbidden coredump
                if (entries_.size() == 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    KeyGroupEntry next()
    {
        if (!hasNext()) {
            throw std::out_of_range("No more elements in KeyGroupEntryIterator");
        }
        return entries_[currentIndex_++];
    }

private:
    int64_t offset_;
    std::shared_ptr<KeyGroupsStateHandle> keyGroupsStateHandle_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    int currentKvStateId_;
    size_t currentIndex_;
    jobject inputStream_;
    bool isUsingKeyGroupCompression_;
    std::vector<KeyGroupEntry> entries_;
    static constexpr int END_OF_KEY_GROUP_MARK = 0XFFFF;
};

#endif // OMNISTREAM_KEYGROUPENTRYITERATOR_H
