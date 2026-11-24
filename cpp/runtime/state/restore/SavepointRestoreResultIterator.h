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

#ifndef OMNISTREAM_SAVEPOINTRESTORERESULTITERATOR_H
#define OMNISTREAM_SAVEPOINTRESTORERESULTITERATOR_H

#include "SavepointRestoreResult.h"
#include "KeyGroup.h"
#include "KeyGroupIterator.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/TaskStateSnapshotSerializer.h"

class SavepointRestoreResultIterator {
public:
    SavepointRestoreResultIterator() : currentIndex_(0)
    {
    }

    SavepointRestoreResultIterator(
        const std::vector<std::shared_ptr<KeyedStateHandle>>& stateHandles,
        std::shared_ptr<OmniTaskBridge> omniTaskBridge)
        : omniTaskBridge_(omniTaskBridge),
          stateHandles_(stateHandles),
          currentIndex_(0)
    {
    }

    bool hasNext()
    {
        return currentIndex_ < stateHandles_.size();
    }

    std::unique_ptr<SavepointRestoreResult> next()
    {
        if (!hasNext()) {
            throw std::out_of_range("No more elements in SavepointRestoreResultIterator");
        }

        auto stateHandle = stateHandles_[currentIndex_++];
        auto keyedStateHandle = std::dynamic_pointer_cast<KeyGroupsStateHandle>(stateHandle);
        if (!keyedStateHandle) {
            throw unexpectedStateHandleException(typeid(KeyGroupsStateHandle), typeid(*stateHandle));
        }
        // In a real implementation, this would deserialize the state handle
        // and extract the actual state metadata and key groups
        // For now, we create a placeholder
        auto serializerStr = TaskStateSnapshotSerializer::parseKeyGroupsStateHandle(keyedStateHandle);
        std::vector<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
            omniTaskBridge_->readMetaData(to_string(serializerStr));

        // This would involve deserializing the savepoint data
        auto keyGroupIterator = std::make_shared<KeyGroupIterator>(keyedStateHandle, omniTaskBridge_);

        return std::make_unique<SavepointRestoreResult>(stateMetaInfoSnapshots, keyGroupIterator);
    };

private:
    std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles_;
    std::shared_ptr<OmniTaskBridge> omniTaskBridge_;
    size_t currentIndex_;
    std::runtime_error unexpectedStateHandleException(const std::type_info& expected, const std::type_info& actual)
    {
        return std::runtime_error(
            "Unexpected state handle type: expected " + std::string(expected.name()) + ", but got " +
            std::string(actual.name()));
    }
};

#endif // OMNISTREAM_SAVEPOINTRESTORERESULTITERATOR_H
