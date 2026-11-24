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

#ifndef OMNISTREAM_KEYEDSTATECHECKPOINTOUTPUTSTREAM
#define OMNISTREAM_KEYEDSTATECHECKPOINTOUTPUTSTREAM

#include <cstdint>
#include <memory>
#include <vector>

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/bridge/OmniTaskBridge.h"
#include "core/include/common.h"

/**
 * Output stream for raw keyed state.
 *
 * This mirrors Flink's KeyedStateCheckpointOutputStream: every call to
 * startNewKeyGroup records the current delegate stream offset for the key-group.
 * close() is intentionally non-finalizing because snapshotToRawKeyedState calls it
 * before StateSnapshotContextSynchronousImpl builds the final future. The actual
 * close and KeyGroupsStateHandle creation is done by closeAndGetHandle().
 */
class KeyedStateCheckpointOutputStream {
public:
    static constexpr int64_t NO_OFFSET_SET = -1L;
    static constexpr int32_t NO_CURRENT_KEY_GROUP = -1;

    KeyedStateCheckpointOutputStream(
        const std::shared_ptr<omnistream::OmniTaskBridge>& bridge,
        long checkpointId,
        CheckpointOptions* checkpointOptions,
        KeyGroupRange* keyGroupRange)
        : delegate_(std::make_unique<CheckpointStateOutputStreamProxy>(bridge, checkpointId, checkpointOptions)),
          keyGroupRange_(*keyGroupRange),
          keyGroupRangeOffsets_(
              *keyGroupRange, std::vector<int64_t>(keyGroupRange->getNumberOfKeyGroups(), NO_OFFSET_SET))
    {
    }

    std::vector<int32_t> getKeyGroupList() const
    {
        std::vector<int32_t> result;
        for (int32_t keyGroup = keyGroupRange_.getStartKeyGroup(); keyGroup <= keyGroupRange_.getEndKeyGroup();
             ++keyGroup) {
            result.push_back(keyGroup);
        }
        return result;
    }

    const KeyGroupRange& getKeyGroupRange() const
    {
        return keyGroupRange_;
    }

    void startNewKeyGroup(int32_t keyGroupId)
    {
        if (!keyGroupRange_.contains(keyGroupId)) {
            throw KeyGroupRangeOffsets::newIllegalKeyGroupException(keyGroupId, keyGroupRange_);
        }
        if (keyGroupRangeOffsets_.getKeyGroupOffset(keyGroupId) != NO_OFFSET_SET) {
            INFO_RELEASE(
                "Error: startNewKeyGroup Key group already registered in raw keyed stream, keyGroupId:" << keyGroupId);
            THROW_LOGIC_EXCEPTION("Key group already registered in raw keyed stream: " << keyGroupId);
        }
        keyGroupRangeOffsets_.setKeyGroupOffset(keyGroupId, static_cast<int64_t>(delegate_->getPos()));
        currentKeyGroup_ = keyGroupId;
    }

    void writeByte(uint8_t data)
    {
        delegate_->writeByte(data);
    }

    void writeShort(int16_t data)
    {
        delegate_->writeShort(data);
    }

    void writeInt(int32_t data)
    {
        delegate_->writeInt(data);
    }

    void writeLong(int64_t data)
    {
        delegate_->writeLong(data);
    }

    void writeUTF(const std::string& data)
    {
        delegate_->writeUTF(data);
    }

    void writeBytes(const void* data, size_t len)
    {
        delegate_->writeBytes(data, len);
    }

    int64_t getPos() const
    {
        return static_cast<int64_t>(delegate_->getPos());
    }

    void close()
    {
        userClosed_ = true;
    }

    std::shared_ptr<SnapshotResult<KeyedStateHandle>> closeAndGetHandle()
    {
        if (finalized_) {
            return finalizedResult_;
        }
        finalized_ = true;

        if (delegate_ == nullptr) {
            finalizedResult_ = SnapshotResult<KeyedStateHandle>::Empty();
            return finalizedResult_;
        }

        std::shared_ptr<SnapshotResult<StreamStateHandle>> streamSnapshot = delegate_->close();
        if (streamSnapshot == nullptr || streamSnapshot->GetJobManagerOwnedSnapshot() == nullptr) {
            finalizedResult_ = SnapshotResult<KeyedStateHandle>::Empty();
            return finalizedResult_;
        }

        std::shared_ptr<KeyedStateHandle> keyedStateHandle =
            std::make_shared<KeyGroupsStateHandle>(keyGroupRangeOffsets_, streamSnapshot->GetJobManagerOwnedSnapshot());
        finalizedResult_ = SnapshotResult<KeyedStateHandle>::Of(keyedStateHandle);
        return finalizedResult_;
    }

    void closeExceptionally()
    {
        if (delegate_ != nullptr) {
            delegate_->close();
        }
        finalized_ = true;
        finalizedResult_ = SnapshotResult<KeyedStateHandle>::Empty();
    }

private:
    std::unique_ptr<CheckpointStateOutputStreamProxy> delegate_;
    KeyGroupRange keyGroupRange_;
    KeyGroupRangeOffsets keyGroupRangeOffsets_;
    int32_t currentKeyGroup_ = NO_CURRENT_KEY_GROUP;
    bool userClosed_ = false;
    bool finalized_ = false;
    std::shared_ptr<SnapshotResult<KeyedStateHandle>> finalizedResult_ = nullptr;
};

#endif // OMNISTREAM_KEYEDSTATECHECKPOINTOUTPUTSTREAM
