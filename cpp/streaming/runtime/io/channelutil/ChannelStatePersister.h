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

#ifndef CHANNELSTATEPERSISTER_H
#define CHANNELSTATEPERSISTER_H
#include <exception>
#include <memory>

#include "partition/consumer/InputChannelInfo.h"
#include "checkpoint/channel/ChannelStateWriter.h"
#include "io/network/api/serialization/EventSerializer.h"

namespace omnistream {

class ChannelStatePersister {
    enum class CheckpointStatus {
        COMPLETED,
        BARRIER_PENDING,
        BARRIER_RECEIVED
    };

    class RetainedBufferGuard {
    public:
        explicit RetainedBufferGuard(Buffer* buffer) : buffer_(buffer)
        {
        }

        RetainedBufferGuard(const RetainedBufferGuard&) = delete;
        RetainedBufferGuard& operator=(const RetainedBufferGuard&) = delete;

        ~RetainedBufferGuard()
        {
            if (buffer_ == nullptr) {
                return;
            }
            try {
                buffer_->RecycleBuffer();
            } catch (const std::exception& e) {
                INFO_RELEASE("ERROR: Failed to recycle an untransferred channel-state buffer: " << e.what());
            } catch (...) {
                INFO_RELEASE(
                    "ERROR: Failed to recycle an untransferred channel-state buffer due to an unknown exception");
            }
        }

        Buffer* Get() const
        {
            return buffer_;
        }

        void Release()
        {
            buffer_ = nullptr;
        }

    private:
        Buffer* buffer_;
    };

public:
    explicit ChannelStatePersister(
        const std::shared_ptr<ChannelStateWriter>& channelStateWriter, const InputChannelInfo& channelInfo)
        : channelStateWriter_(channelStateWriter),
          channelInfo_(channelInfo)
    {
    }

    void StartPersisting(int64_t barrierId, const std::vector<Buffer*>& knownBuffers)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        LOG("StartPersisting" << barrierId);

        if (checkpointStatus_ == CheckpointStatus::BARRIER_RECEIVED && lastSeenBarrier_ > barrierId) {
            throw std::runtime_error(
                "Barrier for newer checkpoint " + std::to_string(lastSeenBarrier_) +
                " has already been received compared to the requested checkpoint " + std::to_string(barrierId));
        }

        if (lastSeenBarrier_ < barrierId) {
            checkpointStatus_ = CheckpointStatus::BARRIER_PENDING;
            lastSeenBarrier_ = barrierId;
        }

        if (!knownBuffers.empty()) {
            channelStateWriter_->AddInputData(
                barrierId, channelInfo_, ChannelStateWriter::sequenceNumberUnknown, knownBuffers);
        }
    }

    void StopPersisting(int64_t id)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        LOG("StopPersisting" << id);
        if (id >= lastSeenBarrier_) {
            checkpointStatus_ = CheckpointStatus::COMPLETED;
            lastSeenBarrier_ = id;
        }
    }

    void MaybePersist(Buffer* buffer)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        if (checkpointStatus_ != CheckpointStatus::BARRIER_PENDING || buffer == nullptr || !buffer->isBuffer()) {
            return;
        }

        try {
            RetainedBufferGuard retainedBuffer(buffer->RetainBuffer());
            if (retainedBuffer.Get() == nullptr) {
                INFO_RELEASE("ERROR: Failed to retain an input buffer for channel-state persistence");
                return;
            }

            std::vector<Buffer*> buffers;
            buffers.push_back(retainedBuffer.Get());
            channelStateWriter_->AddInputData(
                lastSeenBarrier_, channelInfo_, ChannelStateWriter::sequenceNumberUnknown, buffers);
            // A successful call transfers this retained reference to ChannelStateWriter.
            retainedBuffer.Release();
        } catch (const std::exception& e) {
            INFO_RELEASE("ERROR: Failed to persist an input buffer as channel state: " << e.what());
            throw;
        } catch (...) {
            INFO_RELEASE("ERROR: Failed to persist an input buffer as channel state due to an unknown exception");
            throw;
        }
    }

    void AddInputData(
        std::vector<omnistream::Buffer*> knownBuffers, long checkpointId, const omnistream::InputChannelInfo& info)
    {
        if (!knownBuffers.empty()) {
            channelStateWriter_->AddInputData(
                checkpointId, info, ChannelStateWriter::sequenceNumberUnknown, knownBuffers);
        }
    }

    std::optional<int64_t> CheckForBarrier(Buffer* buffer)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        auto event = ParseEvent(buffer);

        if (event == nullptr) {
            return std::nullopt;
        }
        if (auto barrier = std::dynamic_pointer_cast<CheckpointBarrier>(event)) {
            int64_t barrierId = barrier->GetId();
            int64_t expectedBarrierId =
                (checkpointStatus_ == CheckpointStatus::COMPLETED) ? lastSeenBarrier_ + 1 : lastSeenBarrier_;

            if (barrierId >= expectedBarrierId) {
                LOG("found barrier" << barrierId);
                checkpointStatus_ = CheckpointStatus::BARRIER_RECEIVED;
                lastSeenBarrier_ = barrierId;
                return std::make_optional(lastSeenBarrier_);
            } else {
                LOG("ignoring barrier" << barrierId);
            }
        }

        return std::nullopt;
    }

    std::shared_ptr<AbstractEvent> ParseEvent(Buffer* buffer)
    {
        if (buffer->isBuffer()) {
            return nullptr;
        } else {
            std::shared_ptr<AbstractEvent> event = EventSerializer::fromBufferNotRecycle(buffer);
            // 重置buffer的读取位置
            buffer->SetReaderIndex(0);
            return event;
        }
    }

    bool HasBarrierReceived() const
    {
        return checkpointStatus_ == CheckpointStatus::BARRIER_RECEIVED;
    }

private:
    std::string CheckpointStatusToString() const
    {
        switch (checkpointStatus_) {
            case CheckpointStatus::COMPLETED: return "COMPLETED";
            case CheckpointStatus::BARRIER_PENDING: return "BARRIER_PENDING";
            case CheckpointStatus::BARRIER_RECEIVED: return "BARRIER_RECEIVED";
            default: return "UNKNOWN";
        }
    }

private:
    InputChannelInfo channelInfo_;
    CheckpointStatus checkpointStatus_ = CheckpointStatus::COMPLETED;
    long lastSeenBarrier_ = -1L;
    std::shared_ptr<ChannelStateWriter> channelStateWriter_;
    std::mutex mutex_;
};
} // namespace omnistream
#endif
