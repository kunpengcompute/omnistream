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

#ifndef OMNISTREAM_MOCKINPUTGATE_H
#define OMNISTREAM_MOCKINPUTGATE_H
#pragma once

#include "runtime/partition/consumer/InputGate.h"
#include "runtime/partition/consumer/IndexedInputGate.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/buffer/ObjectSegmentProvider.h"
#include "runtime/buffer/ObjectBufferPool.h"
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "core/utils/lang/AutoCloseable.h"
#include "core/include/common.h"

namespace omnistream {

class MockInputGate : public IndexedInputGate, public AutoCloseable {
public:
    explicit MockInputGate(int gateIndex)
        : gateIndex_(gateIndex),
          stateConsumedFuture_(std::make_shared<CompletableFutureV2<void>>()),
          closeFuture_(std::make_shared<CompletableFuture>()),
          buffersInUseCount_(0),
          consumedPartitionType_(0),
          isFinished_(false),
          hasReceivedEndOfData_(false),
          bufferPool_(nullptr),
          segmentProvider_(nullptr)
    {
    }

    void setup() override
    {
        LOG("MockInputGate::setup called for gateIndex=" << gateIndex_);
    }

    std::shared_ptr<CompletableFutureV2<void>> getStateConsumedFuture() override
    {
        return stateConsumedFuture_;
    }

    std::vector<bool> getStateConsumedFuture1() override
    {
        return {};
    }

    void RequestPartitions() override
    {
        LOG("MockInputGate::RequestPartitions called");
        partitionsRequested_ = true;
    }

    void FinishReadRecoveredState() override
    {
        LOG("MockInputGate::FinishReadRecoveredState called");
    }

    int GetNumberOfInputChannels() override
    {
        return 1;
    }

    int GetGateIndex() override
    {
        return gateIndex_;
    }

    std::vector<InputChannelInfo> getUnfinishedChannels() override
    {
        return {};
    }

    int getBuffersInUseCount() override
    {
        return buffersInUseCount_;
    }

    void announceBufferSize(int newBufferSize) override
    {
        bufferSize_ = newBufferSize;
    }

    std::shared_ptr<InputChannel> getChannel(int channelIndex) override
    {
        return nullptr;
    }

    std::vector<InputChannelInfo> GetChannelInfos() override
    {
        std::vector<InputChannelInfo> infos;
        for (int i = 0; i < GetNumberOfInputChannels(); ++i) {
            auto inputChannelInfo = new InputChannelInfo(gateIndex_, i);
            infos.emplace_back(*inputChannelInfo);
        }
        return infos;
    }

    bool IsFinished() override
    {
        return isFinished_;
    }

    bool HasReceivedEndOfData() override
    {
        return hasReceivedEndOfData_;
    }

    BufferOrEvent* GetNext() override
    {
        return nullptr;
    }

    BufferOrEvent* PollNext() override
    {
        return nullptr;
    }

    std::optional<std::shared_ptr<BufferOrEvent>> getNextBufferOrEvent(bool /*blocking*/)
    {
        return std::nullopt;
    }

    void sendTaskEvent(const std::shared_ptr<TaskEvent>& /*event*/) override
    {
    }
    void ResumeConsumption(const InputChannelInfo& /*channelInfo*/) override
    {
    }
    void acknowledgeAllRecordsProcessed(const InputChannelInfo& /*channelInfo*/) override
    {
    }

    void close() override
    {
        LOG("MockInputGate::close called");
        isClosed_ = true;
        closeFuture_.reset();
    }

    std::shared_ptr<CompletableFuture> getCloseFuture()
    {
        return closeFuture_;
    }

    std::string toString() override
    {
        return "MockInputGate[" + std::to_string(gateIndex_) + "]";
    }

    bool partitionsRequested() const
    {
        return partitionsRequested_;
    }

    void setConsumedPartitionType(int type)
    {
        consumedPartitionType_ = type;
    }

    int getConsumedPartitionType()
    {
        return consumedPartitionType_;
    }

    std::shared_ptr<ObjectBufferPool> getBufferPool()
    {
        return bufferPool_;
    }

    std::shared_ptr<ObjectSegmentProvider> getMemorySegmentProvider()
    {
        return segmentProvider_;
    }

    void setBufferPool(std::shared_ptr<ObjectBufferPool> bufferPool)
    {
        bufferPool_ = bufferPool;
    }

private:
    int gateIndex_;
    bool partitionsRequested_ = false;
    bool isClosed_ = false;
    bool isFinished_;
    bool hasReceivedEndOfData_;
    int buffersInUseCount_;
    int bufferSize_;
    int consumedPartitionType_;

    std::shared_ptr<CompletableFutureV2<void>> stateConsumedFuture_;
    std::shared_ptr<CompletableFuture> closeFuture_;

    std::shared_ptr<ObjectBufferPool> bufferPool_;
    std::shared_ptr<ObjectSegmentProvider> segmentProvider_;
};

} // namespace omnistream
#endif // OMNISTREAM_MOCKINPUTGATE_H
