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

#ifndef OMNISTREAM_BUFFERMANAGER_H
#define OMNISTREAM_BUFFERMANAGER_H
#include <iostream>
#include <memory>
#include <mutex>
#include "InputChannel.h"
#include "runtime/buffer/ObjectSegmentProvider.h"
#include "runtime/buffer/Buffer.h"
#include "core/memory/Segment.h"
#include "core/include/common.h"
#include "runtime/buffer/BufferRecycler.h"
#include "runtime/buffer/NetworkMemoryBufferPool.h"
#include "runtime/buffer/NetworkBuffer.h"
#include "runtime/partition/consumer/SingleInputGate.h"
#include "runtime/buffer/BufferListener.h"

using namespace omnistream::datastream;
class AvailableBufferQueue {
public:
    void addFloatingBuffer(std::shared_ptr<omnistream::Buffer> buffer)
    {
        floatingBuffers.emplace_back(buffer);
    }

    void releaseAll(std::vector<Segment*>& exclusiveSegments)
    {
        std::shared_ptr<omnistream::Buffer> buffer;
        while (!floatingBuffers.empty()) {
            buffer = floatingBuffers.front();
            floatingBuffers.pop_front();
            buffer->RecycleBuffer();
        }
        while (!exclusiveBuffers.empty()) {
            buffer = exclusiveBuffers.front();
            exclusiveBuffers.pop_front();
            exclusiveSegments.emplace_back(buffer->GetSegment());
        }
    }

    std::shared_ptr<omnistream::Buffer> addExclusiveBuffer(
        std::shared_ptr<omnistream::Buffer> buffer, int numRequiredBuffers)
    {
        exclusiveBuffers.emplace_back(buffer);

        // 只有在“超额”且确实有 floating buffer 时，才回收一个 floating buffer
        if (getAvailableBufferSize() > numRequiredBuffers && !floatingBuffers.empty()) {
            auto buf = floatingBuffers.front();
            floatingBuffers.pop_front();
            return buf;
        }

        return nullptr;
    }

    std::shared_ptr<omnistream::Buffer> takeBuffer()
    {
        if (!floatingBuffers.empty()) {
            auto buffer = floatingBuffers.front();
            floatingBuffers.pop_front();
            return buffer;
        }

        if (!exclusiveBuffers.empty()) {
            auto buffer = exclusiveBuffers.front();
            exclusiveBuffers.pop_front();
            return buffer;
        }

        return nullptr;
    }

    int getAvailableBufferSize()
    {
        return floatingBuffers.size() + exclusiveBuffers.size();
    }

    std::deque<std::shared_ptr<omnistream::Buffer>> getFloatingBuffers()
    {
        return floatingBuffers;
    }

    std::deque<std::shared_ptr<omnistream::Buffer>> clearFloatingBuffers()
    {
        std::deque<std::shared_ptr<omnistream::Buffer>> buffers = std::move(floatingBuffers);
        floatingBuffers.clear();
        return buffers;
    }

    void notifyAll()
    {
        queueCondition.notify_all();
    }

    void wait()
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        queueCondition.wait(lock);
    }

private:
    std::deque<std::shared_ptr<omnistream::Buffer>> floatingBuffers;
    std::deque<std::shared_ptr<omnistream::Buffer>> exclusiveBuffers;
    std::mutex queueMutex;
    std::condition_variable queueCondition;
};

class BufferManager : public BufferListener,
                      public omnistream::BufferRecycler,
                      public std::enable_shared_from_this<BufferManager> {
public:
    BufferManager(
        std::shared_ptr<datastream::SegmentProvider> provider, omnistream::InputChannel* channel, int numBuffers)
        : inputChannel(channel),
          numRequiredBuffers(numBuffers),
          globalPool(provider)
    {
        bufferQueue = std::make_shared<AvailableBufferQueue>();
    }

    void releaseAllBuffers(std::deque<Buffer*> buffers);

    void requestExclusiveBuffers(int numExclusiveBuffers);

    std::shared_ptr<omnistream::Buffer> requestBufferBlocking();

    int unsynchronizedGetFloatingBuffersAvailable()
    {
        return bufferQueue->getFloatingBuffers().size();
    }

    bool shouldContinueRequest(std::shared_ptr<BufferPool> bufferPool)
    {
        if (bufferPool->addBufferListener(shared_from_this())) {
            isWaitingForFloatingBuffers = true;
            numRequiredBuffers = 1;
            return false;
        } else if (bufferPool->isDestroyed()) {
            throw std::runtime_error("Local buffer pool has already been released.");
        } else {
            return true;
        }
    }

    void releaseFloatingBuffers();

    [[nodiscard]] std::string toString() const override;

    void recycle(Segment* segment) override;

    bool notifyBufferAvailable(std::shared_ptr<Buffer> buffer) override;

    void notifyBufferDestroyed() override;

private:
    omnistream::InputChannel* inputChannel;
    int numRequiredBuffers;
    bool isWaitingForFloatingBuffers = false;
    std::shared_ptr<datastream::SegmentProvider> globalPool;
    std::shared_ptr<AvailableBufferQueue> bufferQueue;
    std::mutex bufferQueueLock;
};

#endif // OMNISTREAM_BUFFERMANAGER_H
