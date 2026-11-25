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

#include "BufferManager.h"
#include "vector"

void BufferManager::releaseAllBuffers(std::deque<Buffer*> buffers)
{
    std::vector<Segment*> exclusiveRecyclingSegments;
    Buffer* buffer;
    while (!buffers.empty()) {
        buffer = buffers.front();
        buffers.pop_front();
        if (buffer == nullptr) {
            continue;
        }
        try {
            if (buffer->GetRecycler().get() == this) {
                exclusiveRecyclingSegments.emplace_back(buffer->GetSegment());
            } else {
                buffer->RecycleBuffer();
                delete buffer;
            }
        } catch (const std::exception& e) {
            LOG("recycle exception!" << e.what());
            throw std::runtime_error("recycle exception!");
        }
    }
    {
        try {
            std::lock_guard<std::mutex> lock(bufferQueueLock);
            bufferQueue->releaseAll(exclusiveRecyclingSegments);
            bufferQueue->notifyAll();
        } catch (const std::exception& e) {
            LOG("bufferQueue recycle exception!" << e.what());
            throw std::runtime_error("bufferQueue recycle exception!");
        }
    }

    try {
        if (!exclusiveRecyclingSegments.empty()) {
            std::vector<MemorySegment*> memorySegments;
            memorySegments.reserve(exclusiveRecyclingSegments.size());

            for (Segment* segment : exclusiveRecyclingSegments) {
                auto memorySegment = (MemorySegment*)(segment);
                if (memorySegment != nullptr) {
                    memorySegments.push_back(memorySegment);
                }
            }
            globalPool->recycleUnpooledMemorySegments(memorySegments);
        }
    } catch (const std::exception& e) {
        LOG("globalPool recycle exception!" << e.what());
        throw std::runtime_error("globalPool recycle exception!");
    }
}

void BufferManager::requestExclusiveBuffers(int numExclusiveBuffers)
{
    if (numExclusiveBuffers < 0) {
        LOG("invalid arg numExclusiveBuffers");
        throw std::invalid_argument("invalid arg numExclusiveBuffers");
    }
    if (numExclusiveBuffers == 0) {
        return;
    }

    std::vector<MemorySegment*> segments = globalPool->requestUnpooledMemorySegments(numExclusiveBuffers);

    std::vector<std::shared_ptr<omnistream::Buffer>> toRecycle;

    {
        std::lock_guard<std::mutex> bufLock(bufferQueueLock);

        // 恢复期至少需要保留这么多 required buffers
        numRequiredBuffers = std::max(numRequiredBuffers, numExclusiveBuffers);

        for (const auto& item : segments) {
            auto recycled = bufferQueue->addExclusiveBuffer(
                std::make_shared<datastream::NetworkBuffer>(item, shared_from_this()), numRequiredBuffers);
            if (recycled) {
                toRecycle.push_back(recycled);
            }
        }
    }

    for (auto& buffer : toRecycle) {
        buffer->RecycleBuffer();
    }
}

#include <thread>

std::shared_ptr<omnistream::Buffer> BufferManager::requestBufferBlocking()
{
    //    while (true) {
    //        {
    std::lock_guard<std::mutex> bufLock(bufferQueueLock);
    auto buffer = bufferQueue->takeBuffer();
    if (buffer != nullptr) {
        return buffer;
    }

    if (inputChannel->isReleased()) {
        LOG("Input channel [" << inputChannel->getChannelInfo().toString() << "] has already been released");
        throw std::runtime_error("channel has already been released!");
    }

    if (!isWaitingForFloatingBuffers) {
        std::shared_ptr<SingleInputGate> inputGate = inputChannel->getInputGate();
        std::shared_ptr<BufferPool> bufferPool = inputGate->getBufferPool();

        buffer = bufferPool->requestBuffer();
        if (buffer != nullptr) {
            return buffer;
        }

        shouldContinueRequest(bufferPool);
    }
    //        }
    //        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    //    }
}

void BufferManager::releaseFloatingBuffers()
{
    std::deque<std::shared_ptr<omnistream::Buffer>> buffers;
    {
        std::lock_guard<std::mutex> bufLock(bufferQueueLock);
        numRequiredBuffers = 0;
        buffers = bufferQueue->clearFloatingBuffers();
    }
    while (!buffers.empty()) {
        std::shared_ptr<omnistream::Buffer> buffer = buffers.front();
        buffers.pop_front();
        buffer->RecycleBuffer();
    }
}

std::string BufferManager::toString() const
{
    return "RecoveredInputChannel BufferManager";
}

void BufferManager::recycle(Segment* segment)
{
    if (segment == nullptr) {
        return;
    }

    auto memorySegment = static_cast<MemorySegment*>(segment);
    std::shared_ptr<omnistream::Buffer> recycled;
    {
        std::lock_guard<std::mutex> bufLock(bufferQueueLock);
        recycled = bufferQueue->addExclusiveBuffer(
            std::make_shared<datastream::NetworkBuffer>(memorySegment, shared_from_this()), numRequiredBuffers);
        isWaitingForFloatingBuffers = false;
        bufferQueue->notifyAll();
    }

    if (recycled) {
        recycled->RecycleBuffer();
    }
}

bool BufferManager::notifyBufferAvailable(std::shared_ptr<Buffer> buffer)
{
    if (buffer == nullptr) {
        return false;
    }

    std::lock_guard<std::mutex> bufLock(bufferQueueLock);
    isWaitingForFloatingBuffers = false;
    bufferQueue->addFloatingBuffer(buffer);
    bufferQueue->notifyAll();
    return true;
}

void BufferManager::notifyBufferDestroyed()
{
    std::lock_guard<std::mutex> bufLock(bufferQueueLock);
    isWaitingForFloatingBuffers = false;
    bufferQueue->notifyAll();
}
