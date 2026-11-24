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

#include "VectorBatchBuffer.h"

#include <algorithm>

#include "LocalObjectBufferPool.h"
#include "ReadOnlySlicedVectorBatchBuffer.h"

namespace omnistream {
VectorBatchBuffer::VectorBatchBuffer(ObjectSegment* segment, std::shared_ptr<BufferRecycler> recycle)
{
    bufferType = 0;
    if (segment == nullptr) {
        throw std::runtime_error("segment is null");
    }
    objectSegment = segment;

    if (recycle == nullptr) {
        throw std::runtime_error("recycler is null");
    }
    this->recycler = recycle;

    // Invoking this constructor implies that the caller (bufferBuilder) owns the segment
    refCount_.store(1, std::memory_order_relaxed);
    readerIndex_ = -1;
    event_type = -1;
    isCompressed_ = false;
}

void VectorBatchBuffer::RecycleBuffer()
{
    recycleBuffer(true);
}

void VectorBatchBuffer::recycleBuffer(bool selfDelete)
{
    // Data buffers have a recycler; event buffers do not.
    if (recycler == nullptr) {
        return;
    }

    const int previous = refCount_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous <= 0) {
        refCount_.fetch_add(1, std::memory_order_relaxed);
        throw std::runtime_error("Trying to recycle a VectorBatchBuffer that has already been recycled");
    }
    LOG_PART("The buffer " << this << " refCount is decremented from " << previous << " to " << (previous - 1));
    if (previous != 1) {
        return;
    }

    LOG_PART("VectorBatch Buffer recycled " << this);
    isRecycled_.store(true, std::memory_order_release);
    recycleRemainingBytes();
    recycler->recycle(GetObjectSegment());
    if (selfDelete) {
        delete this;
    }
}

void VectorBatchBuffer::RecycleBytes(int64_t bytes)
{
    if (bytes <= 0) {
        return;
    }

    auto localRecycler = std::dynamic_pointer_cast<LocalObjectBufferPool::SubpartitionBufferRecycler>(recycler);
    if (localRecycler == nullptr) {
        return;
    }

    accountedBytes_.fetch_add(bytes, std::memory_order_relaxed);
    localRecycler->recycleBytes(bytes);
}

void VectorBatchBuffer::recycleRemainingBytes()
{
    auto localRecycler = std::dynamic_pointer_cast<LocalObjectBufferPool::SubpartitionBufferRecycler>(recycler);
    if (localRecycler == nullptr || objectSegment == nullptr) {
        return;
    }

    const int64_t totalBytes = objectSegment->getObjectSizeInBytes();
    const int64_t returnedBytes = accountedBytes_.load(std::memory_order_relaxed);
    const int64_t bytesToRecycle = std::max<int64_t>(0, totalBytes - returnedBytes);

    if (bytesToRecycle > 0) {
        localRecycler->recycleBytes(bytesToRecycle);
    }
}

std::shared_ptr<BufferRecycler> VectorBatchBuffer::GetRecycler()
{
    return recycler;
}

ObjectSegment* VectorBatchBuffer::GetObjectSegment()
{
    return objectSegment;
}

Buffer* VectorBatchBuffer::ReadOnlySlice(int index, int length)
{
    if (bufferType == 0) {
        LOG_TRACE("Beginning VectorBatchBuffer ");
        auto sliceBuffer = new ReadOnlySlicedVectorBatchBuffer(this, index, length);
        int64_t bytesToRecycle = 0;
        int from = index;
        int to = index + length;
        for (int i = from; i < to; ++i) {
            bytesToRecycle += ObjectSegment::calculateStoredObjectSizeInBytes(objectSegment->getObject(i));
        }
        sliceBuffer->SetByteToRecycle(bytesToRecycle);
        return sliceBuffer;
    } else {
        LOG_TRACE("Event Buffer  ");
        return new VectorBatchBuffer(event_type);
    }
}

} // namespace omnistream
