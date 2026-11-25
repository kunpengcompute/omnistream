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

#ifndef READONLYSLICEDVECTORBATCHBUFFER_H
#define READONLYSLICEDVECTORBATCHBUFFER_H

#include "ObjectBuffer.h"
#include "ObjectSegment.h"
#include "VectorBatchBuffer.h"

namespace omnistream {
class ReadOnlySlicedVectorBatchBuffer : public VectorBatchBuffer {
public:
    ReadOnlySlicedVectorBatchBuffer(VectorBatchBuffer* parent, int index, int length)
        : VectorBatchBuffer(parent->GetObjectSegment(), parent->GetRecycler()),
          parent_(parent),
          index_(index),
          length_(length)
    {
        SetSize(length);
    }

    ~ReadOnlySlicedVectorBatchBuffer() override
    {
        // delete parent_;
    }

    std::shared_ptr<BufferRecycler> GetRecycler() override
    {
        return parent_->GetRecycler();
    }

    void RecycleBuffer() override
    {
        if (parent_ == nullptr) {
            THROW_LOGIC_EXCEPTION("ReadOnlySlicedVectorBatchBuffer::RecycleBuffer(), parent_ is nullptr");
        }
        if (recycled_.exchange(true, std::memory_order_acq_rel)) {
            THROW_LOGIC_EXCEPTION("ReadOnlySlicedVectorBatchBuffer::RecycleBuffer() called twice");
        }
        LOG_TRACE("Calling RecycleBuffer() from ReadOnlySlicedVectorBatchBuffer");
        parent_->RecycleBytes(byteToRecycle);
        LOG_TRACE("ReadOnlySlicedVectorBatchBuffer bytesToRecycle = " << byteToRecycle);
        parent_->RecycleBuffer();
    }

    Buffer* RetainBuffer() override
    {
        LOG_TRACE("Calling RetainBuffer() from ReadOnlySlicedVectorBatchBuffer");
        return parent_->RetainBuffer();
    }

    Buffer* ReadOnlySlice() override
    {
        throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
    }

    Buffer* ReadOnlySlice(int index, int length) override
    {
        throw std::runtime_error("ReadOnlySlicedVectorBatchBuffer does not support ReadOnlySlice");
    }

    int GetMaxCapacity() const override
    {
        return parent_->GetMaxCapacity();
    }

    int GetReaderIndex() const override
    {
        return parent_->GetReaderIndex() + index_;
    }

    int GetOffset() const override
    {
        return index_;
    }

    void SetByteToRecycle(int64_t bytes)
    {
        byteToRecycle = bytes;
    }

private:
    VectorBatchBuffer* parent_ = nullptr;
    int index_;
    int length_;
    int64_t byteToRecycle = 0;
    std::atomic<bool> recycled_{false};
};
} // namespace omnistream

#endif
