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

#ifndef READONLYSLICEDNETWORKBUFFER_H
#define READONLYSLICEDNETWORKBUFFER_H

#include "NetworkBuffer.h"
// check
namespace omnistream::datastream {
class ReadOnlySlicedNetworkBuffer : public NetworkBuffer {
public:
    ReadOnlySlicedNetworkBuffer(NetworkBuffer* parent, int index, int length)
        : NetworkBuffer(parent->getMemorySegment(), parent->GetRecycler()),
          parent_(parent),
          index_(index),
          length_(length),
          bufferType(parent->GetBufferType())
    {
        this->SetDataType(parent->GetDataType());
        SetSize(length);
        memorySegmentOffset = parent->GetMemorySegmentOffset() + index;
    }

    std::shared_ptr<BufferRecycler> GetRecycler() override
    {
        return parent_->GetRecycler();
    }

    void RecycleBuffer() override
    {
        if (parent_ == nullptr) {
            THROW_RUNTIME_ERROR("ReadOnlySlicedNetworkBuffer::RecycleBuffer() >>> parent_ is nullptr");
        }
        parent_->RecycleBuffer();
    }

    NetworkBuffer* GetNetWorkBuffer()
    {
        return parent_;
    }

    Buffer* RetainBuffer() override
    {
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

    int GetMemorySegmentOffset() const
    {
        return memorySegmentOffset;
    }

    void SetBufferType(int type)
    {
        bufferType = type;
    }

    int GetBufferType() const
    {
        return bufferType;
    }

private:
    NetworkBuffer* parent_ = nullptr;
    int index_;
    int length_;
    int bufferType = 0; // 0 for data buffer, 1 for event buffer
    int memorySegmentOffset;
};
} // namespace omnistream::datastream

#endif // READONLYSLICEDNETWORKBUFFER_H
