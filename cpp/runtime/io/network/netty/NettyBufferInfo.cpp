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

#include "NettyBufferInfo.h"
#include <common.h>

namespace omnistream {
int NettyBufferInfo::elementNumBytes = 4; // Assuming 4 bytes for int
NettyBufferInfo::NettyBufferInfo(std::shared_ptr<NettyMemorySegment> nettyMemorySegment)
{
    nettyMemorySegment_ = nettyMemorySegment;
    originalAddress_ = nettyMemorySegment->GetOriginalAddress();
    dataAddress_ = originalAddress_ + nettyMemorySegment->GetUsedBytes();
    position = dataAddress_ + elementNumBytes;
    size_ = nettyMemorySegment->getAvailableBytes();
}

NettyBufferInfo::NettyBufferInfo(uint8_t* buffer, int capacity)
{
    originalAddress_ = buffer;
    dataAddress_ = buffer;
    position = dataAddress_ + elementNumBytes;
    size_ = capacity;
}

NettyBufferInfo::~NettyBufferInfo()
{
    // delete [] originalAddress_;
}

uint8_t*& NettyBufferInfo::GetPosition()
{
    return position;
}
uint8_t* NettyBufferInfo::GetOriginalAddress()
{
    return originalAddress_;
}
uint8_t* NettyBufferInfo::GetDataAddress()
{
    return dataAddress_;
}
int NettyBufferInfo::GetSize()
{
    return size_;
}

bool NettyBufferInfo::operator==(const NettyBufferInfo& other) const
{
    return dataAddress_ == other.dataAddress_;
}

int NettyBufferInfo::GetWrittenBytes() const
{
    return writtenBytes_;
}
void NettyBufferInfo::SetWrittenBytes(int bytes)
{
    writtenBytes_ += bytes;
}

void NettyBufferInfo::ResetBuffer()
{
    writtenBytes_ = 0;
    elementNum = 0;
    position = dataAddress_ + elementNumBytes;
}

bool NettyBufferInfo::Useable(int newSize)
{
    return newSize <= size_ - writtenBytes_ - elementNumBytes;
}

void NettyBufferInfo::IncrementElementNum()
{
    elementNum++;
}
int NettyBufferInfo::GetElementNum() const
{
    return elementNum;
}
void NettyBufferInfo::MarkElementNumWritten()
{
    writtenBytes_ += elementNumBytes;
    nettyMemorySegment_->IncreaseUsedBytes(writtenBytes_);
    nettyMemorySegment_->IncreaseRefCount();
}
} // namespace omnistream
