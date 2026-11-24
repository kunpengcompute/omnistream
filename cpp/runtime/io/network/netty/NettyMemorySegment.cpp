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

#include "NettyMemorySegment.h"

namespace omnistream {
NettyMemorySegment::NettyMemorySegment(uint8_t* addr, int32_t capacity) : originalAddress_(addr), capacity(capacity)
{
}

NettyMemorySegment::~NettyMemorySegment()
{
    delete[] originalAddress_;
}

bool NettyMemorySegment::operator==(const NettyMemorySegment& other) const
{
    return originalAddress_ == other.originalAddress_;
}

uint8_t* NettyMemorySegment::GetOriginalAddress()
{
    return originalAddress_;
}

int NettyMemorySegment::GetCapacity()
{
    return capacity;
}

int NettyMemorySegment::GetUsedBytes() const
{
    return usedBytes;
}

void NettyMemorySegment::IncreaseUsedBytes(int bytes)
{
    usedBytes += bytes;
}

int NettyMemorySegment::getAvailableBytes()
{
    return capacity - usedBytes;
}

void NettyMemorySegment::ResetBuffer()
{
    usedBytes = 0;
    refCount = 0;
    eligibleForRecycling = false;
}

void NettyMemorySegment::IncreaseRefCount()
{
    std::lock_guard<std::recursive_mutex> lock(countMutex);
    refCount++;
}

int NettyMemorySegment::DecreaseRefCount()
{
    std::lock_guard<std::recursive_mutex> lock(countMutex);
    refCount--;
    return refCount;
}

int NettyMemorySegment::GetRefCount()
{
    std::lock_guard<std::recursive_mutex> lock(countMutex);
    return refCount;
}

void NettyMemorySegment::EnableEligibleRecycling()
{
    std::lock_guard<std::recursive_mutex> lock(eligibleMutex);
    eligibleForRecycling = true;
}

bool NettyMemorySegment::GetEligibleRecycling()
{
    std::lock_guard<std::recursive_mutex> lock(eligibleMutex);
    return eligibleForRecycling;
}
} // namespace omnistream
