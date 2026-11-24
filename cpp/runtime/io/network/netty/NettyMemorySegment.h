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

#ifndef OMNISTREAM_NETTYSEGMENT_H
#define OMNISTREAM_NETTYSEGMENT_H

#include <cstdint>
#include <mutex>

namespace omnistream {
class NettyMemorySegment {
public:
    NettyMemorySegment(uint8_t* addr, int32_t capacity);
    ~NettyMemorySegment();
    uint8_t* GetOriginalAddress();
    int GetCapacity();
    bool operator==(const NettyMemorySegment& other) const;
    int GetUsedBytes() const;
    void IncreaseUsedBytes(int bytes);
    void ResetBuffer();
    int getAvailableBytes();
    void IncreaseRefCount();
    int DecreaseRefCount();
    int GetRefCount();
    void EnableEligibleRecycling();
    bool GetEligibleRecycling();

private:
    volatile int usedBytes = 0;
    uint8_t* originalAddress_;
    volatile int capacity;
    volatile int refCount = 0;
    volatile bool eligibleForRecycling = false;
    std::recursive_mutex countMutex;
    std::recursive_mutex eligibleMutex;
};
} // namespace omnistream

#endif // OMNISTREAM_NETTYSEGMENT_H
