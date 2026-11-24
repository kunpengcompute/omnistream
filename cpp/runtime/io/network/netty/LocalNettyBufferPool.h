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

#ifndef LOCAL_NETTY_BUFFER_POOL_H
#define LOCAL_NETTY_BUFFER_POOL_H

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "NettyBufferInfo.h"
#include "NettyMemorySegment.h"

namespace omnistream {

class GlobalNettyBufferPool;

/**
 * Per-task local NettyBuffer pool. Borrows buffers from GlobalNettyBufferPool.
 *
 * Follows the original Flink LocalBufferPool pattern:
 * - numberOfRequiredBuffers_: minimum guaranteed (immutable)
 * - currentPoolSize_: current assigned size (mutable via setNumBuffers from redistribution)
 * - maxNumberOfBuffers_: absolute max
 * - numberOfRequestedBuffers_: total borrowed from global (available + in-use)
 * - Recycle: if destroyed or hasExcessBuffers() → return to global; else → keep local
 */
class LocalNettyBufferPool : public std::enable_shared_from_this<LocalNettyBufferPool> {
public:
    LocalNettyBufferPool(GlobalNettyBufferPool* globalPool, int numberOfRequiredBuffers, int maxNumberOfBuffers);
    ~LocalNettyBufferPool();

    // --- Main API ---
    std::shared_ptr<NettyMemorySegment> requestBuffer();
    std::shared_ptr<NettyMemorySegment> requestBufferBlocking();
    std::shared_ptr<NettyMemorySegment> requestBigBuffer(int size);
    void recycleBuffer(long bufferAddress);
    void destroyCorruptBuffer(long bufferAddress);

    // --- Pool management (called by GlobalNettyBufferPool::redistributeBuffers) ---
    void setNumBuffers(int numBuffers);
    int getNumberOfRequiredBuffers() const;
    int getMaxNumberOfBuffers() const;
    int getCurrentPoolSize();
    int getNumberOfAvailableBuffers();
    int getNumberOfRequestedBuffers();
    int getNettyBufferSize();
    int getRequestRegularBufferCount();
    int getRequestBigBufferCount();
    int getRecycleRegularBufferCount();
    int getRecycleBigBufferCount();
    int getBigTotalMemorySize();
    int getAvailableBigMemorySize();
    int getActiveBigBufferCount();

    // --- Notification (called by GlobalNettyBufferPool when a buffer is recycled to global) ---
    void notifyBufferAvailable();

    // --- Lifecycle ---
    void lazyDestroy();
    bool isDestroyed();

private:
    bool requestBufferFromGlobal();
    bool isRequestedSizeReached();
    bool hasExcessBuffers();
    void recycleBuffersToGlobal(const std::vector<std::shared_ptr<NettyMemorySegment>>& buffers);
    std::vector<std::shared_ptr<NettyMemorySegment>> removeExcessBuffers();

    GlobalNettyBufferPool* globalPool_;

    std::recursive_mutex mutex_;
    std::condition_variable_any cv_;
    std::deque<std::shared_ptr<NettyMemorySegment>> availableBuffers_;
    // Address → NettyBufferInfo mapping for JNI recycle (address-based lookup)
    std::unordered_map<long, std::shared_ptr<NettyMemorySegment>> allTrackedBuffers_;

    int numberOfRequiredBuffers_;
    int currentPoolSize_;
    int maxNumberOfBuffers_;
    int numberOfRequestedBuffers_ = 0;
    int requestRegularBufferCount_ = 0;
    int requestBigBufferCount_ = 0;
    int recycleRegularBufferCount_ = 0;
    int recycleBigBufferCount_ = 0;

    // Big buffer memory management (managed entirely in local pool)
    int64_t bigTotalMemorySize_;        // = maxNumberOfBuffers * bufferSize
    int64_t availableBigMemorySize_;    // starts at bigTotalMemorySize_, decremented per request
    int activeBigBufferCount_ = 0;      // number of currently alive big buffers
    std::condition_variable_any bigCv_; // for blocking when big memory exhausted
    // Address → size mapping for big buffer recycle
    std::unordered_map<long, int64_t> bigBufferSizes_;

    bool isDestroyed_ = false;
};

} // namespace omnistream

#endif // LOCAL_NETTY_BUFFER_POOL_H
