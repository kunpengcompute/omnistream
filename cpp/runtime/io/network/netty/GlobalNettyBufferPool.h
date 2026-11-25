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

#ifndef GLOBAL_NETTY_BUFFER_POOL_H
#define GLOBAL_NETTY_BUFFER_POOL_H

#include <cstdint>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <vector>

#include "NettyBufferConf.h"
#include "NettyBufferInfo.h"
#include "NettyMemorySegment.h"
#include "runtime/metrics/groups/GlobalNettyBufferMetricGroup.h"

namespace omnistream {

class LocalNettyBufferPool;

/**
 * Global NettyBuffer pool held by the TaskManager.
 * Pre-allocates an initial set of NettyBuffers at startup. Local pools borrow/return buffers from/to this pool.
 *
 * Follows the original Flink NetworkBufferPool pattern:
 * - Two separate locks (factoryLock_ and availableBuffersMutex_) to avoid deadlock
 * - redistributeBuffers() dynamically adjusts local pool sizes
 * - condition_variable for blocking requests
 */
class GlobalNettyBufferPool {
public:
    explicit GlobalNettyBufferPool(const NettyBufferConf& conf);
    ~GlobalNettyBufferPool();

    // --- Pooled buffer request/recycle (called by LocalNettyBufferPool) ---
    // These use availableBuffersMutex_ only (no factoryLock_)
    std::shared_ptr<NettyMemorySegment> requestPooledBuffer();
    void recyclePooledBuffer(std::shared_ptr<NettyMemorySegment> buffer);
    std::vector<std::shared_ptr<NettyMemorySegment>> requestPooledBuffersBlocking(int count);

    // --- Big buffer support ---
    std::shared_ptr<NettyMemorySegment> allocateBigBuffer(int size);
    void recycleBigBuffer(long bufferAddress);

    // --- Local pool factory (under factoryLock_) ---
    std::shared_ptr<LocalNettyBufferPool> createLocalPool(int numOfSubPartition);
    void destroyLocalPool(std::shared_ptr<LocalNettyBufferPool> pool);

    // --- Metrics ---
    int getTotalBufferCount() const;
    int getAvailableBufferCount();
    int getUsedBufferCount();
    int getBufferSize() const;
    GlobalNettyBufferMetricGroup::SizeSupplierFactory CreateGlobalNettyBufferMetricSupplierFactory();

    void destroy();
    bool isDestroyed() const;

private:
    void redistributeBuffers();
    std::shared_ptr<NettyMemorySegment> allocateRegularBuffer();

    NettyBufferConf conf_;

    // Two separate locks following Flink's deadlock-free pattern
    std::recursive_mutex factoryLock_;
    std::recursive_mutex availableBuffersMutex_;
    std::condition_variable_any cv_;

    std::deque<std::shared_ptr<NettyMemorySegment>> availableBuffers_;
    int totalNumberOfBuffers_;
    int allocatedRegularBufferCount_ = 0;
    bool isDestroyed_ = false;

    // Local pool management (under factoryLock_)
    std::set<std::shared_ptr<LocalNettyBufferPool>> allLocalPools_;
    std::set<std::shared_ptr<LocalNettyBufferPool>> resizableLocalPools_;
    int numTotalRequiredBuffers_ = 0;
    int createdLocalPoolCount_ = 0;
    int destroyedLocalBufferCount_ = 0;

    // Big buffer tracking
    std::recursive_mutex bigBufferMutex_;
    std::unordered_map<long, std::shared_ptr<NettyMemorySegment>> activeBigBuffers_;
};

} // namespace omnistream

#endif // GLOBAL_NETTY_BUFFER_POOL_H
