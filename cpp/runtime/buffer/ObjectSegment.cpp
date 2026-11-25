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

#include "ObjectSegment.h"

#include <algorithm>
#include <atomic>

#include "core/include/common.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {

namespace {
std::atomic<long> g_segCreated{0};
std::atomic<long> g_segDestroyed{0};
std::atomic<long> g_stored{0};  // putObject calls
std::atomic<long> g_drained{0}; // elements actually taken by a consumer
} // namespace

void ObjectSegment::countDrained()
{
    g_drained.fetch_add(1, std::memory_order_relaxed);
}

void ObjectSegment::countCreated()
{
    g_segCreated.fetch_add(1, std::memory_order_relaxed);
}

void ObjectSegment::countDestroyed()
{
    g_segDestroyed.fetch_add(1, std::memory_order_relaxed);
}

void ObjectSegment::countStored()
{
    g_stored.fetch_add(1, std::memory_order_relaxed);
}

void ObjectSegment::reportCounters(const char* where)
{
    long stored = g_stored.load();
    long drained = g_drained.load();
    INFO_RELEASE(
        "OBJSEG_COUNTERS[" << where << "] created=" << g_segCreated.load() << " destroyed=" << g_segDestroyed.load()
                           << " stored=" << stored << " drained=" << drained << " undrained=" << (stored - drained));
}

int64_t ObjectSegment::getObjectSizeInBytes() const
{
    return sizeInBytes_;
}

void ObjectSegment::reset()
{
    sizeInBytes_ = 0;
    capacityInBytes_ = 0;
    // reset elements in objects
    std::fill(objects_, objects_ + size, nullptr);
}

int64_t ObjectSegment::calculateStoredObjectSizeInBytes(const StreamElement* record)
{
    if (record->getTag() == StreamElementTag::TAG_STREAM_STATUS) {
        return sizeof(int32_t);
    }
    // A watermark is not a StreamRecord and its getValue() is not a VectorBatch, so it must be
    // handled before the cast below. It carries only a timestamp, so account a fixed size for it.
    if (record->getTag() == StreamElementTag::TAG_WATERMARK) {
        return static_cast<int64_t>(sizeof(int64_t));
    }
    // ObjectSegment is used by the vector-batch transport path, so only StreamRecord payloads
    // contribute bytes here and they are expected to be VectorBatch instances.
    auto* vectorBatch = static_cast<const VectorBatch*>(record->getValue());
    return vectorBatch == nullptr ? 0 : vectorBatch->getSizeInBytes();
}

void ObjectSegment::setCapacity(int64_t capacity)
{
    capacityInBytes_ = capacity;
}

int64_t ObjectSegment::getCapacity()
{
    return capacityInBytes_;
}

} // namespace omnistream
