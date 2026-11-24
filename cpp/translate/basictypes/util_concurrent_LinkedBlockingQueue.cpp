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

#include "basictypes/util_concurrent_LinkedBlockingQueue.h"

LinkedBlockingQueue::LinkedBlockingQueue(size_t capacity) : capacity_(capacity)
{
}

void LinkedBlockingQueue::put(Object* element)
{
    element->getRefCount();
    std::unique_lock<std::mutex> lock(mtx_);
    if (capacity_ > 0 && queue_.size() >= capacity_) {
        not_full_.wait(lock, [this]() { return queue_.size() < capacity_; });
    }
    queue_.push_back(element);
    not_empty_.notify_one();
}

Object* LinkedBlockingQueue::take()
{
    std::unique_lock<std::mutex> lock(mtx_);
    while (queue_.empty()) {
        not_empty_.wait(lock);
    }
    Object* element = queue_.front();
    queue_.pop_front();
    not_full_.notify_one();
    return element;
}

int LinkedBlockingQueue::size()
{
    std::lock_guard<std::mutex> lock(mtx_);
    return int(queue_.size());
}

int LinkedBlockingQueue::drainTo(Collection& collection, int maxElements)
{
    std::lock_guard<std::mutex> lock(mtx_);
    int transferCount = std::min(maxElements, int(queue_.size()));
    for (int i = 0; i < transferCount; ++i) {
        collection.push_back(queue_.front());
        queue_.pop_front();
    }
    return transferCount;
}

void LinkedBlockingQueue::clear()
{
    std::lock_guard<std::mutex> lock(mtx_);
    queue_.clear();
}
