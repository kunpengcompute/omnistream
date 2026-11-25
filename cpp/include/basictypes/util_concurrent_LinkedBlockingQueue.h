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

#ifndef OMNISTREAM_JAVA_UTIL_CONCURRENT_LINKEDBLOCKINGQUEUE_H
#define OMNISTREAM_JAVA_UTIL_CONCURRENT_LINKEDBLOCKINGQUEUE_H
#include <queue>
#include <mutex>
#include <vector>
#include <condition_variable>
#include <algorithm>

#include "java_util_Collection.h"
#include "Object.h"
#include "String.h"

class LinkedBlockingQueue : public Object {
public:
    explicit LinkedBlockingQueue(size_t capacity = 0);
    ~LinkedBlockingQueue() {};

    void put(Object* element);

    Object* take();

    int size();

    int drainTo(Collection& collection, int maxElements);

    void clear();

private:
    std::deque<Object*> queue_;
    std::mutex mtx_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    size_t capacity_;
};
#endif // OMNISTREAM_JAVA_UTIL_CONCURRENT_LINKEDBLOCKINGQUEUE_H
