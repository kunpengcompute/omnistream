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

#include "basictypes/LongObjectPool.h"

LongObjectPool& LongObjectPool::getInstance()
{
    constexpr int POOL_SIZE = 16;
    thread_local static LongObjectPool instance(POOL_SIZE);
    return instance;
}

LongObjectPool::LongObjectPool(size_t poolSize)
{
    while (poolSize > 0) {
        Long* newLong = new Long();
        newLong->isPool = true;
        newLong->next = head;
        head = newLong;
        poolSize--;
    }
}

void LongObjectPool::capacityExpansion()
{
    size_t size = capacityExpansionNum;
    while (size > 0) {
        Long* newLong = new Long();
        newLong->isPool = true;
        newLong->next = head;
        head = newLong;
        size--;
    }
    this->capacityExpansionNum = (this->capacityExpansionNum << 1);
}

LongObjectPool::~LongObjectPool()
{
    while (head != nullptr) {
        Long* cur = head;
        head = head->next;
        delete cur;
    }
}
