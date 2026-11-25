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

#ifndef OMNISTREAM_LONGOBJECTPOOL_H
#define OMNISTREAM_LONGOBJECTPOOL_H
#include "Long.h"

class LongObjectPool {
public:
    Long* head;
    static LongObjectPool& getInstance();

    int32_t capacityExpansionNum = 16;

public:
    void capacityExpansion();

private:
    explicit LongObjectPool(size_t poolSize);
    ~LongObjectPool();

    LongObjectPool(const LongObjectPool&) = delete;
    LongObjectPool& operator=(const LongObjectPool&) = delete;
};
#endif // OMNISTREAM_LONGOBJECTPOOL_H
