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

#ifndef OMNISTREAM_OBJECTPOOL_H
#define OMNISTREAM_OBJECTPOOL_H

#include <iostream>

template <typename T>
class ObjectPool {
public:
    static ObjectPool<T>* getInstance();

    void capacityExpansion();

    T* getObject();

    T* head;

    ~ObjectPool();

    ObjectPool(const ObjectPool&) = default;

private:
    explicit ObjectPool(size_t poolSize);

    ObjectPool& operator=(const ObjectPool&) = delete;

    int32_t capacityExpansionNum = 16;
};
#endif // OMNISTREAM_OBJECTPOOL_H
