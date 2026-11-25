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

#ifndef FLINK_BENCHMARK_RECYCLABLE_H
#define FLINK_BENCHMARK_RECYCLABLE_H

#include <functional>
#include <memory>
#include "FlinkKafkaInternalProducer.h"

template <typename T>
class Recyclable {
public:
    Recyclable(T* object, std::function<void(T*)> recycler);
    ~Recyclable();

    T* GetObject();
    bool IsRecycled();
    void Close();

private:
    T* object;
    std::function<void(T*)> recycler;
};

#endif // FLINK_BENCHMARK_RECYCLABLE_H
