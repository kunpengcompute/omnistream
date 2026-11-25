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

#ifndef OMNISTREAM_EXECUTORSERVICE_H
#define OMNISTREAM_EXECUTORSERVICE_H
#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <functional>
#include <condition_variable>
#include <future>
#include <iostream>
#include <atomic>
class ExecutorService {
public:
    explicit ExecutorService(size_t numThreads);
    ~ExecutorService();

    void Execute(std::function<void()> task);

protected:
    void WorkerLoop();

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queueMutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};
#endif // OMNISTREAM_EXECUTORSERVICE_H
