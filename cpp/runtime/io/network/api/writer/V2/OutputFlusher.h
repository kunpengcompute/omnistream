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

#ifndef OUTPUTFLUSHER_H
#define OUTPUTFLUSHER_H

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <exception>
#include <string>
#include <vector>

namespace omnistream {
class RecordWriterV2;

class OutputFlusher {
public:
    OutputFlusher(const std::string& name, long timeout, RecordWriterV2* writer);
    ~OutputFlusher();

    void terminate();
    void run();
    void start();

private:
    long timeout;
    RecordWriterV2* writer;
    std::thread thread;
    // Serialize start and stop, including the whole join, across cancel and cleanup callers.
    std::mutex lifecycleMutex;
    // Written by the task thread in terminate(), read by the flusher thread in run(). Must be
    // atomic: as a plain bool the read can be hoisted out of the loop, and terminate() would then
    // block forever in join().
    std::atomic<bool> running;

    std::string taskName;

    // trouble shooting
    long counter = 0;

    void notifyFlusherException(const std::exception_ptr& e);
};
} // namespace omnistream

#endif // OUTPUTFLUSHER_H
