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

#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_EXECUTOR_IMPL_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_EXECUTOR_IMPL_H

#include <deque>
#include <map>
#include <queue>
#include <set>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <functional>
#include <memory>
#include "ChannelStateWriteRequestExecutor.h"
#include "ChannelStateWriteRequestDispatcher.h"
#include "ChannelStateWriteRequest.h"
#include "runtime/jobgraph/JobVertexID.h"

namespace omnistream {

class ChannelStateWriteRequestExecutorImpl : public ChannelStateWriteRequestExecutor {
public:
    ChannelStateWriteRequestExecutorImpl(std::shared_ptr<ChannelStateWriteRequestDispatcher> dispatcher);

    ~ChannelStateWriteRequestExecutorImpl() override;

    void start() override;
    void submit(std::shared_ptr<ChannelStateWriteRequest> req) override;
    void submitPriority(std::shared_ptr<ChannelStateWriteRequest> req) override;
    void registerSubtask(const JobVertexID& jvid, int idx) override;
    void releaseSubtask(const JobVertexID& jvid, int idx) override;
    void shutdown() override;

private:
    std::shared_ptr<ChannelStateWriteRequestDispatcher> dispatcher;
    const int maxSubtasks;
    std::function<void(ChannelStateWriteRequestExecutor*)> onRegistered;
    std::mutex& registerLock;

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<std::shared_ptr<ChannelStateWriteRequest>> readyQueue;
    std::map<SubtaskID, std::queue<std::shared_ptr<ChannelStateWriteRequest>>> unreadyQueues;
    std::set<SubtaskID> subtasks;

    std::atomic<bool> isRegistering;
    std::atomic<bool> stopped{false};
    std::atomic<bool> started{false};
    std::thread worker;
    std::exception_ptr exceptionPtr;

    void run();
    void loop();
    std::shared_ptr<ChannelStateWriteRequest> take();
    void enqueue(std::shared_ptr<ChannelStateWriteRequest> req, bool priority);
    void registerCallback(std::shared_ptr<ChannelStateWriteRequest> first, SubtaskID sid);
    void cleanup();
    void completeRegistration();
};

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_EXECUTOR_IMPL_H
