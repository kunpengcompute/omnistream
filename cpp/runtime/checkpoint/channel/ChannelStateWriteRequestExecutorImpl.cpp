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

#include "ChannelStateWriteRequestExecutorImpl.h"

namespace omnistream {

ChannelStateWriteRequestExecutorImpl::ChannelStateWriteRequestExecutorImpl(
    std::shared_ptr<ChannelStateWriteRequestDispatcher> dispatcher)
    : dispatcher(dispatcher)
{
}

ChannelStateWriteRequestExecutorImpl::~ChannelStateWriteRequestExecutorImpl()
{
    shutdown();
    if (worker.joinable()) {
        worker.join();
    }
}

void ChannelStateWriteRequestExecutorImpl::start()
{
    if (!started.exchange(true)) {
        worker = std::thread([this] { run(); });
    }
}

void ChannelStateWriteRequestExecutorImpl::submit(std::shared_ptr<ChannelStateWriteRequest> req)
{
    std::exception_ptr rejectionCause;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!stopped) {
            enqueue(req, false);
            return;
        }
        rejectionCause = exceptionPtr ? exceptionPtr : std::make_exception_ptr(std::runtime_error("executor stopped"));
    }
    req->cancel(rejectionCause);
}

void ChannelStateWriteRequestExecutorImpl::submitPriority(std::shared_ptr<ChannelStateWriteRequest> req)
{
    std::exception_ptr rejectionCause;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!stopped) {
            enqueue(req, true);
            return;
        }
        rejectionCause = exceptionPtr ? exceptionPtr : std::make_exception_ptr(std::runtime_error("executor stopped"));
    }
    req->cancel(rejectionCause);
}

void ChannelStateWriteRequestExecutorImpl::registerSubtask(const JobVertexID& jvid, int idx)
{
    std::lock_guard<std::mutex> regLock(registerLock);
    std::lock_guard<std::mutex> lock(mutex);
    if (!isRegistering || stopped) {
        throw std::logic_error("registration already completed or executor stopped");
    }

    SubtaskID sid = SubtaskID::Of(jvid, idx);
    subtasks.insert(sid);
    unreadyQueues.emplace(sid, std::queue<std::shared_ptr<ChannelStateWriteRequest>>());

    enqueue(ChannelStateWriteRequest::registerSubtask(jvid, idx), false);

    if ((int)subtasks.size() >= maxSubtasks) {
        completeRegistration();
    }
}

void ChannelStateWriteRequestExecutorImpl::releaseSubtask(const JobVertexID& jvid, int idx)
{
    std::lock_guard<std::mutex> regLock(registerLock);
    std::lock_guard<std::mutex> lock(mutex);
    SubtaskID sid = SubtaskID::Of(jvid, idx);

    enqueue(ChannelStateWriteRequest::releaseSubtask(jvid, idx), false);
    subtasks.erase(sid);

    if (subtasks.empty()) {
        stopped = true;
    }
    cv.notify_all();
}

void ChannelStateWriteRequestExecutorImpl::shutdown()
{
    std::lock_guard<std::mutex> lock(mutex);
    stopped = true;
    cv.notify_all();
}

void ChannelStateWriteRequestExecutorImpl::run()
{
    try {
        loop();
    } catch (...) {
        std::lock_guard<std::mutex> lock(mutex);
        exceptionPtr = std::current_exception();
    }

    cleanup();
}

void ChannelStateWriteRequestExecutorImpl::loop()
{
    while (true) {
        auto req = take();
        if (!req) {
            continue;
        }
        dispatcher->dispatch(req);
    }
}

std::shared_ptr<ChannelStateWriteRequest> ChannelStateWriteRequestExecutorImpl::take()
{
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&]() { return stopped || !readyQueue.empty(); });
    if (readyQueue.empty()) {
        return nullptr;
    }
    std::shared_ptr<ChannelStateWriteRequest> req = std::move(readyQueue.front());
    readyQueue.pop_front();
    return req;
}

void ChannelStateWriteRequestExecutorImpl::enqueue(std::shared_ptr<ChannelStateWriteRequest> req, bool priority)
{
    SubtaskID sid = SubtaskID::Of(req->getJobVertexID(), req->getSubtaskIndex());
    auto& queue = unreadyQueues[sid];
    bool flag = queue.empty();
    if (!flag || !req->getReadyFuture()->IsDone()) {
        std::shared_ptr<ChannelStateWriteRequest> raw = req;
        {
            queue.push(req);
        }
        if (flag) {
            registerCallback(raw, sid);
        }
    } else {
        if (priority) {
            readyQueue.push_front(req);
        } else {
            readyQueue.push_back(req);
        }
        cv.notify_all();
    }
}

void ChannelStateWriteRequestExecutorImpl::registerCallback(
    std::shared_ptr<ChannelStateWriteRequest> first, SubtaskID sid)
{
    auto future = first->getReadyFuture();
    future->ThenRun([this, first, sid, future]() {
        auto it = unreadyQueues.find(sid);
        if (it == unreadyQueues.end()) {
            return;
        }
        auto& q = it->second;

        q.front()->getReadyFuture()->Get();
        std::lock_guard<std::mutex> lock(mutex);
        while (!q.empty() && q.front()->getReadyFuture()->IsDone()) {
            readyQueue.push_back(q.front());
            q.pop();
        }
        if (!q.empty()) {
            registerCallback(q.front(), sid);
        }
        cv.notify_all();
    });
}

void ChannelStateWriteRequestExecutorImpl::cleanup()
{
    std::vector<std::shared_ptr<ChannelStateWriteRequest>> all;
    {
        std::lock_guard<std::mutex> lock(mutex);

        while (!readyQueue.empty()) {
            all.push_back(std::move(readyQueue.front()));
            readyQueue.pop_front();
        }

        for (auto& kv : unreadyQueues) {
            auto& q = kv.second;
            while (!q.empty()) {
                all.push_back(std::move(q.front()));
                q.pop();
            }
        }
        unreadyQueues.clear();
    }

    auto cause = exceptionPtr ? exceptionPtr : std::make_exception_ptr(std::runtime_error("executor stopped"));

    for (auto& r : all) {
        r->cancel(cause);
    }

    if (exceptionPtr) {
        dispatcher->fail(exceptionPtr);
    }
}

void ChannelStateWriteRequestExecutorImpl::completeRegistration()
{
    if (isRegistering.exchange(false)) {
        onRegistered(this);
    }
}

} // namespace omnistream
