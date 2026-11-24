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

#ifndef OMNISTREAM_TASKMAILBOXIMPL_H
#define OMNISTREAM_TASKMAILBOXIMPL_H

#include <deque>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <optional>
#include <algorithm>
#include "TaskMailbox.h"

namespace omnistream {

class TaskMailboxImpl : public TaskMailbox {
public:
    explicit TaskMailboxImpl(std::thread::id taskMailboxThreadId) : taskMailboxThreadId(taskMailboxThreadId)
    {
    }
    TaskMailboxImpl() : taskMailboxThreadId(std::thread::id())
    {
    }

    bool isMailboxThread() const override;
    bool hasMail() const override;
    int size();

    Mail* tryTake(int priority) override;
    Mail* take(int priority) override;

    bool createBatch() override;
    Mail* tryTakeFromBatch() override;

    void put(Mail* mail) override;
    void putFirst(Mail* mail) override;

    std::vector<Mail*> drain() override;
    void quiesce() override;
    std::vector<Mail*> close() override;
    State getState() override;
    void runExclusively(const std::shared_ptr<ThrowingRunnable>& runnable) override;

    std::string toString() override;

private:
    std::recursive_mutex lock;
    std::deque<Mail*> queue;
    std::condition_variable_any notEmpty;
    State state = State::OPEN;
    std::thread::id taskMailboxThreadId;
    std::deque<Mail*> batch;
    std::atomic<bool> hasNewMail = false;

    Mail* takeOrNull(std::deque<Mail*>& queue, int priority);
    void checkIsMailboxThread() const;
    void checkPutStateConditions();
    void checkTakeStateConditions();
};

} // namespace omnistream

#endif // OMNISTREAM_TASKMAILBOXIMPL_H
