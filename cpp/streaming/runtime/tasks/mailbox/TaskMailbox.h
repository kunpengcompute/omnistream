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

#ifndef TASKMAILBOX_H
#define TASKMAILBOX_H

#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "Mail.h"
#include "core/utils/function/ThrowingRunnable.h"

namespace omnistream {

class TaskMailbox {
public:
    static constexpr int MIN_PRIORITY = -1;
    static constexpr int MAX_PRIORITY = std::numeric_limits<int>::max();

    enum class State {
        OPEN,
        QUIESCED,
        CLOSED
    };

    virtual ~TaskMailbox() = default;

    virtual bool isMailboxThread() const = 0;
    virtual bool hasMail() const = 0;
    virtual Mail* tryTake(int priority) = 0;
    virtual Mail* take(int priority) = 0;

    virtual bool createBatch() = 0;
    virtual Mail* tryTakeFromBatch() = 0;

    virtual void put(Mail* mail) = 0;
    virtual void putFirst(Mail* mail) = 0;

    virtual std::vector<Mail*> drain() = 0;
    virtual void quiesce() = 0;
    virtual std::vector<Mail*> close() = 0;
    virtual State getState() = 0;

    virtual void runExclusively(const std::shared_ptr<ThrowingRunnable>& runnable) = 0;

    virtual std::string toString() = 0;

    class MailboxClosedException : public std::runtime_error {
    public:
        explicit MailboxClosedException(const std::string& message) : std::runtime_error(message)
        {
        }
    };
};

} // namespace omnistream

#endif // TASKMAILBOX_H
