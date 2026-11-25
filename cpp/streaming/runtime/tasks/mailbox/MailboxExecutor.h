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

#ifndef OMNISTREAM_MAILBOX_EXECUTOR_H
#define OMNISTREAM_MAILBOX_EXECUTOR_H

#include <any>
#include <memory>
#include <string>
#include <vector>
#include <future>
#include <exception>

#include "TaskMailbox.h"
#include "MailboxDefaultAction.h"
#include "StreamTaskActionExecutor.h"

namespace omnistream {
class MailboxExecutor {
public:
    virtual ~MailboxExecutor() = default;

    virtual void execute(std::shared_ptr<ThrowingRunnable> command, const std::string& description) = 0;
    virtual void execute(
        std::shared_ptr<ThrowingRunnable> command,
        const std::string& descriptionFormat,
        const std::vector<std::any>& descriptionArgs) = 0;

    virtual void yield() = 0;
    virtual bool tryYield() = 0;

    virtual std::string toString() const = 0;
};
} // namespace omnistream

#endif // OMNISTREAM_MAILBOX_EXECUTOR_H
