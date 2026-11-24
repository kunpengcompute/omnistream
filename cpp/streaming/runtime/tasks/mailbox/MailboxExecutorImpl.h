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

#ifndef MAILBOXEXECUTORIMPL_H
#define MAILBOXEXECUTORIMPL_H

#include "MailboxExecutor.h"

#include <utility>
#include <vector>
#include <memory>
#include <string>
#include <stdexcept>
#include <optional>

#include "MailboxProcessor.h"

namespace omnistream {
class MailboxExecutorImpl : public MailboxExecutor {
public:
    MailboxExecutorImpl(TaskMailbox* mailbox, int priority, std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
        : mailbox(mailbox),
          priority(priority),
          actionExecutor(std::move(actionExecutor))
    {
    }

    MailboxExecutorImpl(
        TaskMailbox* mailbox,
        int priority,
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
        MailboxProcessor* mailboxProcessor)
        : mailbox(mailbox),
          priority(priority),
          actionExecutor(std::move(actionExecutor)),
          mailboxProcessor_(mailboxProcessor)
    {
    }

    void execute(std::shared_ptr<ThrowingRunnable> command, const std::string& description) override
    {
        auto mail = new Mail(std::move(command), priority, actionExecutor, description, std::vector<std::string>());
        mailbox->put(mail);
    }

    void execute(
        std::shared_ptr<ThrowingRunnable> command,
        const std::string& descriptionFormat,
        const std::vector<std::any>& descriptionArgs) override
    {
        auto mail =
            new Mail(std::move(command), priority, actionExecutor, descriptionFormat, std::vector<std::string>());
        mailbox->put(mail);
    }

    void yield() override
    {
        auto mail = mailbox->take(priority);

        mail->run();
    }

    bool tryYield() override
    {
        auto optionalMail = mailbox->tryTake(priority);
        if (optionalMail) {
            optionalMail->run();

            return true;
        } else {
            return false;
        }
    }

    std::string toString() const override
    {
        return "MailboxExecutorImpl";
    }

private:
    TaskMailbox* mailbox;
    const int priority;
    const std::shared_ptr<StreamTaskActionExecutor> actionExecutor;
    MailboxProcessor* mailboxProcessor_;
};
} // namespace omnistream

#endif // MAILBOXEXECUTORIMPL_H
