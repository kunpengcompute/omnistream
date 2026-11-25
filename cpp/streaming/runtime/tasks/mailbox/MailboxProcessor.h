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

#ifndef MAILBOX_PROCESSOR_H
#define MAILBOX_PROCESSOR_H

#include <memory>
#include <utility>
#include <vector>
#include <string>
#include "TaskMailbox.h"
#include "MailboxDefaultAction.h"
#include "StreamTaskActionExecutor.h"
#include "MailboxExecutor.h"
#include "PeriodTimer.h"

namespace omnistream {
class MailboxProcessor {
public:
    MailboxProcessor();
    explicit MailboxProcessor(std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction);
    MailboxProcessor(
        std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction,
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor);
    MailboxProcessor(
        std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction,
        TaskMailbox* mailbox,
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor);
    ~MailboxProcessor();

    std::shared_ptr<MailboxExecutor> getMainMailboxExecutor();
    std::shared_ptr<MailboxExecutor> getMailboxExecutor(int priority);

    inline void prepareClose();
    void close();
    inline void drain();

    void runMailboxLoop();
    void suspend();
    bool runMailboxStep();
    bool isMailboxThread();
    void reportThrowable(std::exception_ptr throwable);
    void allActionsCompleted();

    bool isDefaultActionAvailable()
    {
        return suspendedDefaultAction == nullptr;
    }
    bool isMailboxLoopRunning();
    bool hasMail();

    std::string toString() const;

    void sendPoisonMail(std::shared_ptr<ThrowingRunnable> mail);
    void sendPoisonMail(std::shared_ptr<ThrowingRunnable> mail, const std::string& descriptionFormat);
    void sendControlMail(
        std::shared_ptr<ThrowingRunnable> mail,
        const std::string& descriptionFormat,
        const std::vector<std::string>& descriptionArgs = {});
    bool processMail(TaskMailbox* mailbox, bool singleStep);
    bool processMailsWhenDefaultActionUnavailable();
    bool processMailsNonBlocking(bool singleStep);
    void maybePauseIdleTimer();
    void maybeRestartIdleTimer();
    std::shared_ptr<MailboxDefaultAction::Suspension> suspendDefaultAction(
        std::shared_ptr<PeriodTimer> suspensionTimer);
    bool isNextLoopPossible() const
    {
        return !suspended;
    }

    class DefaultActionSuspension : public MailboxDefaultAction::Suspension {
    public:
        explicit DefaultActionSuspension(
            MailboxProcessor* mailboxProcessor, std::shared_ptr<PeriodTimer> suspensionTimer)
            : mailboxProcessor_(mailboxProcessor),
              suspensionTimer_(std::move(suspensionTimer))
        {
        }

        void resume() override;
        std::shared_ptr<PeriodTimer> getSuspensionTimer()
        {
            return suspensionTimer_;
        };

    private:
        MailboxProcessor* mailboxProcessor_;
        std::shared_ptr<PeriodTimer> suspensionTimer_;
    };

    class lambdaHelper {
    public:
        explicit lambdaHelper(MailboxProcessor* mailbox_processor) : processor_(mailbox_processor) {};
        MailboxProcessor* processor_;
        void suspended(bool suspended)
        {
            processor_->suspended = suspended;
        }

        void mailboxLoopRunning(bool mailboxLoopRunning)
        {
            processor_->mailboxLoopRunning = mailboxLoopRunning;
        }

        void resume()
        {
            processor_->suspendedDefaultAction = nullptr;
        }

        TaskMailbox* getMailbox()
        {
            return processor_->mailbox_;
        }

        MailboxProcessor* getOwner()
        {
            return processor_;
        }
    };

private:
    // inner class forward declaration
    TaskMailbox* mailbox_ = nullptr;
    std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction;
    bool suspended;
    bool mailboxLoopRunning;
    std::shared_ptr<DefaultActionSuspension> suspendedDefaultAction;
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor;
    int workaround = 0;
};

} // namespace omnistream

#endif // MAILBOX_PROCESSOR_H
