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

#include "MailboxProcessor.h"

#include <stdexcept>
#include <sstream>
#include <algorithm>
#include <climits>

#include "MailboxController.h"
#include "MailboxExecutorImpl.h"
#include "TaskMailboxImpl.h"

namespace omnistream {
MailboxProcessor::MailboxProcessor() : suspended(false), mailboxLoopRunning(false)
{
}

MailboxProcessor::MailboxProcessor(std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction)
    : MailboxProcessor(std::move(mailboxDefaultAction), StreamTaskActionExecutor::IMMEDIATE)
{
}

MailboxProcessor::MailboxProcessor(
    std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction,
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
    : MailboxProcessor(std::move(mailboxDefaultAction), new TaskMailboxImpl(std::this_thread::get_id()), actionExecutor)
{
}

MailboxProcessor::MailboxProcessor(
    std::unique_ptr<MailboxDefaultAction> mailboxDefaultAction,
    TaskMailbox* mailbox,
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
    : mailbox_(mailbox),
      mailboxDefaultAction(std::move(mailboxDefaultAction)),
      suspended(false),
      mailboxLoopRunning(true),
      suspendedDefaultAction(nullptr),
      actionExecutor(actionExecutor)
{
}

MailboxProcessor::~MailboxProcessor()
{
    if (mailbox_) {
        delete mailbox_;
    }
}

std::shared_ptr<MailboxExecutor> MailboxProcessor::getMainMailboxExecutor()
{
    return std::make_shared<MailboxExecutorImpl>(mailbox_, TaskMailbox::MIN_PRIORITY, actionExecutor);
}

std::shared_ptr<MailboxExecutor> MailboxProcessor::getMailboxExecutor(int priority)
{
    return std::make_shared<MailboxExecutorImpl>(mailbox_, priority, actionExecutor, this);
}

void MailboxProcessor::prepareClose()
{
    mailbox_->quiesce();
}

void MailboxProcessor::close()
{
    auto droppedMails = mailbox_->close();
    if (!droppedMails.empty()) {
        // Log dropped mails (if logging is available)
        std::exception_ptr maybeErr;
        for (auto& droppedMail : droppedMails) {
            try {
                droppedMail->tryCancel();
            } catch (...) {
                if (!maybeErr) {
                    maybeErr = std::current_exception();
                }
            }
        }
        if (maybeErr) {
            std::rethrow_exception(maybeErr);
        }
    }
}

void MailboxProcessor::drain()
{
    for (auto& mail : mailbox_->drain()) {
        mail->run();
    }
}

void MailboxProcessor::runMailboxLoop()
{
    LOG("runMailboxLoop  start");
    suspended = !mailboxLoopRunning;

    LOG("mailbox localmailbox");
    auto localMailbox = mailbox_;

    LOG(">>>localMailbox  isMailboxThread");
    if (!localMailbox->isMailboxThread()) {
        throw std::runtime_error("Method must be executed by declared mailbox thread!");
    }

    if (localMailbox->getState() != TaskMailbox::State::OPEN) {
        throw std::runtime_error("Mailbox must be opened!");
    }
    LOG(">>>before isNextLoopPossible");
    MailboxController* mailboxController = new MailboxController(this);
    while (isNextLoopPossible()) {
        // LOG(">>>before procsiyessMail")
        // workaround
        // workaround++;
        // LOG_TRACE( "Round number " << workaround)
        // LOG("The current mailbox_ before processMail :" << mailbox_->toString())

        // 阻塞的 `processMail` 调用将不会返回，直到有默认操作可用为止。
        processMail(mailbox_, false);
        if (isNextLoopPossible()) {
            LOG(">>>before runDefaultAction");
            mailboxDefaultAction->runDefaultAction(mailboxController);
        }
    }
    delete mailboxController;
    INFO_RELEASE(">>>after isNextLoopPossible");
}

class LambdaSuspend : public MailboxProcessor::lambdaHelper {
public:
    explicit LambdaSuspend(MailboxProcessor* obj1) : lambdaHelper(obj1) {};

    void lambda()
    {
        lambdaHelper::suspended(true);
    }
};
void MailboxProcessor::suspend()
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaSuspend>>(
        std::make_shared<LambdaSuspend>(this), &LambdaSuspend::lambda, "Suspend");
    sendPoisonMail(runnable);
}

bool MailboxProcessor::runMailboxStep()
{
    suspended = !mailboxLoopRunning;

    if (processMail(mailbox_, true)) {
        return true;
    }
    if (isDefaultActionAvailable() && isNextLoopPossible()) {
        auto controller = new MailboxController(this);
        mailboxDefaultAction->runDefaultAction(controller);
        delete controller;
        return true;
    }
    return false;
}

bool MailboxProcessor::isMailboxThread()
{
    return mailbox_->isMailboxThread();
}

class LambdaReportThrowable {
public:
    explicit LambdaReportThrowable(std::exception_ptr throwable) : throwable_(throwable) {};
    std::exception_ptr throwable_;
    void lambda()
    {
        std::rethrow_exception(throwable_);
    }
};

void MailboxProcessor::reportThrowable(std::exception_ptr throwable)
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaReportThrowable>>(
        std::make_shared<LambdaReportThrowable>(throwable), &LambdaReportThrowable::lambda);
    sendControlMail(runnable, "Report throwable");
}

class LambdaAllActionCompleted : public MailboxProcessor::lambdaHelper {
public:
    explicit LambdaAllActionCompleted(MailboxProcessor* obj1) : lambdaHelper(obj1) {};
    void lambda()
    {
        lambdaHelper::suspended(true);
        lambdaHelper::mailboxLoopRunning(false);
    }
};

void MailboxProcessor::allActionsCompleted()
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaAllActionCompleted>>(
        std::make_shared<LambdaAllActionCompleted>(this), &LambdaAllActionCompleted::lambda);
    sendPoisonMail(runnable);
}

bool MailboxProcessor::isMailboxLoopRunning()
{
    return mailboxLoopRunning;
}

bool MailboxProcessor::hasMail()
{
    return mailbox_->hasMail();
}

std::string MailboxProcessor::toString() const
{
    std::stringstream ss;
    ss << "MailboxProcessor";
    return ss.str();
}

class LambdaSendPoisonMail : public MailboxProcessor::lambdaHelper {
public:
    LambdaSendPoisonMail(MailboxProcessor* processor, std::shared_ptr<ThrowingRunnable> runnableMail)
        : lambdaHelper(processor),
          runnableMail_(runnableMail) {};
    void lambda()
    {
        auto mailbox = lambdaHelper::getMailbox();
        if (mailbox->getState() == TaskMailbox::State::OPEN) {
            std::vector<std::string> stringVector;
            processor_->sendControlMail(runnableMail_, "PoisonMail" + runnableMail_->ToString(), stringVector);
        }
    }
    std::shared_ptr<ThrowingRunnable> runnableMail_;
};

void MailboxProcessor::sendPoisonMail(std::shared_ptr<ThrowingRunnable> runnableMail)
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaSendPoisonMail>>(
        std::make_shared<LambdaSendPoisonMail>(this, runnableMail), &LambdaSendPoisonMail::lambda);
    mailbox_->runExclusively(runnable);
}

void MailboxProcessor::sendControlMail(
    std::shared_ptr<ThrowingRunnable> runnable,
    const std::string& descriptionFormat,
    const std::vector<std::string>& descriptionArgs)
{
    auto mail = new Mail(runnable, INT_MAX, descriptionFormat, descriptionArgs);
    mailbox_->putFirst(mail);
    LOG("MailboxProcessor::sendControlMail after putFirst " << mailbox_->toString());
}

// 处理 MailBox 中的 mail, 如果没有 mail 要处理，这里直接返回; MailBox 中当前现存的 mail 全部处理完.
bool MailboxProcessor::processMail(TaskMailbox* mailbox, bool singleStep)
{
    bool isBatchAvailable = mailbox->createBatch();
    bool processed = isBatchAvailable && processMailsNonBlocking(singleStep);
    if (singleStep) {
        return processed;
    }
    processed |= processMailsWhenDefaultActionUnavailable();
    return processed;
}

bool MailboxProcessor::processMailsWhenDefaultActionUnavailable()
{
    bool processedSomething = false;
    Mail* maybeMail = nullptr;
    while (!isDefaultActionAvailable() && isNextLoopPossible()) {
        maybeMail = mailbox_->tryTake(TaskMailbox::MIN_PRIORITY);
        if (!maybeMail) {
            maybeMail = mailbox_->take(TaskMailbox::MIN_PRIORITY);
        }
        maybePauseIdleTimer();
        maybeMail->run();
        delete maybeMail;
        maybeRestartIdleTimer();
        processedSomething = true;
    }
    return processedSomething;
}

bool MailboxProcessor::processMailsNonBlocking(bool singleStep)
{
    long processedMails = 0;
    Mail* maybeMail = nullptr;

    while (isNextLoopPossible() && (maybeMail = mailbox_->tryTakeFromBatch())) {
        if (processedMails++ == 0) {
            maybePauseIdleTimer();
        }
        maybeMail->run();
        delete maybeMail;
        if (singleStep) {
            break;
        }
    }
    if (processedMails > 0) {
        maybeRestartIdleTimer();
        return true;
    } else {
        return false;
    }
}

void MailboxProcessor::maybePauseIdleTimer()
{
    if (suspendedDefaultAction && suspendedDefaultAction->getSuspensionTimer()) {
        suspendedDefaultAction->getSuspensionTimer()->markEnd();
    }
}

void MailboxProcessor::maybeRestartIdleTimer()
{
    if (suspendedDefaultAction && suspendedDefaultAction->getSuspensionTimer()) {
        suspendedDefaultAction->getSuspensionTimer()->markStart();
    }
}

std::shared_ptr<MailboxDefaultAction::Suspension> MailboxProcessor::suspendDefaultAction(
    std::shared_ptr<PeriodTimer> suspensionTimer)
{
    if (!mailbox_->isMailboxThread()) {
        throw std::runtime_error("Suspending must only be called from the mailbox thread!");
    }

    if (suspendedDefaultAction) {
        throw std::runtime_error("Default action has already been suspended");
    }
    if (!suspendedDefaultAction) {
        suspendedDefaultAction = std::make_shared<DefaultActionSuspension>(this, suspensionTimer);
    }

    return suspendedDefaultAction;
}

void MailboxProcessor::sendPoisonMail(std::shared_ptr<ThrowingRunnable> mail, const std::string& descriptionFormat)
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaSendPoisonMail>>(
        std::make_shared<LambdaSendPoisonMail>(this, mail), &LambdaSendPoisonMail::lambda);
    mailbox_->runExclusively(runnable);
}

class LambdaResume : public MailboxProcessor::lambdaHelper {
public:
    LambdaResume(MailboxProcessor* processor) : lambdaHelper(processor)
    {
    }
    void lambda()
    {
        resume();
    }
};

void MailboxProcessor::DefaultActionSuspension::resume()
{
    if (this->mailboxProcessor_->isMailboxThread()) {
        this->mailboxProcessor_->suspendedDefaultAction = nullptr;
        return;
    }

    try {
        auto runnable = std::make_shared<MemberFunctionRunnable<LambdaResume>>(
            std::make_shared<LambdaResume>(this->mailboxProcessor_), &LambdaResume::lambda);
        this->mailboxProcessor_->sendControlMail(runnable, "resume default action");
    } catch (...) {
        GErrorLog("Something wrong happen, but ignore it");
    }
}

} // namespace omnistream
