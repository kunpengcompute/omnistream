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

#include "MailboxController.h"
#include "MailboxProcessor.h"
#include "PeriodTimer.h"
#include "MailboxDefaultAction.h"
#include <sstream>

namespace omnistream {

MailboxController::MailboxController(MailboxProcessor* mailboxProcessor) : mailboxProcessor_(mailboxProcessor)
{
}

MailboxController::~MailboxController() = default;

void MailboxController::allActionsCompleted()
{
    mailboxProcessor_->allActionsCompleted();
}

std::shared_ptr<MailboxDefaultAction::Suspension> MailboxController::suspendDefaultAction(
    std::shared_ptr<PeriodTimer> suspensionPeriodTimer)
{
    return mailboxProcessor_->suspendDefaultAction(suspensionPeriodTimer);
}

std::shared_ptr<MailboxDefaultAction::Suspension> MailboxController::suspendDefaultAction()
{
    return mailboxProcessor_->suspendDefaultAction(nullptr);
}

std::string MailboxController::toString() const
{
    std::stringstream ss;
    ss << "MailboxController[mailboxProcessor=" << (mailboxProcessor_ ? "present" : "nullptr") << "]";
    return ss.str();
}

} // namespace omnistream
