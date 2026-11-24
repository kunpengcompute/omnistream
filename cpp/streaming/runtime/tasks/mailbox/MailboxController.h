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

#ifndef MAILBOXCONTROLLER_H
#define MAILBOXCONTROLLER_H

#include <memory>
#include <string>

#include "MailboxDefaultAction.h"

#include "MailboxProcessor.h"

namespace omnistream {

class MailboxController : public MailboxDefaultAction::Controller {
public:
    explicit MailboxController(MailboxProcessor* mailboxProcessor);

    ~MailboxController() override;

    void allActionsCompleted() override;
    std::shared_ptr<MailboxDefaultAction::Suspension> suspendDefaultAction(
        std::shared_ptr<PeriodTimer> suspensionPeriodTimer) override;
    std::shared_ptr<MailboxDefaultAction::Suspension> suspendDefaultAction() override;

    std::string toString() const;

private:
    MailboxProcessor* mailboxProcessor_;
};

} // namespace omnistream

#endif // MAILBOXCONTROLLER_H
