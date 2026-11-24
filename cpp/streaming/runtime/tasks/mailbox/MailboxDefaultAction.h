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

#ifndef MAILBOX_DEFAULT_ACTION_H
#define MAILBOX_DEFAULT_ACTION_H

#include <memory>

#include "PeriodTimer.h"

namespace omnistream {

class MailboxDefaultAction {
public:
    class Suspension {
    public:
        virtual void resume() = 0;
        virtual ~Suspension() = default;
    };

    class Controller {
    public:
        virtual void allActionsCompleted() = 0;
        virtual std::shared_ptr<Suspension> suspendDefaultAction() = 0;
        virtual std::shared_ptr<Suspension> suspendDefaultAction(std::shared_ptr<PeriodTimer> periodTimer) = 0;
        virtual ~Controller() = default;
    };

    virtual void runDefaultAction(Controller* controller) = 0;
    virtual ~MailboxDefaultAction() = default;
};

} // namespace omnistream

#endif // MAILBOX_DEFAULT_ACTION_H
