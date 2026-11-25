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

#ifndef MAIL_H
#define MAIL_H

#include <string>
#include <memory>
#include <vector>
#include <sstream>
#include <iostream>

#include "StreamTaskActionExecutor.h"

namespace omnistream {
class Mail {
public:
    Mail();

    Mail(
        std::shared_ptr<ThrowingRunnable> runnable,
        int priority,
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
        const std::string& descriptionFormat,
        const std::vector<std::string>& descriptionArgs);

    Mail(
        std::shared_ptr<ThrowingRunnable> runnable,
        int priority,
        const std::string& descriptionFormat,
        const std::vector<std::string>& descriptionArgs);

    ~Mail();

    int getPriority() const;
    void tryCancel();
    std::string toString() const;
    void run();

private:
    std::shared_ptr<ThrowingRunnable> runnable_;
    int priority_;
    std::string descriptionFormat_;
    std::vector<std::string> descriptionArgs_;
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor_;
};
} // namespace omnistream

#endif // MAIL_H
