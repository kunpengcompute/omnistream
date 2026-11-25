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

#ifndef THROWABLE_H
#define THROWABLE_H

#include <string>

namespace omnistream {
class Throwable : std::exception {
public:
    explicit Throwable(const std::string& message) : message_(message)
    {
    }
    const char* what() const noexcept override
    {
        return message_.c_str();
    }
    std::string toString() const
    {
        return "Throwable(" + message_ + ")";
    }

private:
    std::string message_;
};
} // namespace omnistream

#endif
