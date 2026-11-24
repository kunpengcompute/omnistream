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

#include "ScopeGuard.h"

#include <cstdio>
#include <utility>
#include "common.h"

namespace omnistream {
ScopeGuard::ScopeGuard(std::function<void()> cleanup) noexcept : cleanup_(std::move(cleanup))
{
}

ScopeGuard::~ScopeGuard() noexcept
{
    if (!cleanup_) {
        return;
    }
    try {
        cleanup_();
    } catch (const std::exception& e) {
        INFO_RELEASE("ScopeGuard cleanup failed: " << e.what());
    } catch (...) {
        INFO_RELEASE("ScopeGuard cleanup failed: unknown exception");
    }
}
} // namespace omnistream
