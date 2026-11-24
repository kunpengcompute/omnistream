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

#include "FutureState.h"
#include <sstream>

namespace omnistream {

FutureState::FutureState(State initialState) : currentState(initialState)
{
}

void FutureState::setCompleted()
{
    currentState = Completed;
}

void FutureState::setCompletedExceptional()
{
    currentState = CompletedExceptional;
}

void FutureState::setCancelled()
{
    currentState = Cancelled;
}

void FutureState::setPending()
{
    currentState = Pending;
}

bool FutureState::isCompleted() const
{
    return currentState == Completed;
}

bool FutureState::isCompletedExceptional() const
{
    return currentState == CompletedExceptional;
}

bool FutureState::isCancelled() const
{
    return currentState == Cancelled;
}

bool FutureState::isPending() const
{
    return currentState == Pending;
}

std::string FutureState::toString() const
{
    switch (currentState) {
        case Pending: return "Pending";
        case Completed: return "Completed";
        case CompletedExceptional: return "CompletedExceptional";
        case Cancelled: return "Cancelled";
        default: return "Unknown";
    }
}

bool FutureState::operator==(const FutureState& other) const
{
    return currentState == other.currentState;
}

bool FutureState::operator!=(const FutureState& other) const
{
    return !(*this == other);
}

} // namespace omnistream
