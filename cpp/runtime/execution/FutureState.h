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

// omnistream/FutureState.h
#ifndef OMNISTREAM_FUTURESTATE_H
#define OMNISTREAM_FUTURESTATE_H

#include <memory>
#include <string>

namespace omnistream {

class FutureState {
public:
    enum State {
        Completed,
        CompletedExceptional,
        Cancelled,
        Pending,
    };

    explicit FutureState(State initialState = Pending);

    void setCompleted();
    void setCompletedExceptional();
    void setCancelled();
    void setPending();

    bool isCompleted() const;
    bool isCompletedExceptional() const;
    bool isCancelled() const;
    bool isPending() const;

    std::string toString() const;

    bool operator==(const FutureState& other) const;
    bool operator!=(const FutureState& other) const;

    static std::shared_ptr<FutureState> allOf(
        const std::shared_ptr<FutureState>& first, const std::shared_ptr<FutureState>& second)
    {
        if (first && second && *first == *second) {
            return first;
        }

        if (!first && !second) {
            return nullptr;
        } else if (!first) {
            return second;
        } else if (!second) {
            return first;
        }

        if (first->currentState < second->currentState) {
            return first;
        } else {
            return second;
        }
    }

    static std::shared_ptr<FutureState> anyOf(
        const std::shared_ptr<FutureState>& first, const std::shared_ptr<FutureState>& second)
    {
        if (first && second && *first == *second) {
            return first;
        }

        if (!first && !second) {
            return nullptr;
        } else if (!first) {
            return second;
        } else if (!second) {
            return first;
        }

        if (first->currentState > second->currentState) {
            return first;
        } else {
            return second;
        }
    }

private:
    State currentState;
};

} // namespace omnistream

#endif // OMNISTREAM_FUTURESTATE_H
