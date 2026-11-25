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

#ifndef OMNISTREAM_EXECUTIONSTATE_H
#define OMNISTREAM_EXECUTIONSTATE_H

#include <string>
#include <memory>
#include <sstream>

namespace omnistream {
enum class ExecutionState {
    CREATED,
    SCHEDULED,
    DEPLOYING,
    RUNNING,
    FINISHED,
    CANCELING,
    CANCELED,
    FAILED,
    RECONCILING,
    INITIALIZING
};

class ExecutionStateWrapper {
public:
    explicit ExecutionStateWrapper(ExecutionState state = ExecutionState::CREATED) : state_(state) {};

    ~ExecutionStateWrapper();

    ExecutionState getState() const
    {
        return state_;
    };
    void setState(ExecutionState state)
    {
        state_ = state;
    };

    bool isTerminal() const
    {
        return state_ == ExecutionState::FINISHED || state_ == ExecutionState::CANCELED ||
               state_ == ExecutionState::FAILED;
    };

    std::string toString() const
    {
        switch (state_) {
            case ExecutionState::CREATED: return "CREATED";
            case ExecutionState::SCHEDULED: return "SCHEDULED";
            case ExecutionState::DEPLOYING: return "DEPLOYING";
            case ExecutionState::RUNNING: return "RUNNING";
            case ExecutionState::FINISHED: return "FINISHED";
            case ExecutionState::CANCELING: return "CANCELING";
            case ExecutionState::CANCELED: return "CANCELED";
            case ExecutionState::FAILED: return "FAILED";
            case ExecutionState::RECONCILING: return "RECONCILING";
            case ExecutionState::INITIALIZING: return "INITIALIZING";
            default: return "UNKNOWN";
        }
    };

private:
    ExecutionState state_;
};

} // namespace omnistream

#endif // OMNISTREAM_EXECUTIONSTATE_H
