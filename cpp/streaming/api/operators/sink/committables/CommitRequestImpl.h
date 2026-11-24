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

#ifndef FLINK_BENCHMARK_COMMITREQUESTIMPL_H
#define FLINK_BENCHMARK_COMMITREQUESTIMPL_H

#include <memory>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <sstream>
#include <string>
#include "Committer.h"
#include "core/include/common.h"

enum class CommitRequestState {
    RECEIVED,
    RETRY,
    FAILED,
    COMMITTED
};

inline bool IsFinalState(CommitRequestState state)
{
    switch (state) {
        case CommitRequestState::FAILED:
        case CommitRequestState::COMMITTED: return true;
        case CommitRequestState::RECEIVED:
        case CommitRequestState::RETRY: return false;
        default: throw std::invalid_argument("Unknown CommitRequestState");
    }
}

template <typename CommT>
class CommitRequestImpl : public CommitRequest<CommT> {
public:
    explicit CommitRequestImpl(const CommT& committable)
        : committable(committable),
          numRetries(0),
          state(CommitRequestState::RECEIVED)
    {
    }

    CommitRequestImpl(const CommT& committable, int numRetries, const CommitRequestState& state)
        : committable(committable),
          numRetries(numRetries),
          state(state)
    {
    }

    bool IsFinished() const
    {
        return IsFinalState(state);
    }

    CommitRequestState GetState() const
    {
        return state;
    }

    CommT GetCommittable() const
    {
        return committable;
    }

    int GetNumberOfRetries() const
    {
        return numRetries;
    }

    void signalFailedWithKnownReason(const std::exception& t)
    {
        state = CommitRequestState::FAILED;
    }

    void signalFailedWithUnknownReason(const std::exception& t)
    {
        state = CommitRequestState::FAILED;
        std::ostringstream oss;
        // oss << "FAILED to Commit " << committable;
        oss << "FAILED to Commit " << committable.toString() << " with exception: " << t.what();
        GErrorLog(oss.str());
    }

    void RetryLater()
    {
        state = CommitRequestState::RETRY;
        numRetries++;
    }

    void UpdateAndRetryLater(const CommT& committable)
    {
        this->committable = committable;
        RetryLater();
    }

    void SignalAlreadyCommitted()
    {
        state = CommitRequestState::COMMITTED;
    }

    void SetSelected()
    {
        state = CommitRequestState::RECEIVED;
    }

    void SetCommittedIfNoError()
    {
        if (state == CommitRequestState::RECEIVED) {
            state = CommitRequestState::COMMITTED;
        }
    }

    std::shared_ptr<CommitRequestImpl<CommT>> Copy() const
    {
        return std::make_shared<CommitRequestImpl<CommT>>(committable, numRetries, state);
    }

private:
    CommT committable;
    int numRetries;
    CommitRequestState state;
};

#endif // FLINK_BENCHMARK_COMMITREQUESTIMPL_H
