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

#ifndef OMNISTREAM_CHECKPOINTEXCEPTION_H
#define OMNISTREAM_CHECKPOINTEXCEPTION_H
#pragma once

#include <exception>
#include <string>
#include <memory>
namespace omnistream {
enum class CheckpointFailureReason {
    // Add all the failure reasons from Java version here
    // Example:
    CHECKPOINT_DECLINED,
    CHECKPOINT_DECLINED_SUBSUMED,
    CHECKPOINT_EXPIRED,
    CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER,
    CHECKPOINT_DECLINED_INPUT_END_OF_STREAM,
    CHANNEL_STATE_SHARED_STREAM_EXCEPTION,
    CHECKPOINT_DECLINED_TASK_CLOSING,
    CHECKPOINT_DECLINED_TASK_NOT_READY
    // ... other reasons
};

inline std::string toString(CheckpointFailureReason reason)
{
    switch (reason) {
        case CheckpointFailureReason::CHECKPOINT_DECLINED: return "CHECKPOINT_DECLINED";
        case CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED: return "CHECKPOINT_DECLINED_SUBSUMED";
        case CheckpointFailureReason::CHECKPOINT_EXPIRED: return "CHECKPOINT_EXPIRED";
        case CheckpointFailureReason::CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER:
            return "CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER";
        case CheckpointFailureReason::CHECKPOINT_DECLINED_INPUT_END_OF_STREAM:
            return "CHECKPOINT_DECLINED_INPUT_END_OF_STREAM";
        case CheckpointFailureReason::CHANNEL_STATE_SHARED_STREAM_EXCEPTION:
            return "CHANNEL_STATE_SHARED_STREAM_EXCEPTION";
        case CheckpointFailureReason::CHECKPOINT_DECLINED_TASK_CLOSING: return "CHECKPOINT_DECLINED_TASK_CLOSING";
        case CheckpointFailureReason::CHECKPOINT_DECLINED_TASK_NOT_READY: return "CHECKPOINT_DECLINED_TASK_NOT_READY";
        default: return "UNKNOWN";
    }
}

class CheckpointException : public std::exception {
public:
    explicit CheckpointException(CheckpointFailureReason failureReason);
    CheckpointException(CheckpointFailureReason failureReason, const std::exception& cause);
    CheckpointException(const std::string& message, CheckpointFailureReason failureReason, const std::exception& cause);

    ~CheckpointException() override {};
    const char* what() const noexcept override
    {
        return message_.c_str();
    }

    CheckpointFailureReason GetCheckpointFailureReason() const;

private:
    const CheckpointFailureReason failureReason_;
    std::string message_;
};
} // namespace omnistream
#endif // OMNISTREAM_CHECKPOINTEXCEPTION_H
