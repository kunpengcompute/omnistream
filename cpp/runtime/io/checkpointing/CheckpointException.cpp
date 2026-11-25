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

#include "CheckpointException.h"
#include <sstream>
namespace omnistream {
// Helper function to get reason message
static std::string GetReasonMessage(CheckpointFailureReason reason)
{
    // Implement mapping of reasons to messages
    switch (reason) {
        case CheckpointFailureReason::CHECKPOINT_DECLINED: return std::string("Checkpoint declined");
        case CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED:
            return std::string(
                "Checkpoint was canceled "
                "because a barrier from newer checkpoint was received.");
        case CheckpointFailureReason::CHANNEL_STATE_SHARED_STREAM_EXCEPTION:
            return std::string(
                "The checkpoint was aborted "
                "due to exception of other subtasks sharing the ChannelState file.");
        case CheckpointFailureReason::CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER:
            return std::string("Task received cancellation from one of its inputs");
            // ... other cases
        default: return "Unknown checkpoint failure";
    }
}

CheckpointException::CheckpointException(CheckpointFailureReason failureReason)
    : failureReason_(failureReason),
      message_(GetReasonMessage(failureReason))
{
}

CheckpointException::CheckpointException(CheckpointFailureReason failureReason, const std::exception& cause)
    : failureReason_(failureReason)
{
    std::ostringstream oss;
    oss << GetReasonMessage(failureReason) << " Caused by: " << cause.what();
    message_ = oss.str();
}

CheckpointException::CheckpointException(
    const std::string& message, CheckpointFailureReason failureReason, const std::exception& cause)
    : failureReason_(failureReason)
{
    std::ostringstream oss;
    oss << message << " Failure reason: " << GetReasonMessage(failureReason) << " Caused by: " << cause.what();
    message_ = oss.str();
}

CheckpointFailureReason CheckpointException::GetCheckpointFailureReason() const
{
    return failureReason_;
}
} // namespace omnistream
