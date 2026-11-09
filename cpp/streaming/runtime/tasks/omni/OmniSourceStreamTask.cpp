/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "OmniSourceStreamTask.h"

#include <exception>

#include "common.h"
#include "core/utils/threads/CompletableFutureV2.h"

namespace omnistream {
StopMode FinishingReasonToStopMode(FinishingReason reason)
{
    switch (reason) {
        case FinishingReason::END_OF_DATA: return StopMode::DRAIN;
        case FinishingReason::STOP_WITH_SAVEPOINT_DRAIN: return StopMode::DRAIN;
        case FinishingReason::STOP_WITH_SAVEPOINT_NO_DRAIN: return StopMode::NO_DRAIN;
        default: return StopMode::DRAIN;
    }
}

// Optional: For easier debugging or logging
std::string FinishingReasonToString(FinishingReason reason)
{
    switch (reason) {
        case FinishingReason::END_OF_DATA: return "END_OF_DATA";
        case FinishingReason::STOP_WITH_SAVEPOINT_DRAIN: return "STOP_WITH_SAVEPOINT_DRAIN";
        case FinishingReason::STOP_WITH_SAVEPOINT_NO_DRAIN: return "STOP_WITH_SAVEPOINT_NO_DRAIN";
        default: return "UNKNOWN_FINISHING_REASON";
    }
}

OmniSourceStreamTask::OmniSourceStreamTask(
    std::shared_ptr<RuntimeEnvironmentV2>& env, std::unique_ptr<Object> lockObject, int taskType)
    : OmniStreamTask(env, SynchronizedStreamTaskActionExecutor::synchronizedExecutor(&lockObject->mutex), taskType)
{
    this->lockObject_ = std::move(lockObject);
}

void OmniSourceStreamTask::init()
{
    OmniStreamTask::init();
}

void OmniSourceStreamTask::processInput(MailboxDefaultAction::Controller* controller)
{
    controller->suspendDefaultAction();
    sourceThread_ = std::make_unique<std::thread>([this] { runSourceInThread(); });
}

void OmniSourceStreamTask::runSourceInThread()
{
    try {
        auto* mainOperator = dynamic_cast<StreamSource<omnistream::VectorBatch>*>(mainOperator_);
        if (!mainOperator) {
            THROW_RUNTIME_ERROR("mainOperator_ is not of type StreamSource<omnistream::VectorBatch>");
        }
        mainOperator->run(lockObject_.get());

        CompleteProcessing();

        auto completionMail = std::make_shared<VoidFunctionRunnable>([this]() { mailboxProcessor_->suspend(); });
        mainMailboxExecutor_->execute(completionMail, "Source completion");
    } catch (const std::exception&) {
        const auto exception = std::current_exception();
        auto errorMail = std::make_shared<VoidFunctionRunnable>(
            [this, exception]() { mailboxProcessor_->reportThrowable(exception); });
        mainMailboxExecutor_->execute(errorMail, "Source error");
    }
}

void OmniSourceStreamTask::CompleteProcessing()
{
    auto stopMode = FinishingReasonToStopMode(finishingReason);
    auto completionFuture = std::make_shared<CompletableFutureV2<void>>();
    mainMailboxExecutor_->execute(
        std::make_shared<VoidFunctionRunnable>(
            [this, stopMode, completionFuture]() {
                try {
                    EndData(stopMode);
                    completionFuture->Complete();
                } catch (...) {
                    completionFuture->CompleteExceptionally(std::current_exception());
                }
            },
            "SourceStreamTask finished processing data"),
        "SourceStreamTask finished processing data");
    completionFuture->Get();
}

void OmniSourceStreamTask::AdvanceToEndOfEventTime()
{
    auto watermark = Watermark(LONG_MAX);
    operatorChain->GetMainOperatorOutput()->emitWatermark(&watermark);
}

const std::string OmniSourceStreamTask::getName() const
{
    return std::string("OmniSourceStreamTask");
}

void OmniSourceStreamTask::cancel()
{
    // Cancel the writer first: the source thread may be blocked waiting for output buffers, and
    // only cancelling the writer releases it. Joining before that would wait on a thread that
    // cannot exit until this call runs. Null until postConstruct() runs, so guard the deref.
    if (recordWriter_) {
        recordWriter_->cancel();
    }

    if (mainOperator_) {
        auto* source = dynamic_cast<StreamSource<omnistream::VectorBatch>*>(mainOperator_);
        if (source) {
            source->cancel();
        }
    }

    if (sourceThread_ && sourceThread_->joinable()) {
        sourceThread_->join();
    }
    OmniStreamTask::cancel();
}

OmniSourceStreamTask::~OmniSourceStreamTask()
{
    if (sourceThread_ && sourceThread_->joinable()) {
        sourceThread_->join();
    }
}
} // namespace omnistream
