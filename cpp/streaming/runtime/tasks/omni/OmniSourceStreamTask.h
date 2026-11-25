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

#pragma once

#include <taskmanager/OmniRuntimeEnvironment.h>
#include "OmniStreamTask.h"
#include "streaming/api/operators/StreamSource.h"
#include "io/network/api/StopMode.h"
#include <atomic>
#include <thread>

namespace omnistream {

enum class FinishingReason : int {
    END_OF_DATA = 1,
    STOP_WITH_SAVEPOINT_DRAIN,
    STOP_WITH_SAVEPOINT_NO_DRAIN
};

StopMode FinishingReasonToStopMode(FinishingReason reason);

// Optional: For easier debugging or logging
std::string FinishingReasonToString(FinishingReason reason);

class OmniSourceStreamTask : public OmniStreamTask {
public:
    OmniSourceStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env, std::unique_ptr<Object> lockObject, int taskType);

    ~OmniSourceStreamTask() override;

    void init() override;

    void processInput(MailboxDefaultAction::Controller* controller) override;

    const std::string getName() const override;
    void AdvanceToEndOfEventTime() override;
    void cancel() override;

private:
    FinishingReason finishingReason = FinishingReason::END_OF_DATA;

    std::unique_ptr<Object> lockObject_;
    std::unique_ptr<std::thread> sourceThread_;
    std::atomic<bool> sourceCancelRequested_{false};

    void CompleteProcessing();
    void runSourceInThread();
};
} // namespace omnistream
