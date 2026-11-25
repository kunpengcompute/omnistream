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

#ifndef FLINK_TNEL_EXECTUIONCHECKPOINTINGOPTIONS_H
#define FLINK_TNEL_EXECTUIONCHECKPOINTINGOPTIONS_H

#include "core/configuration/ConfigOption.h"
#include "core/configuration/ConfigOptions.h"

#include <chrono>
#include <memory>
#include <string>

using namespace std::chrono_literals;

enum class CheckpointingMode {
    EXACTLY_ONCE,
    AT_LEAST_ONCE
};

class ExecutionCheckpointingOptions {
public:
    static const ConfigOptionV2<CheckpointingMode>* CHECKPOINTING_MODE;
    static const ConfigOptionV2<std::chrono::milliseconds>* CHECKPOINTING_TIMEOUT;
    static const ConfigOptionV2<int>* MAX_CONCURRENT_CHECKPOINTS;
    static const ConfigOptionV2<std::chrono::milliseconds>* MIN_PAUSE_BETWEEN_CHECKPOINTS;
    static const ConfigOptionV2<int>* TOLERABLE_FAILURE_NUMBER;
    // static const ConfigOption<CheckpointConfig::ExternalizedCheckpointCleanup> *EXTERNALIZED_CHECKPOINT;
    static const ConfigOptionV2<std::chrono::milliseconds>* CHECKPOINTING_INTERVAL;
    static const ConfigOptionV2<bool>* ENABLE_UNALIGNED;
    static const ConfigOptionV2<std::chrono::milliseconds>* ALIGNED_CHECKPOINT_TIMEOUT;
    static const ConfigOptionV2<std::chrono::milliseconds>* ALIGNMENT_TIMEOUT;
    static const ConfigOptionV2<bool>* FORCE_UNALIGNED;
    static const ConfigOptionV2<long>* CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA;
    static const ConfigOptionV2<bool>* ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;
};

#endif // FLINK_TNEL_EXECTUIONCHECKPOINTINGOPTIONS_H
