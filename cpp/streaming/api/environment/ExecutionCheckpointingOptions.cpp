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

#include "ExecutionCheckpointingOptions.h"

const ConfigOptionV2<CheckpointingMode>* ExecutionCheckpointingOptions::CHECKPOINTING_MODE =
    ConfigOptions::key("execution.checkpointing.mode")
        ->enumType<CheckpointingMode>()
        ->defaultValue(CheckpointingMode::EXACTLY_ONCE)
        ->withDescription("The checkpointing mode (exactly-once vs. at-least-once).");

const ConfigOptionV2<std::chrono::milliseconds>* ExecutionCheckpointingOptions::CHECKPOINTING_TIMEOUT =
    ConfigOptions::key("execution.checkpointing.timeout")
        ->durationType()
        ->defaultValue(10min)
        ->withDescription("The maximum time that a checkpoint may take before being discarded.");

const ConfigOptionV2<int>* ExecutionCheckpointingOptions::MAX_CONCURRENT_CHECKPOINTS =
    ConfigOptions::key("execution.checkpointing.max-concurrent-checkpoints")
        ->intType()
        ->defaultValue(1)
        ->withDescription(
            "The maximum number of checkpoint attempts that may be in progress at the same time. If this "
            "value is n, "
            "then no checkpoints will be triggered while n checkpoint attempts are currently in flight.");

const ConfigOptionV2<std::chrono::milliseconds>* ExecutionCheckpointingOptions::MIN_PAUSE_BETWEEN_CHECKPOINTS =
    ConfigOptions::key("execution.checkpointing.min-pause")
        ->durationType()
        ->defaultValue(0ms)
        ->withDescription(
            "The minimal pause between checkpointing attempts. This setting defines how soon the "
            "checkpoint "
            "coordinator may trigger another checkpoint after it becomes possible to trigger "
            "another checkpoint with "
            "respect to the maximum number of concurrent checkpoints (see " +
            MAX_CONCURRENT_CHECKPOINTS->key() +
            ").\n\nIf the maximum number of concurrent checkpoints is set to one, this setting "
            "makes effectively sure "
            "that a minimum amount of time passes where no checkpoint is in progress at all.");

const ConfigOptionV2<int>* ExecutionCheckpointingOptions::TOLERABLE_FAILURE_NUMBER =
    ConfigOptions::key("execution.checkpointing.tolerable-failed-checkpoints")
        ->intType()
        ->noDefaultValue()
        ->withDescription(
            "The tolerable checkpoint consecutive failure number. If set to 0, that means we do not "
            "tolerance any checkpoint failure.");

const ConfigOptionV2<std::chrono::milliseconds>* ExecutionCheckpointingOptions::CHECKPOINTING_INTERVAL =
    ConfigOptions::key("execution.checkpointing.interval")
        ->durationType()
        ->noDefaultValue()
        ->withDescription(
            "Gets the interval in which checkpoints are periodically scheduled.\n\nThis setting defines "
            "the base interval. Checkpoint triggering may be delayed by the settings " +
            MAX_CONCURRENT_CHECKPOINTS->key() + " and " + MIN_PAUSE_BETWEEN_CHECKPOINTS->key());

const ConfigOptionV2<bool>* ExecutionCheckpointingOptions::ENABLE_UNALIGNED =
    ConfigOptions::key("execution.checkpointing.unaligned")
        ->booleanType()
        ->defaultValue(false)
        ->withDescription(
            "Enables unaligned checkpoints, which greatly reduce checkpointing times under "
            "backpressure.\n\nUnaligned "
            "checkpoints contain data stored in buffers as part of the checkpoint state, which allows "
            "checkpoint "
            "barriers to overtake these buffers. Thus, the checkpoint duration becomes independent of the "
            "current "
            "throughput as checkpoint barriers are effectively not embedded into the stream of data "
            "anymore.\n\nUnaligned checkpoints can only be enabled if " +
            CHECKPOINTING_MODE->key() + " is EXACTLY_ONCE and if " + MAX_CONCURRENT_CHECKPOINTS->key() + " is 1");

const ConfigOptionV2<std::chrono::milliseconds>* ExecutionCheckpointingOptions::ALIGNED_CHECKPOINT_TIMEOUT =
    ConfigOptions::key("execution.checkpointing.aligned-checkpoint-timeout")
        ->durationType()
        ->defaultValue(0ms)
        ->withDeprecatedKeys({"execution.checkpointing.alignment-timeout"})
        ->withDescription(
            "Only relevant if " + ENABLE_UNALIGNED->key() +
            " is enabled.\n\nIf timeout is 0, checkpoints will always start unaligned.\n\nIf "
            "timeout has a positive "
            "value, checkpoints will start aligned. If during checkpointing, checkpoint start "
            "delay exceeds this "
            "timeout, alignment will timeout and checkpoint barrier will start working as "
            "unaligned checkpoint.");

const ConfigOptionV2<std::chrono::milliseconds>* ExecutionCheckpointingOptions::ALIGNMENT_TIMEOUT =
    ConfigOptions::key("execution.checkpointing.alignment-timeout")
        ->durationType()
        ->defaultValue(0ms)
        ->withDescription(
            "Deprecated. execution.checkpointing.aligned-checkpoint-timeout should be used "
            "instead. Only relevant if " +
            ENABLE_UNALIGNED->key() +
            " is enabled.\n\nIf timeout is 0, checkpoints will always start unaligned.\n\nIf "
            "timeout has a positive "
            "value, checkpoints will start aligned. If during checkpointing, checkpoint start "
            "delay exceeds this "
            "timeout, alignment will timeout and checkpoint barrier will start working as "
            "unaligned checkpoint.");

const ConfigOptionV2<bool>* ExecutionCheckpointingOptions::FORCE_UNALIGNED =
    ConfigOptions::key("execution.checkpointing.unaligned.forced")
        ->booleanType()
        ->defaultValue(false)
        ->withDescription("Forces unaligned checkpoints, particularly allowing them for iterative jobs.");

const ConfigOptionV2<long>* ExecutionCheckpointingOptions::CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA =
    ConfigOptions::key("execution.checkpointing.recover-without-channel-state.checkpoint-id")
        ->longType()
        ->defaultValue(-1L)
        ->withDescription(
            "Checkpoint id for which in-flight data should be ignored in case of the recovery from this "
            "checkpoint.\n\nIt is better to keep this value empty until there is explicit need to "
            "restore from the specific checkpoint without in-flight data.");

const ConfigOptionV2<bool>* ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH =
    ConfigOptions::key("execution.checkpointing.checkpoints-after-tasks-finish.enabled")
        ->booleanType()
        ->defaultValue(false)
        ->withDescription(
            "Feature toggle for enabling checkpointing even if some of tasks have finished. Before you "
            "enable it, please take a look at the important considerations.");
