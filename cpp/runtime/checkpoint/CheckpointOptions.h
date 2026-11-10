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
#ifndef FLINK_TNEL_CHECKPOINTOPTIONS_H
#define FLINK_TNEL_CHECKPOINTOPTIONS_H

#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

#include "../state/CheckpointStorageLocationReference.h"
#include "CheckpointType.h"
#include <nlohmann/json.hpp>

class CheckpointOptions {
public:
    enum class AlignmentType {
        AT_LEAST_ONCE,
        ALIGNED,
        UNALIGNED,
        FORCED_ALIGNED
    };
    static constexpr long NO_ALIGNED_CHECKPOINT_TIME_OUT = std::numeric_limits<long>::max();
    CheckpointOptions(
        std::shared_ptr<SnapshotType> checkpointType,
        std::shared_ptr<CheckpointStorageLocationReference> targetLocation,
        AlignmentType alignmentType,
        long alignedCheckpointTimeout);

    CheckpointOptions(
        std::shared_ptr<SnapshotType> checkpointType,
        std::shared_ptr<CheckpointStorageLocationReference> targetLocation);

    static CheckpointOptions* NotExactlyOnce(
        SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location);

    static CheckpointOptions* AlignedNoTimeout(
        SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location);

    static CheckpointOptions* Unaligned(
        SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location);

    static CheckpointOptions* Unaligned(
        std::shared_ptr<SnapshotType> type, std::shared_ptr<CheckpointStorageLocationReference> location);

    static CheckpointOptions* AlignedWithTimeout(
        SnapshotType& type,
        std::shared_ptr<CheckpointStorageLocationReference> location,
        long alignedCheckpointTimeout);

    static CheckpointOptions* AlignedWithTimeout(
        std::shared_ptr<SnapshotType> type,
        std::shared_ptr<CheckpointStorageLocationReference> location,
        long alignedCheckpointTimeout);

    static CheckpointOptions* ForConfig(
        SnapshotType& checkpointType,
        std::shared_ptr<CheckpointStorageLocationReference> locationReference,
        bool isExactlyOnceMode,
        bool isUnalignedEnabled,
        long alignedCheckpointTimeout);

    AlignmentType GetAlignment() const;
    long GetAlignedCheckpointTimeout() const;
    bool NeedsAlignment() const;
    bool IsTimeoutable() const;
    SnapshotType* GetCheckpointType() const;
    std::shared_ptr<CheckpointStorageLocationReference> GetTargetLocation() const;

    bool IsExactlyOnceMode() const;
    bool IsUnalignedCheckpoint() const;
    bool NeedsChannelState() const;
    static CheckpointOptions* FromJson(nlohmann::json& config);

    CheckpointOptions* WithUnalignedSupported();
    CheckpointOptions* WithUnalignedUnsupported();
    std::shared_ptr<CheckpointOptions> ToAlignedWithTimeout();
    bool operator==(const CheckpointOptions& other) const;
    bool operator!=(const CheckpointOptions& other) const;
    std::string ToString() const;
    nlohmann::json ToJson() const;
    CheckpointOptions* ToUnaligned() const;
    static CheckpointOptions* ForCheckpointWithDefaultLocation();

private:
    static CheckpointOptions* ForceAligned(
        std::shared_ptr<SnapshotType> type,
        std::shared_ptr<CheckpointStorageLocationReference> location,
        long alignedCheckpointTimeout);

    std::shared_ptr<SnapshotType> checkpointType_;
    std::shared_ptr<CheckpointStorageLocationReference> targetLocation_;
    const AlignmentType alignmentType_;
    const long alignedCheckpointTimeout_;
    static CheckpointOptions* CHECKPOINT_AT_DEFAULT_LOCATION;
};
#endif // FLINK_TNEL_CHECKPOINTOPTIONS_H
