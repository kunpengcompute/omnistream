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
#include "CheckpointOptions.h"
#include <memory>
#include "SavepointType.h"
#include "checkpoint/SnapshotType.h"
#include "common.h"

CheckpointOptions::CheckpointOptions(
    std::shared_ptr<SnapshotType> checkpointType,
    std::shared_ptr<CheckpointStorageLocationReference> targetLocation,
    AlignmentType alignmentType,
    long alignedCheckpointTimeout)
    : checkpointType_(std::move(checkpointType)),
      targetLocation_(targetLocation),
      alignmentType_(alignmentType),
      alignedCheckpointTimeout_(alignedCheckpointTimeout)
{
    if (checkpointType_ == nullptr) {
        throw std::invalid_argument("checkpointType must not be null");
    }

    if (alignmentType == AlignmentType::UNALIGNED && checkpointType_->IsSavepoint()) {
        throw std::invalid_argument("Savepoint can't be unaligned");
    }

    if (alignedCheckpointTimeout != NO_ALIGNED_CHECKPOINT_TIME_OUT && alignmentType == AlignmentType::UNALIGNED) {
        throw std::invalid_argument(
            "Unaligned checkpoint can't have timeout (" + std::to_string(alignedCheckpointTimeout) + ")");
    }

    if (targetLocation == nullptr) {
        throw std::invalid_argument("targetLocation must not be null");
    }
}

CheckpointOptions::CheckpointOptions(
    std::shared_ptr<SnapshotType> checkpointType, std::shared_ptr<CheckpointStorageLocationReference> targetLocation)
    : checkpointType_(std::move(checkpointType)),
      targetLocation_(targetLocation),
      alignmentType_(AlignmentType::ALIGNED),
      alignedCheckpointTimeout_(NO_ALIGNED_CHECKPOINT_TIME_OUT)
{
}

CheckpointOptions* CheckpointOptions::NotExactlyOnce(
    SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location)
{
    return new CheckpointOptions(
        std::shared_ptr<SnapshotType>(&type, [](SnapshotType*) {}),
        location,
        AlignmentType::AT_LEAST_ONCE,
        NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions* CheckpointOptions::AlignedNoTimeout(
    SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location)
{
    return new CheckpointOptions(
        std::shared_ptr<SnapshotType>(&type, [](SnapshotType*) {}),
        location,
        AlignmentType::ALIGNED,
        NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions* CheckpointOptions::Unaligned(
    SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location)
{
    if (type.IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(
        std::shared_ptr<SnapshotType>(&type, [](SnapshotType*) {}),
        location,
        AlignmentType::UNALIGNED,
        NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions* CheckpointOptions::Unaligned(
    std::shared_ptr<SnapshotType> type, std::shared_ptr<CheckpointStorageLocationReference> location)
{
    if (type->IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(type, location, AlignmentType::UNALIGNED, NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions* CheckpointOptions::AlignedWithTimeout(
    SnapshotType& type, std::shared_ptr<CheckpointStorageLocationReference> location, long alignedCheckpointTimeout)
{
    if (type.IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(
        std::shared_ptr<SnapshotType>(&type, [](SnapshotType*) {}),
        location,
        AlignmentType::ALIGNED,
        alignedCheckpointTimeout);
}

CheckpointOptions* CheckpointOptions::AlignedWithTimeout(
    std::shared_ptr<SnapshotType> type,
    std::shared_ptr<CheckpointStorageLocationReference> location,
    long alignedCheckpointTimeout)
{
    if (type->IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(type, location, AlignmentType::ALIGNED, alignedCheckpointTimeout);
}

CheckpointOptions* CheckpointOptions::ForceAligned(
    std::shared_ptr<SnapshotType> type,
    std::shared_ptr<CheckpointStorageLocationReference> location,
    long alignedCheckpointTimeout)
{
    if (type->IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(type, location, AlignmentType::FORCED_ALIGNED, alignedCheckpointTimeout);
}
CheckpointOptions* CheckpointOptions::FromJson(nlohmann::json& config)
{
    auto alignmentTypeStr = config["alignment"].get<std::string>();
    AlignmentType alignmentType;
    if (alignmentTypeStr == "AT_LEAST_ONCE") {
        alignmentType = AlignmentType::AT_LEAST_ONCE;
    } else if (alignmentTypeStr == "ALIGNED") {
        alignmentType = AlignmentType::ALIGNED;
    } else if (alignmentTypeStr == "UNALIGNED") {
        alignmentType = AlignmentType::UNALIGNED;
    } else if (alignmentTypeStr == "FORCED_ALIGNED") {
        alignmentType = AlignmentType::FORCED_ALIGNED;
    } else {
        INFO_RELEASE("Error:Unknown alignment type : " << alignmentTypeStr);
        throw std::invalid_argument("Unknown alignment type : " + alignmentTypeStr);
    }

    auto alignedCheckpointTimeout = config["alignedCheckpointTimeout"].get<long>();

    std::shared_ptr<CheckpointStorageLocationReference> targetLocation = nullptr;
    auto reference = config["targetLocation"]["referenceBytes"];
    if (reference.is_null()) {
        targetLocation = CheckpointStorageLocationReference::GetDefault();
    } else {
        auto encodedReference = Base64_decode(reference.get<std::string>());
        auto referenceBytes = std::make_shared<std::vector<uint8_t>>(encodedReference.begin(), encodedReference.end());
        if (referenceBytes == nullptr || referenceBytes->empty()) {
            targetLocation = CheckpointStorageLocationReference::GetDefault();
        } else {
            targetLocation = std::make_shared<CheckpointStorageLocationReference>(referenceBytes);
        }
    }

    bool isSavepoint = config["checkpointType"]["name"].get<std::string>().find("Savepoint") != std::string::npos;
    if (isSavepoint) {
        auto savepointFormatTypeStr = config["checkpointType"]["formatType"].get<std::string>();
        std::shared_ptr<SavepointType> savepointType;
        SavepointFormatType savepointFormatType;
        if (savepointFormatTypeStr == "CANONICAL") {
            savepointFormatType = SavepointFormatType::CANONICAL;
        } else if (savepointFormatTypeStr == "NATIVE") {
            savepointFormatType = SavepointFormatType::NATIVE;
        } else if (savepointFormatTypeStr == "COMPATIBLE") {
            savepointFormatType = SavepointFormatType::COMPATIBLE;
        } else {
            INFO_RELEASE("Error: Unknown savepoint formatType:" << savepointFormatTypeStr);
            throw std::invalid_argument("Unknown savepoint formatType : " + savepointFormatTypeStr);
        }

        auto savepointTypeStr = config["checkpointType"]["name"].get<std::string>();
        if (savepointTypeStr == "Savepoint") {
            savepointType = SavepointType::savepoint(savepointFormatType);
        } else if (savepointTypeStr == "Terminate Savepoint") {
            savepointType = SavepointType::terminate(savepointFormatType);
        } else if (savepointTypeStr == "Suspend Savepoint") {
            savepointType = SavepointType::suspend(savepointFormatType);
        } else {
            INFO_RELEASE("Error: Unknown savepoint type: " << savepointTypeStr);
            throw std::invalid_argument("Unknown savepoint type : " + savepointTypeStr);
        }
        return new CheckpointOptions(savepointType, targetLocation, alignmentType, alignedCheckpointTimeout);
    } else {
        auto checkpointTypeStr = config["checkpointType"]["name"].get<std::string>();
        std::shared_ptr<CheckpointType> checkpointType;
        if (checkpointTypeStr == "Checkpoint") {
            checkpointType = CheckpointType::CHECKPOINT;
        } else if (checkpointTypeStr == "Full Checkpoint") {
            checkpointType = CheckpointType::FULL_CHECKPOINT;
        } else {
            INFO_RELEASE("Error: Unknown checkpoint type: " << checkpointTypeStr);
            throw std::invalid_argument("Unknown checkpoint type : " + checkpointTypeStr);
        }
        return new CheckpointOptions(checkpointType, targetLocation, alignmentType, alignedCheckpointTimeout);
    }
}

CheckpointOptions* CheckpointOptions::ForConfig(
    SnapshotType& checkpointType,
    std::shared_ptr<CheckpointStorageLocationReference> locationReference,
    bool isExactlyOnceMode,
    bool isUnalignedEnabled,
    long alignedCheckpointTimeout)
{
    if (!isExactlyOnceMode) {
        return NotExactlyOnce(checkpointType, locationReference);
    } else if (checkpointType.IsSavepoint()) {
        return AlignedNoTimeout(checkpointType, locationReference);
    } else if (!isUnalignedEnabled) {
        return AlignedNoTimeout(checkpointType, locationReference);
    } else if (alignedCheckpointTimeout == 0 || alignedCheckpointTimeout == NO_ALIGNED_CHECKPOINT_TIME_OUT) {
        return Unaligned(checkpointType, locationReference);
    } else {
        return AlignedWithTimeout(checkpointType, locationReference, alignedCheckpointTimeout);
    }
}

long CheckpointOptions::GetAlignedCheckpointTimeout() const
{
    return alignedCheckpointTimeout_;
}

bool CheckpointOptions::NeedsAlignment() const
{
    return IsExactlyOnceMode() && (checkpointType_->IsSavepoint() || !IsUnalignedCheckpoint());
}

bool CheckpointOptions::operator==(const CheckpointOptions& other) const
{
    const bool sameLocation =
        (targetLocation_ == other.targetLocation_) || (targetLocation_ != nullptr && other.targetLocation_ != nullptr &&
                                                       ((*targetLocation_) == (*other.targetLocation_)));

    return *checkpointType_ == (*other.checkpointType_) && sameLocation && alignmentType_ == other.alignmentType_ &&
           alignedCheckpointTimeout_ == other.alignedCheckpointTimeout_;
}

bool CheckpointOptions::operator!=(const CheckpointOptions& other) const
{
    return !(*this == other);
}

std::string CheckpointOptions::ToString() const
{
    nlohmann::json json;

    if (checkpointType_ != nullptr) {
        json["checkpointType"] = checkpointType_->ToString();
    }
    if (targetLocation_ != nullptr) {
        json["targetLocation"] = targetLocation_->ToString();
    } else {
        INFO_RELEASE("savepoint: targetLocation_ == null:");
        json["targetLocation"]["referenceBytes"] = "(default)";
    }
    if (alignmentType_ == AlignmentType::AT_LEAST_ONCE) {
        json["alignmentType"] = "AT_LEAST_ONCE";
    } else if (alignmentType_ == AlignmentType::ALIGNED) {
        json["alignmentType"] = "ALIGNED";
    } else if (alignmentType_ == AlignmentType::UNALIGNED) {
        json["alignmentType"] = "UNALIGNED";
    } else if (alignmentType_ == AlignmentType::FORCED_ALIGNED) {
        json["alignmentType"] = "FORCED_ALIGNED";
    }
    json["alignedCheckpointTimeout"] = alignedCheckpointTimeout_;
    return json.dump();
}

nlohmann::json CheckpointOptions::ToJson() const
{
    nlohmann::json json;

    if (checkpointType_ != nullptr) {
        json["checkpointType"] = checkpointType_->ToJson();
    }
    if (targetLocation_ != nullptr) {
        json["targetLocation"] = targetLocation_->ToJson();
    } else {
        INFO_RELEASE("savepoint: targetLocation_ == null:");
        json["targetLocation"]["referenceBytes"] = "(default)";
    }
    if (alignmentType_ == AlignmentType::AT_LEAST_ONCE) {
        json["alignmentType"] = "AT_LEAST_ONCE";
    } else if (alignmentType_ == AlignmentType::ALIGNED) {
        json["alignmentType"] = "ALIGNED";
    } else if (alignmentType_ == AlignmentType::UNALIGNED) {
        json["alignmentType"] = "UNALIGNED";
    } else if (alignmentType_ == AlignmentType::FORCED_ALIGNED) {
        json["alignmentType"] = "FORCED_ALIGNED";
    }
    json["alignedCheckpointTimeout"] = alignedCheckpointTimeout_;
    return json;
}

CheckpointOptions* CheckpointOptions::ForCheckpointWithDefaultLocation()
{
    return CHECKPOINT_AT_DEFAULT_LOCATION;
}

CheckpointOptions* CheckpointOptions::ToUnaligned() const
{
    if (alignmentType_ != AlignmentType::ALIGNED) {
        throw std::logic_error("toUnaligned() can only be called on ALIGNED checkpoints");
    }
    return Unaligned(checkpointType_, targetLocation_);
}

CheckpointOptions* CheckpointOptions::CHECKPOINT_AT_DEFAULT_LOCATION =
    new CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());

bool CheckpointOptions::IsTimeoutable() const
{
    if (alignmentType_ == AlignmentType::FORCED_ALIGNED) {
        return false;
    }
    return alignmentType_ == AlignmentType::ALIGNED &&
           (alignedCheckpointTimeout_ > 0 && alignedCheckpointTimeout_ != NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

SnapshotType* CheckpointOptions::GetCheckpointType() const
{
    return checkpointType_.get();
}

std::shared_ptr<CheckpointStorageLocationReference> CheckpointOptions::GetTargetLocation() const
{
    return targetLocation_;
}

CheckpointOptions::AlignmentType CheckpointOptions::GetAlignment() const
{
    return alignmentType_;
}

bool CheckpointOptions::IsExactlyOnceMode() const
{
    return alignmentType_ != AlignmentType::AT_LEAST_ONCE;
}

bool CheckpointOptions::IsUnalignedCheckpoint() const
{
    return alignmentType_ == AlignmentType::UNALIGNED;
}

bool CheckpointOptions::NeedsChannelState() const
{
    return IsUnalignedCheckpoint() || IsTimeoutable();
}

CheckpointOptions* CheckpointOptions::WithUnalignedSupported()
{
    if (alignmentType_ == AlignmentType::FORCED_ALIGNED) {
        if (alignedCheckpointTimeout_ != NO_ALIGNED_CHECKPOINT_TIME_OUT) {
            return AlignedWithTimeout(checkpointType_, targetLocation_, alignedCheckpointTimeout_);
        } else {
            return Unaligned(checkpointType_, targetLocation_);
        }
    }
    return this;
}

CheckpointOptions* CheckpointOptions::WithUnalignedUnsupported()
{
    if (NeedsChannelState()) {
        return ForceAligned(checkpointType_, targetLocation_, alignedCheckpointTimeout_);
    }
    return this;
}

std::shared_ptr<CheckpointOptions> CheckpointOptions::ToAlignedWithTimeout()
{
    return std::shared_ptr<CheckpointOptions>(
        AlignedWithTimeout(checkpointType_, targetLocation_, alignedCheckpointTimeout_));
}
