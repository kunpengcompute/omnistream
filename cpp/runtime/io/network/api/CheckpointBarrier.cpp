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

#include "CheckpointBarrier.h"

#include <stdexcept>
#include <sstream>

// Constructor
CheckpointBarrier::CheckpointBarrier(long id, long timestamp, CheckpointOptions* checkpointOptions)
    : CheckpointBarrier(id, timestamp, std::shared_ptr<CheckpointOptions>(checkpointOptions, [](CheckpointOptions*) {}))
{
}

CheckpointBarrier::CheckpointBarrier(long id, long timestamp, std::shared_ptr<CheckpointOptions> checkpointOptions)
    : id_(id),
      timestamp_(timestamp),
      checkpointOptions_(std::move(checkpointOptions))
{
    if (checkpointOptions_ == nullptr) {
        INFO_RELEASE("Error checkpointOptions must not be null");
        throw std::invalid_argument("checkpointOptions must not be null");
    }
}

CheckpointBarrier::~CheckpointBarrier()
{
}

std::shared_ptr<CheckpointOptions> CheckpointBarrier::GetCheckpointOptions() const
{
    return checkpointOptions_;
}

CheckpointBarrier* CheckpointBarrier::WithOptions(std::shared_ptr<CheckpointOptions> checkpointOptions)
{
    return this->checkpointOptions_.get() == checkpointOptions.get()
               ? this
               : new CheckpointBarrier(id_, timestamp_, checkpointOptions);
}

bool CheckpointBarrier::operator==(const CheckpointBarrier& other) const
{
    return id_ == other.id_ && timestamp_ == other.timestamp_ && *checkpointOptions_ == *other.checkpointOptions_;
}

long CheckpointBarrier::GetId() const
{
    return id_;
}

long CheckpointBarrier::GetTimestamp() const
{
    return timestamp_;
}

bool CheckpointBarrier::IsCheckpoint() const
{
    return !(checkpointOptions_->GetCheckpointType()->IsSavepoint());
}

CheckpointBarrier* CheckpointBarrier::AsUnaligned(bool& isNeedDel)
{
    if (checkpointOptions_->IsUnalignedCheckpoint()) {
        return this;
    } else {
        isNeedDel = true;
        return new CheckpointBarrier(
            id_, timestamp_, std::shared_ptr<CheckpointOptions>(checkpointOptions_->ToUnaligned()));
    }
}

std::string CheckpointBarrier::ToString() const
{
    std::ostringstream oss;
    oss << "CheckpointBarrier " << id_ << " @ " << timestamp_ << " Options: " << checkpointOptions_->ToString();
    return oss.str();
}

namespace std {
std::size_t hash<CheckpointBarrier>::operator()(const CheckpointBarrier& ref) const
{
    return static_cast<std::size_t>(
        (static_cast<std::uint64_t>(ref.GetId()) ^ (static_cast<std::uint64_t>(ref.GetId()) >> 32) ^
         static_cast<std::uint64_t>(ref.GetTimestamp()) ^ (static_cast<std::uint64_t>(ref.GetTimestamp()) >> 32)));
}
} // namespace std
