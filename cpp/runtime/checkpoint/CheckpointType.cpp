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

#include "CheckpointType.h"
#include "nlohmann/json.hpp"

std::shared_ptr<CheckpointType> CheckpointType::CHECKPOINT = std::shared_ptr<CheckpointType>(
    new CheckpointType("Checkpoint", SnapshotType::SharingFilesStrategy::FORWARD_BACKWARD));

std::shared_ptr<CheckpointType> CheckpointType::FULL_CHECKPOINT =
    std::shared_ptr<CheckpointType>(new CheckpointType("FullCheckpoint", SnapshotType::SharingFilesStrategy::FORWARD));

CheckpointType::CheckpointType(std::string name, SnapshotType::SharingFilesStrategy sharingFileStrategy)
    : name(name),
      sharingFilesStrategy_(sharingFileStrategy)
{
}

bool CheckpointType::operator==(const SnapshotType& other) const
{
    auto castedOther = dynamic_cast<const CheckpointType*>(&other);
    if (!castedOther) {
        return false;
    }
    return name == castedOther->name && sharingFilesStrategy_ == castedOther->sharingFilesStrategy_;
}

std::string CheckpointType::ToString()
{
    nlohmann::json json;
    json["name"] = name;
    json["sharingFilesStrategy"] = sharingFilesStrategy_;
    return json.dump();
}

nlohmann::json CheckpointType::ToJson()
{
    nlohmann::json json;
    json["name"] = name;
    json["sharingFilesStrategy"] = sharingFilesStrategy_;
    return json;
}
