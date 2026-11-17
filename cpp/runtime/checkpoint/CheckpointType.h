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
#ifndef FLINK_TNEL_CHECKPOINTTYPE_H
#define FLINK_TNEL_CHECKPOINTTYPE_H

#include <string>

#include "SnapshotType.h"

class CheckpointType : public SnapshotType {
public:
    static std::shared_ptr<CheckpointType> CHECKPOINT;
    static std::shared_ptr<CheckpointType> FULL_CHECKPOINT;
    CheckpointType(std::string name, SnapshotType::SharingFilesStrategy sharingFilesStrategy);

    bool IsSavepoint() const override
    {
        return false;
    }
    std::string GetName() const override
    {
        return name;
    }
    SharingFilesStrategy GetSharingFilesStrategy() const override
    {
        return sharingFilesStrategy_;
    }
    bool operator==(const SnapshotType& other) const override;

    std::string ToString() override;

    nlohmann::json ToJson() override;

private:
    const std::string name;
    const SharingFilesStrategy sharingFilesStrategy_;
};

#endif // FLINK_TNEL_CHECKPOINTTYPE_H
