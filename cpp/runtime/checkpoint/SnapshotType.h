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

#ifndef FLINK_TNEL_SNAPSHOTTYPE_H
#define FLINK_TNEL_SNAPSHOTTYPE_H

#include <string>
#include "nlohmann/json.hpp"

class SnapshotType {
public:
    enum class SharingFilesStrategy {
        FORWARD_BACKWARD,
        FORWARD,
        NO_SHARING
    };

    virtual ~SnapshotType() = default;
    virtual bool IsSavepoint() const = 0;
    virtual std::string GetName() const = 0;
    virtual SharingFilesStrategy GetSharingFilesStrategy() const = 0;
    virtual std::string ToString() = 0;
    virtual nlohmann::json ToJson() = 0;
    virtual bool operator==(const SnapshotType& other) const = 0;
};

#endif // FLINK_TNEL_SNAPSHOTTYPE_H
