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

#include "StreamPartitionerPOD.h"

#include <sstream>
#include <iostream> // For demonstration purposes

namespace omnistream {

std::string StreamPartitionerPOD::getPartitionerName() const
{
    return partitionerName;
}

void StreamPartitionerPOD::setPartitionerName(const std::string& partitionerName_)
{
    this->partitionerName = partitionerName_;
}

const std::vector<KeyFieldInfoPOD>& StreamPartitionerPOD::getHashFields() const
{
    return hashFields;
}

void StreamPartitionerPOD::setHashFields(const std::vector<KeyFieldInfoPOD>& hashFields_)
{
    this->hashFields = hashFields_;
}

std::string StreamPartitionerPOD::toString() const
{
    std::stringstream ss;
    ss << "StreamPartitionerPOJO{"
       << "partitionerName='" << partitionerName << '\'' << ", hashFields=";

    ss << "[";
    for (const auto& field : hashFields) {
        ss << "{" << field.toString() << "}";
    }
    ss << "]";

    ss << "}";
    return ss.str();
}

} // namespace omnistream
