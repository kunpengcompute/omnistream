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

#include "ShuffleIOOwnerContextPOD.h"

namespace omnistream {

ShuffleIOOwnerContextPOD::ShuffleIOOwnerContextPOD() : ownerName(""), executionAttemptID()
{
}

ShuffleIOOwnerContextPOD::ShuffleIOOwnerContextPOD(
    const std::string& ownerName, const ExecutionAttemptIDPOD& executionAttemptID)
    : ownerName(ownerName),
      executionAttemptID(executionAttemptID)
{
}

ShuffleIOOwnerContextPOD::ShuffleIOOwnerContextPOD(const ShuffleIOOwnerContextPOD& other)
    : ownerName(other.ownerName),
      executionAttemptID(other.executionAttemptID)
{
}

ShuffleIOOwnerContextPOD::~ShuffleIOOwnerContextPOD()
{
}

std::string ShuffleIOOwnerContextPOD::getOwnerName() const
{
    return ownerName;
}

void ShuffleIOOwnerContextPOD::setOwnerName(const std::string& ownerName_)
{
    this->ownerName = ownerName_;
}

ExecutionAttemptIDPOD ShuffleIOOwnerContextPOD::getExecutionAttemptID() const
{
    return executionAttemptID;
}

void ShuffleIOOwnerContextPOD::setExecutionAttemptID(const ExecutionAttemptIDPOD& executionAttemptID)
{
    this->executionAttemptID = executionAttemptID;
}

std::string ShuffleIOOwnerContextPOD::toString() const
{
    std::stringstream ss;
    ss << "ShuffleIOOwnerContextPOD{ownerName='" << ownerName
       << "', executionAttemptID=" << executionAttemptID.toString() << "}";
    return ss.str();
}

ShuffleIOOwnerContextPOD& ShuffleIOOwnerContextPOD::operator=(const ShuffleIOOwnerContextPOD& other)
{
    ownerName = other.ownerName;
    executionAttemptID = other.executionAttemptID;
    return *this;
}

} // namespace omnistream
