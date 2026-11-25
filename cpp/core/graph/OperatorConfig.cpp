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

#include "OperatorConfig.h"

#include <utility>

namespace omnistream {
OperatorConfig::OperatorConfig(
    std::string uniqueName,
    std::string name,
    nlohmann::json inputType,
    nlohmann::json outputType,
    nlohmann::json description)
    : uniqueName_(std::move(uniqueName)),
      name_(std::move(name)),
      inputType_(std::move(inputType)),
      outputType_(std::move(outputType)),
      description_(std::move(description))
{
}

std::string OperatorConfig::getUniqueName() const
{
    return uniqueName_;
}

std::string OperatorConfig::getUdfName() const
{
    return udfName_;
}

std::string OperatorConfig::getName() const
{
    return name_;
}

nlohmann::json OperatorConfig::getInputType() const
{
    return inputType_;
}

nlohmann::json OperatorConfig::getOutputType() const
{
    return outputType_;
}

nlohmann::json OperatorConfig::getUdfInputType() const
{
    return udfInputType_;
}

nlohmann::json OperatorConfig::getUdfOutputType() const
{
    return udfOutputType_;
}

nlohmann::json OperatorConfig::getDescription() const
{
    return description_;
}

} // namespace omnistream
