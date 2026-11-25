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

// InputChannelInfo.cpp
#include "InputChannelInfo.h"
#include <sstream>
#include <nlohmann/json.hpp>

namespace omnistream {

InputChannelInfo::InputChannelInfo() : gateIdx(0), inputChannelIdx(0)
{
}

InputChannelInfo::InputChannelInfo(int gateIdx, int inputChannelIdx)
    : gateIdx(gateIdx),
      inputChannelIdx(inputChannelIdx)
{
}

InputChannelInfo::InputChannelInfo(const InputChannelInfo& other)
    : gateIdx(other.gateIdx),
      inputChannelIdx(other.inputChannelIdx)
{
}

InputChannelInfo& InputChannelInfo::operator=(const InputChannelInfo& other)
{
    if (this != &other) {
        gateIdx = other.gateIdx;
        inputChannelIdx = other.inputChannelIdx;
    }
    return *this;
}

bool InputChannelInfo::operator==(const InputChannelInfo& other) const
{
    return gateIdx == other.gateIdx && inputChannelIdx == other.inputChannelIdx;
}

bool InputChannelInfo::operator!=(const InputChannelInfo& other) const
{
    return !(*this == other);
}

bool InputChannelInfo::operator<(const InputChannelInfo& other) const
{
    if (gateIdx != other.gateIdx) {
        return gateIdx < other.gateIdx;
    }
    return inputChannelIdx < other.inputChannelIdx;
}

InputChannelInfo::~InputChannelInfo()
{
}

int InputChannelInfo::getGateIdx() const
{
    return gateIdx;
}

void InputChannelInfo::setGateIdx(int gateIdx_)
{
    this->gateIdx = gateIdx_;
}

void InputChannelInfo::setOmni()
{
    this->omni = true;
}

bool InputChannelInfo::getOmni()
{
    return omni;
}

int InputChannelInfo::getInputChannelIdx() const
{
    return inputChannelIdx;
}

void InputChannelInfo::setInputChannelIdx(int inputChannelIdx_)
{
    this->inputChannelIdx = inputChannelIdx_;
}

std::string InputChannelInfo::toString() const
{
    nlohmann::json j;
    j["@class"] = "org.apache.flink.runtime.checkpoint.channel.InputChannelInfo";
    j["gateIdx"] = gateIdx;
    j["inputChannelIdx"] = inputChannelIdx;
    return j.dump();
}

} // namespace omnistream
