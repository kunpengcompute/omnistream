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

#ifndef OMNISTREAM_OMNISTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNISTREAMTASKNETWORKINPUT_H

#include "OmniAbstractStreamTaskNetworkInput.h"

namespace omnistream {
class OmniStreamTaskNetworkInput : public OmniAbstractStreamTaskNetworkInput {
public:
    OmniStreamTaskNetworkInput(
        int64_t inputIndex,
        const std::shared_ptr<CheckpointedInputGate>& inputGate,
        int taskType,
        TypeSerializer* inputSerializer,
        std::vector<long>& channelInfos,
        int64_t checkpointInterval)
        : OmniAbstractStreamTaskNetworkInput(
              inputIndex, inputGate, taskType, inputSerializer, channelInfos, checkpointInterval)
    {
    }
};
} // namespace omnistream

#endif
