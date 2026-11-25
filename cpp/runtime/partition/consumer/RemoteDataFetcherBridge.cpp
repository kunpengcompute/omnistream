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

#include "RemoteDataFetcherBridge.h"
#include "RemoteInputChannel.h"

namespace omnistream {
void RemoteDataFetcherBridge::InitCppRemoteInputChannel(std::vector<std::shared_ptr<SingleInputGate>> inputGates)
{
    for (const auto& inputGate : inputGates) {
        const auto& channelMap = inputGate->getInputChannels();
        for (const auto& pair : channelMap) {
            const auto& channel = pair.second;
            if (auto remoteChannel = std::dynamic_pointer_cast<RemoteInputChannel>(channel)) {
                remoteChannel->SetRemoteDataFetcherBridge(shared_from_this());
                LOG("In RemoteDataFetcher Init, set RemoteDataFetcherBridge to RemoteInputChannel");
            } else {
                LOG("In RemoteDataFetcher Init, this Channel is not a RemoteInputChannel");
            }
        }
    }
}
} // namespace omnistream
