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

#include "OmniShuffleEnvironmentConfiguration.h"

namespace omnistream {
std::shared_ptr<OmniShuffleEnvironmentConfiguration> OmniShuffleEnvironmentConfiguration::fromConfiguration(
    int numNetworkBuffers,
    int networkBufferSize,
    long requestSegmentsTimeoutMillis,
    int networkBuffersPerChannel,
    int partitionRequestInitialBackoff,
    int partitionRequestMaxBackoff,
    int floatingNetworkBuffersPerGate,
    int sortShuffleMinBuffers,
    int sortShuffleMinParallelism,
    int maxBuffersPerChannel)
{
    auto configuration = std::make_shared<omnistream::OmniShuffleEnvironmentConfiguration>(
        numNetworkBuffers,
        networkBufferSize,
        requestSegmentsTimeoutMillis,
        networkBuffersPerChannel,
        partitionRequestInitialBackoff,
        partitionRequestMaxBackoff,
        floatingNetworkBuffersPerGate,
        sortShuffleMinBuffers,
        sortShuffleMinParallelism,
        maxBuffersPerChannel);
    return configuration;
}
} // namespace omnistream
