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

#include "OmniShuffleServiceFactory.h"

#include <buffer/NetworkObjectBufferPool.h>
#include <partition/ResultPartitionFactory.h>
#include <partition/ResultPartitionManager.h>
#include <partition/consumer/SingleInputGateFactory.h>

#include "NetConfig.h"
#include "OmniShuffleEnvironment.h"
#include "buffer/NetworkMemoryBufferPool.h"
#include "netty/GlobalNettyBufferPool.h"
#include "netty/NettyBufferConf.h"

namespace omnistream {
std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironment(
    std::shared_ptr<ShuffleEnvironmentContext> context)
{
    return createOmniShuffleEnvironmentWithContext(context);
};

std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithContext(
    std::shared_ptr<ShuffleEnvironmentContext> context)
{
    auto config = std::make_shared<OmniShuffleEnvironmentConfiguration>(
        context->getNumberofSegmentsGlobal(),
        context->getSegmentSize(),
        context->getRequestSegmentsTimeoutMillis(),
        context->getNetworkBuffersPerChannel(),
        context->getPartitionRequestInitialBackoff(),
        context->getPartitionRequestMaxBackoff(),
        context->getFloatingNetworkBuffersPerGate(),
        context->getsortShuffleMinBuffers(),
        context->getsortShuffleMinParallelism(),
        context->getmaxBuffersPerChannel());
    LOG_PART("networkObjectBufferPool will create");

    auto shuffleEnv = createOmniShuffleEnvironmentWithConfig(config, context->getResourceID());
    return shuffleEnv;
}

std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithConfig(
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config, ResourceIDPOD resourceId)
{
    auto resultPartitionManager = std::make_shared<ResultPartitionManager>();
    return createOmniShuffleEnvironmentWithResultPartitionMgr(config, resourceId, resultPartitionManager);
}

std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithResultPartitionMgr(
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
    ResourceIDPOD resourceId,
    std::shared_ptr<ResultPartitionManager> resultPartitionManager)
{
    auto netconfig = std::make_shared<NetConfig>();
    auto connectionMgr = std::make_shared<ConnectionManager>();
    return createOmniShuffleEnvironmentWithConnectionMgr(config, resourceId, resultPartitionManager, connectionMgr);
}

/* full arg and logic factory method */
std::shared_ptr<ShuffleEnvironment> OmniShuffleServiceFactory::createOmniShuffleEnvironmentWithConnectionMgr(
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
    ResourceIDPOD resourceId,
    std::shared_ptr<ResultPartitionManager> resultPartitionManager,
    std::shared_ptr<ConnectionManager> connectionManager)
{
    LOG_PART("networkObjectBufferPool will create");
    LOG_PART(
        " getNumNetworkBuffers  " << std::to_string(config->getNumNetworkBuffers()) << "  getNetworkBufferSize  "
                                  << config->getNetworkBufferSize());
    std::shared_ptr<datastream::NetworkMemoryBufferPool> networkMemoryBufferPool =
        std::make_shared<datastream::NetworkMemoryBufferPool>(
            config->getNumNetworkBuffers(),
            config->getNetworkBufferSize(),
            std::chrono::milliseconds(config->getRequestSegmentsTimeoutMillis()));

    // Create GlobalNettyBufferPool for netty buffer management
    NettyBufferConf nettyBufferConf(
        config->getNumNetworkBuffers(), // totalPoolSize
        config->getNetworkBufferSize(), // bufferSize (32KB)
        config->getNetworkBuffersPerChannel(),
        config->getFloatingNetworkBuffersPerGate());

    auto globalNettyBufferPool = std::make_shared<GlobalNettyBufferPool>(nettyBufferConf);

    std::shared_ptr<NetworkObjectBufferPool> networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(
        config->getNumNetworkBuffers(),
        config->getNetworkBufferSize(),
        std::chrono::milliseconds(config->getRequestSegmentsTimeoutMillis()));

    auto resultPartitionFactory = std::make_shared<ResultPartitionFactory>(
        resultPartitionManager, networkObjectBufferPool, networkMemoryBufferPool, config->getNetworkBufferSize());

    auto singleInputGateFactory = std::make_shared<SingleInputGateFactory>(
        resourceId, config, resultPartitionManager, networkObjectBufferPool, networkMemoryBufferPool);

    auto shuffleEnv = std::make_shared<OmniShuffleEnvironment>(
        resourceId,
        config,
        networkObjectBufferPool,
        resultPartitionManager,
        resultPartitionFactory,
        singleInputGateFactory,
        globalNettyBufferPool);
    return shuffleEnv;
}

} // namespace omnistream
