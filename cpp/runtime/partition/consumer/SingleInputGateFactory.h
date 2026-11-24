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

#ifndef SINGLEINPUTGATEFACTORY_H
#define SINGLEINPUTGATEFACTORY_H
#include <memory>
#include <string>
#include <iostream>

#include <buffer/NetworkObjectBufferPool.h>
#include <executiongraph/descriptor/ResourceIDPOD.h>
#include <partition/ResultPartitionManager.h>
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>
#include "SingleInputGate.h"
#include "executiongraph/descriptor/InputGateDeploymentDescriptorPOD.h"
#include "buffer/NetworkMemoryBufferPool.h"
#include "OmniLocalInputChannel.h"

namespace omnistream {
using ::datastream::NetworkMemoryBufferPool;
class SingleInputGateFactory {
public:
    // Default constructor
    SingleInputGateFactory() = default;

    // Full argument constructor
    SingleInputGateFactory(
        ResourceIDPOD& taskExecutorResourceId,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<NetworkObjectBufferPool> networkBufferPool,
        std::shared_ptr<NetworkMemoryBufferPool> networkMemoryBufferPool)
        : taskExecutorResourceId(taskExecutorResourceId),
          partitionManager(partitionManager),
          networkObjectBufferPool(networkBufferPool),
          networkMemoryBufferPool(networkMemoryBufferPool),
          networkBufferSize(config->getNetworkBufferSize()),
          partitionRequestInitialBackoff(config->getPartitionRequestInitialBackoff()),
          partitionRequestMaxBackoff(config->getPartitionRequestMaxBackoff()),
          networkBuffersPerChannel(config->getNetworkBuffersPerChannel()),
          floatingNetworkBuffersPerGate(config->getFloatingNetworkBuffersPerGate())
    {
    }

    class ChannelStatistics {
    public:
        int numLocalChannels;
        int numRemoteChannels;
        int numUnknownChannels;

        ChannelStatistics() = default;

        std::string toString() const
        {
            return "local:" + std::to_string(numLocalChannels) + ", remote:" + std::to_string(numRemoteChannels) +
                   ", unknown: " + std::to_string(numUnknownChannels);
        }
    };

    std::shared_ptr<SingleInputGate> create(
        std::string owningTaskName,
        int gateIndex,
        std::shared_ptr<InputGateDeploymentDescriptorPOD> igdd,
        std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
        int taskType);

    std::function<std::shared_ptr<BufferPool>()> createBufferPoolFactory(
        std::shared_ptr<BufferPoolFactory> networkBufferPool, int floatingNetworkBuffersPerGate);

    void createInputChannels(
        std::string owningTaskName,
        std::shared_ptr<InputGateDeploymentDescriptorPOD> inputGateDeploymentDescriptor,
        std::shared_ptr<SingleInputGate> inputGate,
        int consumedSubpartitionIndex);

    std::shared_ptr<InputChannel> createInputChannel(
        std::shared_ptr<SingleInputGate> inputGate,
        int index,
        ShuffleDescriptorPOD shuffleDescriptor,
        std::shared_ptr<ChannelStatistics> channelStatistics,
        int consumedSubpartitionIndex);

    // Getters
    ResourceIDPOD getTaskExecutorResourceId() const
    {
        return taskExecutorResourceId;
    }
    std::shared_ptr<ResultPartitionManager> getPartitionManager() const
    {
        return partitionManager;
    }
    std::shared_ptr<NetworkObjectBufferPool> getNetworkBufferPool() const
    {
        return networkObjectBufferPool;
    }
    int getNetworkBufferSize() const
    {
        return networkBufferSize;
    }

    // Setters (consider returning a reference to self for chaining)
    SingleInputGateFactory& setTaskExecutorResourceId(ResourceIDPOD& taskExecutorResourceId)
    {
        this->taskExecutorResourceId = taskExecutorResourceId;
        return *this;
    }

    SingleInputGateFactory& setPartitionManager(std::shared_ptr<ResultPartitionManager> partitionManager)
    {
        this->partitionManager = partitionManager;
        return *this;
    }

    SingleInputGateFactory& setNetworkBufferPool(std::shared_ptr<NetworkObjectBufferPool> networkBufferPool)
    {
        this->networkObjectBufferPool = networkBufferPool;
        return *this;
    }

    SingleInputGateFactory& setNetworkBufferSize(int networkBufferSize_)
    {
        this->networkBufferSize = networkBufferSize_;
        return *this;
    }

    // toString (example, customize as needed)
    std::string toString() const
    {
        return "SingleInputGateFactory{ taskExecutorResourceId=" + taskExecutorResourceId.toString() +
               ", partitionManager=" + partitionManager->toString() +
               ", networkBufferPool=" + networkObjectBufferPool->toString() +
               ", networkBufferSize=" + std::to_string(networkBufferSize) + "}";
    }

    std::shared_ptr<OmniLocalInputChannel> createOriginalInputChannel(
        std::shared_ptr<SingleInputGate> inputGate, int index, ResultPartitionIDPOD& partitionId);
    int getNetworkBuffersPerChannel()
    {
        return networkBuffersPerChannel;
    }

private:
    ResourceIDPOD taskExecutorResourceId;
    std::shared_ptr<ResultPartitionManager> partitionManager;
    std::shared_ptr<NetworkObjectBufferPool> networkObjectBufferPool;
    std::shared_ptr<NetworkMemoryBufferPool> networkMemoryBufferPool;
    int networkBufferSize;
    int partitionRequestInitialBackoff;
    int partitionRequestMaxBackoff;
    int networkBuffersPerChannel;
    int floatingNetworkBuffersPerGate;
};

} // namespace omnistream

#endif // SINGLEINPUTGATEFACTORY_H
