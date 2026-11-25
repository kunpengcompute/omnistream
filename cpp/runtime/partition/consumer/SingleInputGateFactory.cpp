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

#include "SingleInputGateFactory.h"
#include "LocalInputChannel.h"
#include "RemoteInputChannel.h"
#include "OmniLocalInputChannel.h"
#include "LocalRecoveredInputChannel.h"
#include "RemoteRecoveredInputChannel.h"
#include "checkpoint/channel/ChannelStateWriterImpl.h"

namespace omnistream {
std::shared_ptr<SingleInputGate> SingleInputGateFactory::create(
    std::string owningTaskName,
    int gateIndex,
    std::shared_ptr<InputGateDeploymentDescriptorPOD> igdd,
    std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
    int taskType)
{
    // std::function<std::shared_ptr<ObjectBufferPool>()> factoryFunc = createBufferPoolFactory(networkBufferPool,
    // floatingNetworkBuffersPerGate);

    std::function<std::shared_ptr<BufferPool>()> factoryFunc;
    std::shared_ptr<SegmentProvider> segmentProvider;
    if (taskType == 1) {
        factoryFunc = createBufferPoolFactory(networkObjectBufferPool, floatingNetworkBuffersPerGate);
        segmentProvider = networkObjectBufferPool;
    } else if (taskType == 2) {
        factoryFunc = createBufferPoolFactory(networkMemoryBufferPool, floatingNetworkBuffersPerGate);
        segmentProvider = networkMemoryBufferPool;
    }
    LOG("new SingleInputGate will running");
    std::shared_ptr<SingleInputGate> inputGate = std::make_shared<SingleInputGate>(
        owningTaskName,
        gateIndex,
        igdd->getConsumedResultId(),
        igdd->getConsumedPartitionType(),
        igdd->getConsumedSubpartitionIndex(),
        igdd->getShuffleDescriptors().size(),
        partitionProducerStateProvider,
        factoryFunc,
        segmentProvider,
        networkBufferSize);
    LOG("createInputChannels will running");
    createInputChannels(owningTaskName, igdd, inputGate, igdd->getConsumedSubpartitionIndex());
    return inputGate;
}

std::function<std::shared_ptr<BufferPool>()> SingleInputGateFactory::createBufferPoolFactory(
    std::shared_ptr<BufferPoolFactory> bufferPoolFactory, int floatingNetworkBuffersPerGate)
{
    int numRequiredBuffers = 1;
    int maxUsedBuffers = floatingNetworkBuffersPerGate;
    std::function<std::shared_ptr<BufferPool>()> bufferPoolFactoryFunc = [=]() -> std::shared_ptr<BufferPool> {
        LOG("createBufferPoolFactory function running");
        return bufferPoolFactory->createBufferPool(numRequiredBuffers, maxUsedBuffers);
    };
    INFO_RELEASE("bufferPoolFactoryFunc size :" << sizeof(bufferPoolFactoryFunc));
    return bufferPoolFactoryFunc;
}

void SingleInputGateFactory::createInputChannels(
    std::string owningTaskName,
    std::shared_ptr<InputGateDeploymentDescriptorPOD> inputGateDeploymentDescriptor,
    std::shared_ptr<SingleInputGate> inputGate,
    int consumedSubpartitionIndex
    // InputChannelMetrics metrics
)
{
    std::vector<ShuffleDescriptorPOD> shuffleDescriptors = inputGateDeploymentDescriptor->getShuffleDescriptors();
    std::shared_ptr<ChannelStatistics> channelStatistics = std::make_shared<ChannelStatistics>();
    std::vector<std::shared_ptr<InputChannel>> inputChannels(shuffleDescriptors.size());
    for (size_t i = 0; i < inputChannels.size(); i++) {
        inputChannels[i] =
            createInputChannel(inputGate, i, shuffleDescriptors[i], channelStatistics, consumedSubpartitionIndex);
    }
    inputGate->setInputChannels(inputChannels);
    LOG(owningTaskName << ": Created " << inputChannels.size() << " input channels");
}

std::shared_ptr<InputChannel> SingleInputGateFactory::createInputChannel(
    std::shared_ptr<SingleInputGate> inputGate,
    int index,
    ShuffleDescriptorPOD shuffleDescriptor,
    std::shared_ptr<ChannelStatistics> channelStatistics,
    int consumedSubpartitionIndex)
{
    ResourceIDPOD producerResourceId = shuffleDescriptor.getStoresLocalResourcesOn();
    channelStatistics->numLocalChannels++;

    if (producerResourceId == this->taskExecutorResourceId) {
        INFO_RELEASE("CREATE A LOCAL RECOVERED INPUT CHANNEL#################################");
        return std::make_shared<LocalRecoveredInputChannel>(
            inputGate,
            index,
            shuffleDescriptor.getResultPartitionID(),
            consumedSubpartitionIndex,
            partitionManager,
            partitionRequestInitialBackoff,
            partitionRequestMaxBackoff,
            std::shared_ptr<SimpleCounter>(),
            std::shared_ptr<SimpleCounter>(),
            getNetworkBuffersPerChannel());
        ;
    }
    INFO_RELEASE("CREATE A REMOTE RECOVERED INPUT CHANNEL#################################");
    return std::make_shared<RemoteRecoveredInputChannel>(
        inputGate,
        index,
        shuffleDescriptor.getResultPartitionID(),
        partitionManager,
        partitionRequestInitialBackoff,
        partitionRequestMaxBackoff,
        networkBuffersPerChannel,
        std::shared_ptr<SimpleCounter>(),
        std::shared_ptr<SimpleCounter>());
}

std::shared_ptr<OmniLocalInputChannel> SingleInputGateFactory::createOriginalInputChannel(
    std::shared_ptr<SingleInputGate> inputGate, int index, ResultPartitionIDPOD& partitionId)
{
    return std::make_shared<OmniLocalInputChannel>(
        inputGate,
        index,
        partitionId,
        partitionManager,
        partitionRequestInitialBackoff,
        partitionRequestMaxBackoff,
        networkBuffersPerChannel,
        std::shared_ptr<SimpleCounter>(),
        std::shared_ptr<SimpleCounter>(),
        nullptr);
}

} // namespace omnistream
