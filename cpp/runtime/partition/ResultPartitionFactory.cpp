/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "ResultPartitionFactory.h"

#include <buffer/NetworkObjectBufferPool.h>

#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>
#include "PipelinedResultPartition.h"
#include "PipelinedSubpartition.h"
#include "ResultPartitionType.h"
namespace omnistream {

ResultPartitionFactory::ResultPartitionFactory(
    std::shared_ptr<ResultPartitionManager> partitionManager,
    std::shared_ptr<NetworkObjectBufferPool> objectBufferPoolFactory,
    std::shared_ptr<datastream::NetworkMemoryBufferPool> memoryBufferPoolFactory,
    int networkBufferSize)
    : partitionManager(partitionManager),
      objectBufferPoolFactory(objectBufferPoolFactory),
      memoryBufferPoolFactory(memoryBufferPoolFactory),
      networkBufferSize(networkBufferSize)
{
}

std::shared_ptr<ResultPartition> ResultPartitionFactory::create(
    const std::string& taskNameWithSubtaskAndId,
    int partitionIndex,
    const ResultPartitionDeploymentDescriptorPOD& desc,
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
    int taskType)
{
    return create(
        taskNameWithSubtaskAndId,
        partitionIndex,
        desc.getShuffleDescriptor().getResultPartitionID(),
        desc.getPartitionType(),
        desc.getNumberOfSubpartitions(),
        desc.getMaxParallelism(),
        createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType(), config, taskType),
        config,
        taskType);
}

std::shared_ptr<ResultPartition> ResultPartitionFactory::create(
    const std::string& taskNameWithSubtaskAndId,
    int partitionIndex,
    const ResultPartitionIDPOD& id,
    int resultPartitionType,
    int numberOfSubpartitions,
    int maxParallelism,
    std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
    int taskType)
{
    LOG_PART("Inside real partition creation.");
    LOG_PART(
        " resultPartitionType  " << resultPartitionType << " name "
                                 << ResultPartitionType::getNameByType(resultPartitionType));

    if (resultPartitionType == ResultPartitionType::PIPELINED ||
        resultPartitionType == ResultPartitionType::PIPELINED_BOUNDED) {
        auto pipelinedPartition = std::make_shared<PipelinedResultPartition>(
            taskNameWithSubtaskAndId,
            partitionIndex,
            id,
            resultPartitionType,
            numberOfSubpartitions,
            maxParallelism,
            partitionManager,
            bufferPoolFactory,
            taskType);

        std::vector<std::shared_ptr<ResultSubpartition>> resultSubPartitions;
        LOG_PART(
            "Just before sub partition creation. numberOfSubpartitions is  " << std::to_string(numberOfSubpartitions));
        for (auto i = 0; i < numberOfSubpartitions; i++) {
            LOG_PART("Inside sub partition creation. index is  " << std::to_string(i));
            int configuredNetworkBuffersPerChannel = config->getNetworkBuffersPerChannel();
            auto subPartition =
                std::make_shared<PipelinedSubpartition>(i, configuredNetworkBuffersPerChannel, pipelinedPartition);
            resultSubPartitions.push_back(subPartition);
        }
        pipelinedPartition->setSubpartitions(resultSubPartitions);
        return pipelinedPartition;
    } else {
        THROW_LOGIC_EXCEPTION("only support pipelined result partition");
    }
}

class BufferPoolFactoryLambda : public Supplier<BufferPool> {
public:
    BufferPoolFactoryLambda(
        const std::shared_ptr<BufferPoolFactory>& factory,
        int leftNumRequiredBuffers,
        int rightMaxUsedBuffers,
        int numberOfSubpartitions,
        int maxBuffersPerChannel,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config)
        : factory_(factory),
          Left_numRequiredBuffers(leftNumRequiredBuffers),
          Right_maxUsedBuffers(rightMaxUsedBuffers),
          numberOfSubpartitions(numberOfSubpartitions),
          maxBuffersPerChannel(maxBuffersPerChannel),
          config(config)
    {
    }

    std::shared_ptr<BufferPool> get() override
    {
        LOG("BufferPoolFactoryLambda get");
        return factory_->createBufferPool(
            Left_numRequiredBuffers, Right_maxUsedBuffers, numberOfSubpartitions, maxBuffersPerChannel);
    };

    std::string toString() const override
    {
        return "BufferPoolFactoryLambda";
    };

    ~BufferPoolFactoryLambda() override = default;

private:
    std::shared_ptr<BufferPoolFactory> factory_;
    int Left_numRequiredBuffers;
    int Right_maxUsedBuffers;
    int numberOfSubpartitions;
    int maxBuffersPerChannel;
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config;
};

std::shared_ptr<Supplier<BufferPool>> ResultPartitionFactory::createBufferPoolFactory(
    int numberOfSubpartitions,
    int resultPartitionType,
    std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
    int taskType)
{
    LOG_PART("Beginning of createBufferPoolFactory");
    bool isSortShuffle = (resultPartitionType == ResultPartitionType::BLOCKING_PERSISTENT ||
                          resultPartitionType == ResultPartitionType::BLOCKING) &&
                         numberOfSubpartitions >= config->GetsortShuffleMinParallelism();
    int min_val = isSortShuffle ? config->GetsortShuffleMinBuffers() : numberOfSubpartitions + 1;

    bool isbounded =
        (resultPartitionType == ResultPartitionType::PIPELINED_BOUNDED ||
         resultPartitionType == ResultPartitionType::PIPELINED_APPROXIMATE);

    int max_val = isbounded ? (numberOfSubpartitions * config->getNetworkBuffersPerChannel() +
                               config->getFloatingNetworkBuffersPerGate())
                            : (isSortShuffle ? std::max(min_val, 4 * numberOfSubpartitions) : INT_MAX);
    std::pair<int, int> pairs = std::make_pair(min_val, std::max(min_val, max_val));

    int leftNumRequiredBuffers = pairs.first;

    int rightMaxUsedBuffers = pairs.second;

    int maxBuffersPerChannel = config->GetmaxBuffersPerChannel();
    if (taskType == 1) {
        LOG_PART("end  of createBufferPoolFactory");
        return std::make_shared<BufferPoolFactoryLambda>(
            objectBufferPoolFactory,
            leftNumRequiredBuffers,
            rightMaxUsedBuffers,
            numberOfSubpartitions,
            maxBuffersPerChannel,
            config);
    } else if (taskType == 2) {
        return std::make_shared<BufferPoolFactoryLambda>(
            memoryBufferPoolFactory,
            leftNumRequiredBuffers,
            rightMaxUsedBuffers,
            numberOfSubpartitions,
            maxBuffersPerChannel,
            config);
    } else {
        // todo: throw out exception
        return nullptr;
    }
}

} // namespace omnistream
