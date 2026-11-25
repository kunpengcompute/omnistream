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

#ifndef RESULTPARTITIONFACTORY_H
#define RESULTPARTITIONFACTORY_H

#include <executiongraph/descriptor/ResultPartitionDeploymentDescriptorPOD.h>

#include "ResultPartition.h"
#include "ResultPartitionManager.h"
#include "core/utils/function/Supplier.h"
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>

#include "buffer/BufferPoolFactory.h"
#include "buffer/NetworkMemoryBufferPool.h"
#include "buffer/NetworkObjectBufferPool.h"

// check
namespace omnistream {
using datastream::NetworkMemoryBufferPool;

class ResultPartitionFactory {
public:
    // Default constructor
    ResultPartitionFactory() = default;

    // Full argument constructor
    ResultPartitionFactory(
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<NetworkObjectBufferPool> objectBufferPoolFactory,
        std::shared_ptr<NetworkMemoryBufferPool> memoryBufferPoolFactory,
        int networkBufferSize);

    // Getters
    std::shared_ptr<ResultPartitionManager> getPartitionManager() const;
    std::shared_ptr<ObjectBufferPoolFactory> getBufferPoolFactory() const;
    int getNetworkBufferSize() const;

    // Setters
    void setPartitionManager(std::shared_ptr<ResultPartitionManager> partitionManager);
    void setBufferPoolFactory(std::shared_ptr<BufferPoolFactory> bufferPoolFactory);
    void setNetworkBufferSize(int networkBufferSize);

    // toString method
    std::string toString() const;

    std::shared_ptr<ResultPartition> create(
        const std::string& taskNameWithSubtaskAndId,
        int partitionIndex,
        const ResultPartitionDeploymentDescriptorPOD& desc,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
        int taskType);

    std::shared_ptr<ResultPartition> create(
        const std::string& taskNameWithSubtaskAndId,
        int partitionIndex,
        const ResultPartitionIDPOD& id,
        int resultPartitionType,
        int numberOfSubpartitions,
        int maxParallelism,
        std::shared_ptr<Supplier<BufferPool>> bufferPoolFactory,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
        int taskType);
    std::shared_ptr<Supplier<BufferPool>> createBufferPoolFactory(
        int numberOfSubpartitions,
        int resultPartitionType,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
        int taskType);

private:
    std::shared_ptr<ResultPartitionManager> partitionManager;
    std::shared_ptr<NetworkObjectBufferPool> objectBufferPoolFactory;
    std::shared_ptr<NetworkMemoryBufferPool> memoryBufferPoolFactory;
    int networkBufferSize;
};

} // namespace omnistream

#endif
